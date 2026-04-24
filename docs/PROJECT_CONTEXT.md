# IronFlow — Project Context

> An open-source durable execution engine backed entirely by PostgreSQL.
> Java 21 · Spring Boot 3 · PostgreSQL 16 · Flyway · jOOQ

This document is the complete context for the project: what it is, why each design decision
was made, what was built in each phase, what is deliberately unfinished, and what is known to
be wrong or unverified. It is written to be read by someone who has never seen the code.

---

## 1. What this is

A **durable execution engine** — the category occupied by Temporal, Cadence, AWS Step
Functions, and Azure Durable Functions.

The problem it solves: you have a multi-step business process (reserve inventory → charge card
→ ship → email) where each step can fail independently and the whole thing must survive
process crashes, deploys, and machine loss. Writing that with a queue and a state machine
means hand-rolling retry logic, idempotency, timeouts, and crash recovery for every workflow
you write.

A durable execution engine lets you write it as **ordinary sequential code**:

```java
public OrderResult run(OrderInput input, WorkflowContext ctx) {
    var reservation = ctx.activity("reserveInventory", input.sku(), Reservation.class);
    try {
        var charge = ctx.activity("chargePayment", input.payment(), Charge.class);
        return new OrderResult(input.orderId(), "FULFILLED", charge.id());
    } catch (ActivityFailure e) {
        ctx.activity("releaseInventory", reservation.id(), Void.class);
        return new OrderResult(input.orderId(), "PAYMENT_FAILED", null);
    }
}
```

That method may take three weeks to run. The worker executing it can be killed at any point
and the workflow resumes on another machine, at the same line, with the same local variables,
without re-charging the card.

### The defining constraint: zero external brokers

Temporal requires Cassandra or MySQL **plus** a separate service tier. Most homegrown attempts
use Postgres for state and Kafka/Redis/SQS for the queue.

IronFlow puts **the queue, the timers, the event history, and the execution state in one
PostgreSQL database**. This is the central design decision and everything else follows from it.

**Why it matters — the dual-write hazard.** With an external broker, "record that step 3
finished" and "enqueue step 4" are two writes to two systems. There is no way to make them
atomic. A crash in between leaves either:

- history says step 3 finished, but nothing will ever run step 4 → **the workflow hangs
  forever**, and no retry can fix it because the record saying "this needs a task" is the
  record that already committed; or
- step 4 is queued but history does not show step 3 finished → **step 4 runs against stale
  state**, or the workflow replays and re-executes step 3's side effect.

Outbox patterns and two-phase commit mitigate this at significant complexity cost. In one
database it is simply `BEGIN … COMMIT`, and the hazard does not exist.

### Guarantees

| Guarantee | Scope | Mechanism |
|---|---|---|
| **Exactly-once** | Workflow state transitions | Postgres ACID + `uq_wf_tasks_one_open_decision` partial unique index + optimistic version fence |
| **At-least-once** | Activity side effects | Lease expiry + reaper redelivery |
| **Deterministic** | Replay | Event-sourced history + positional command cursor |

**Activities are at-least-once, and must be idempotent.** This is not a limitation that can be
engineered away — it is the two-generals problem. A worker that dies after calling Stripe but
before recording the result is indistinguishable, from the database's point of view, from one
that died before calling Stripe. The engine must assume the work may not have happened and
retry. `ActivityContext.attempt()` is exposed so activity code can detect a retry and skip work
it has already done.

---

## 2. Architecture at a glance

```
                        ┌─────────────────────────────────────┐
   HTTP ────────────────▶  WorkflowController / SignalService  │
                        └──────────────────┬──────────────────┘
                                           │  (one transaction)
                        ┌──────────────────▼──────────────────┐
                        │            PostgreSQL 16            │
                        │                                     │
                        │  wf_executions     state + version  │
                        │  wf_events         append-only      │
                        │  wf_tasks          queue + timers   │
                        │  wf_signal_dedupe                   │
                        │  wf_pending_signals                 │
                        └──────┬───────────────────────┬──────┘
                               │ SKIP LOCKED           │ LISTEN/NOTIFY
              ┌────────────────▼──────────┐   ┌────────▼─────────────────┐
              │      WorkerPoller         │   │ PostgresNotification-    │
              │  ├─ DecisionTaskExecutor  │◀──│ Listener → QueueSignal   │
              │  └─ ActivityTaskExecutor  │   └──────────────────────────┘
              └────────────┬──────────────┘
                           │
              ┌────────────▼──────────────┐   ┌──────────────────────────┐
              │       ReplayRunner        │   │   ShardedTimerPoller     │
              │  ├─ EventHistoryCursor    │   │   (16 shards, vthreads)  │
              │  ├─ SignalInbox           │   └──────────────────────────┘
              │  └─ ReplayContext         │
              └───────────────────────────┘
```

### The replay model, in one paragraph

A workflow's state is **not** stored. It is *recomputed* by re-running the workflow body from
the top against its recorded history. Each SDK call (`ctx.activity`, `ctx.sleep`,
`ctx.waitForSignal`) checks history: if the outcome is recorded, it returns immediately without
re-executing; if not, it records a command and **blocks the workflow's virtual thread forever**.
The decision thread collects the commands and commits them atomically. The parked thread is
abandoned. When the activity completes, a new decision task builds a *fresh* workflow object
and replays from the top again — now one step further.

This is why determinism is mandatory: the Nth SDK call must be the same on every replay, or
recorded outcomes get matched to the wrong calls.

---

## 3. Phase-by-phase build log

### Phase 1 — Schema and walking skeleton

| Decision | Rationale |
|---|---|
| `TEXT` + `CHECK` instead of native enums | Postgres enums cannot have values removed, and `ALTER TYPE … ADD VALUE` cannot run inside a transaction — a migration adding a status could not be rolled back atomically. |
| `TIMESTAMPTZ` everywhere | `TIMESTAMP` without zone is a correctness bug waiting for a DST transition or a container in a non-UTC zone. |
| `JSONB` events, `BYTEA` task payloads | Events are queried and indexed by operators; task payloads are opaque blobs the engine passes through, and `BYTEA` avoids a parse on the hot path. |
| `GENERATED BY DEFAULT AS IDENTITY` | SQL-standard; `BIGSERIAL` creates a sequence with confusing ownership semantics. |
| `fillfactor=70` + aggressive autovacuum on `wf_tasks` | The hottest table in the system, updated on every lease/heartbeat/ack. Leaving page space for HOT updates avoids index bloat. |

**The two load-bearing indexes:**

```sql
-- Dispatch. Partial: index size tracks PENDING tasks, not tasks ever created.
CREATE INDEX idx_wf_tasks_poll ON wf_tasks (task_queue, kind, not_before, id)
    WHERE status = 'PENDING';

-- THE exactly-once invariant for decisions: at most one open WORKFLOW task per execution.
CREATE UNIQUE INDEX uq_wf_tasks_one_open_decision ON wf_tasks (execution_id)
    WHERE kind = 'WORKFLOW' AND status IN ('PENDING', 'LEASED');
```

The second is the structural guarantee that two decision tasks can never run concurrently for
one execution. It is not enforced in application code — it is *impossible* at the schema level.

`wf_events` has an append-only trigger rejecting `UPDATE` and `DELETE`. History is the source
of truth; if it can be edited, nothing above it can be trusted.

**Bug found and fixed:** a placeholder token `BIGGERSERIAL_PLACEHOLDER` was left in the V1 DDL.
It would have failed at first migration.

### Phase 2 — Native queue and the optimistic fence

**The queue** uses the standard `FOR UPDATE SKIP LOCKED` CTE dispatch pattern. Two details:

- **Logical leases, not held locks.** A row lock would require holding a transaction (and a
  connection) for the whole activity. Instead `lease_owner` + `lease_until` are ordinary
  columns, so the transaction commits immediately and the connection returns to the pool. A
  worker that dies simply stops heartbeating.
- **Status literals are inlined, not bound.** `WHERE status = 'PENDING'`, not `status = ?` —
  the planner must match the partial index predicate, which it cannot do against a parameter
  whose value is unknown at plan time.

**Bug found and fixed:** lease arithmetic was `now() + (? / 1000.0) * INTERVAL '1 second'`,
losing sub-second precision through a float. Replaced with `INTERVAL '1 millisecond' * ?`.

**The fence** solves split-brain. Two orchestrators can believe they own an execution
simultaneously — a healed partition, a GC pause outlasting a lease, overlapping pods during a
deploy. Without a fence both apply their transition and history interleaves into a sequence the
workflow never experienced.

The conventional fix is a distributed lock (etcd/ZooKeeper/Redlock). **We deliberately do not
use one**, for the same reason we avoid a broker: a lock held in a *different system* from the
write it guards cannot be acquired in the same transaction. The holder can lose the lock
between checking and writing. That is a dual-write in disguise.

A compare-and-swap on `current_version` closes the window completely, because **the check *is*
the write**:

```sql
UPDATE wf_executions SET current_version = current_version + 1
 WHERE id = ? AND current_version = ? AND status = 'RUNNING'
```

`status = 'RUNNING'` is inside the predicate, not a separate `SELECT` — otherwise an execution
cancelled between the two statements would be resurrected.

**Design correction:** the spec called for `current_version + 2`. Implemented as `+1`: the
version is a *fence*, not a counter. Callers must predict the post-transition version to drive
the next transition; any increment other than one forces every caller to know which transition
type just ran, and a mismatch surfaces as a `StaleExecutionException` indistinguishable from
real contention. Event counting is `next_sequence`'s job.
`applyActivityCompletionWithIncrement` exists for callers with a specific reason to deviate.

`StaleExecutionException` distinguishes **retryable** (someone else won — retry with fresh
state) from **permanent** (the execution is closed — retrying can never succeed). Collapsing
these is how a terminated workflow gets retried until `max_attempts` is exhausted.

### Phase 3 — Activity failure and retry

**`ActivityOptions`** carries `maxAttempts`, `initialInterval`, `backoffCoefficient`,
`maxInterval`, `timeout`, `nonRetryableErrors`.

Options are **recorded into the `ACTIVITY_SCHEDULED` event, not resolved from config at retry
time**. A workflow running for a week must keep the policy it was scheduled with; otherwise a
deploy lowering `maxAttempts` would retroactively exhaust in-flight activities mid-retry,
failing workflows for reasons invisible in their history.

**`RetryPolicy`** is a pure function, isolated so it is unit-testable in microseconds. Retry
timing bugs are silent and slow to surface.

- **Full jitter** — uniform over `[0, computed]`, not a narrow band. When a dependency fails it
  fails for *every* in-flight activity at once; without jitter they all retry at exactly
  `t+1s`, then all at `t+2s`, hammering the recovering dependency in synchronised waves. Full
  jitter is the variant AWS measured as lowest-contention; narrow-band leaves enough
  correlation to keep the herd partly synchronised.
- **Exponent clamped at 32** before `Math.pow`. Without it `2.0^1000` is `Infinity` and the
  `Duration` conversion throws — turning a routine retry into an infrastructure error on
  precisely the task that has failed most often.
- **Cause-chain walking** for `nonRetryableErrors`. Frameworks wrap, so a `ValidationException`
  arrives as `UndeclaredThrowableException → InvocationTargetException → ValidationException`.
  Guards against self-referential causes.

**Two different transaction shapes**, deliberately:

- **Retry** → one `UPDATE` on `wf_tasks`. History records *nothing*: a failed attempt that will
  be retried is not yet an event in the workflow's story.
- **Exhaustion** → atomic across `wf_tasks` + `wf_events` + decision enqueue. Splitting these
  produces an `ACTIVITY_FAILED` with no decision task (workflow durably failed an activity and
  will *never find out*; its `catch` block never runs), or a decision task with no event
  (workflow replays, finds nothing, parks, task re-created in a hot loop).

**Non-retryable is checked *before* the attempt count**, so a validation error surfaces on
attempt 1 rather than after five minutes of pointless backoff.

**Timeouts** run the activity on a separate virtual thread with a bounded wait. Honest
limitation, logged explicitly: interrupting a thread blocked in a non-interruptible native call
does nothing. We stop *waiting*; we cannot always stop the *work*.

### Phase 4 — Idempotent start, signals, and LISTEN/NOTIFY

**Idempotent start.** The easy half (duplicate POST returns the existing execution) was already
handled by the unique index. The hard half is telling a genuine retry from a **key collision** —
a caller reusing `order-123` across tenants gets back an execution running someone else's input
and believes their request succeeded. Silent correctness failure.

Fixed with `input_fingerprint`: SHA-256 over canonicalized JSON (keys recursively sorted, so a
client library reordering map keys does not defeat it) plus the workflow type. Same key + same
fingerprint → `200`. Same key + different fingerprint → `409`, naming the likely cause.

**Signals need a different replay treatment from activities**, and this is the subtle part. An
activity's outcome is *caused by a command the workflow issued*, so history has
`ACTIVITY_SCHEDULED` then `ACTIVITY_COMPLETED` and the cursor matches them positionally. A
signal has **no preceding command**. It arrives unbidden at a sequence number determined by
wall-clock arrival, and `ctx.waitForSignal()` may be called *after* the signal already landed.
Matching signals positionally would break determinism immediately — the same workflow code
would consume a different command index depending on when an external caller happened to POST.

So signals get their own name-keyed FIFO channel (`SignalInbox`), rebuilt from history on every
replay. Determinism still holds because the same history always produces the same inbox.

**`wf_pending_signals`** buffers signals for executions that do not exist yet, handling the real
ordering hazard where "create order" and "cancel order" race. `start()` drains the buffer into
history *in the same transaction* that creates the execution, so a signal can never be lost in
the gap.

**Signal delivery deliberately bypasses the version fence.** An external HTTP caller cannot
know the current version, and requiring one would make delivery fail spuriously exactly when
the workflow is busy — which is when signals are most likely to arrive. Safety comes instead
from append-only history plus an idempotent decision enqueue.

**LISTEN/NOTIFY** replaces tight polling. Three properties make it safe:

1. **NOTIFY is transactional.** Postgres queues notifications until commit, so a listener can
   never be woken for a row not yet visible. This is the classic race in broker-based designs,
   and it is why a *database-native* pub/sub is safe where an external one is not.
2. **A lost notification must degrade to latency, not a stuck task.** `LISTEN/NOTIFY` has no
   delivery guarantee — reconnects drop notifications, the 8GB queue can overflow. So a slow
   safety-net poll runs permanently and the notification only *shortens* the wait.
3. **The payload is a routing hint only.** The poller always re-queries.

Implemented as a **trigger**, not `pg_notify()` calls in each enqueue path. Six call sites today
and more later; one missed call is a latency bug on a single code path, nearly impossible to
spot in review. A trigger cannot be forgotten.

**The lost-wakeup race** (`QueueSignal`) is the non-obvious bug. A `CountDownLatch` per queue
has this failure:

```
Poller:   lease() → empty
Listener:                    notification arrives, wake() called
Poller:   await()  → sleeps, having missed the wakeup entirely
```

The task is enqueued and visible; the poller is asleep for a full safety-net interval. Fixed
with a **monotonic generation counter** — level-triggered, not edge-triggered. The poller
captures the generation *before* leasing; if it advanced by the time it sleeps, it re-polls
immediately. **This ordering is load-bearing and must not be rearranged.**

### Phase 5 — SDK and replay engine

The largest phase. `Workflow`, `WorkflowContext`, `ReplayContext`, `EventHistoryCursor`,
`SignalInbox`, `ReplayRunner`, `DivergenceQuarantine`.

**How pausing works, and three rejected alternatives:**

| Approach | Verdict |
|---|---|
| Throw a control-flow exception | **Fatal.** Destroys the call stack, so user `try/finally` runs at the wrong time and any `catch (Exception)` swallows the engine's control flow. |
| Return a future the workflow awaits | **Rejected.** Forces user code async all the way down, losing the "write ordinary imperative Java" property that is the entire point. |
| **Block the workflow thread forever** | **Chosen.** On a virtual thread, blocking is nearly free; user code stays plain sequential Java with intact stacks. |

Abandoning a parked thread sounds alarming and is correct: it is virtual (a few hundred bytes,
not a megabyte of stack), it holds no database resources, and it *must not* be resumed — the
next decision task builds a fresh workflow object and replays from the top, which is what makes
state reconstruction verifiable rather than dependent on surviving in-memory state.

**`EventHistoryCursor` serves two access patterns**, and conflating them is the classic bug:

- **Sequential** — the workflow's Nth SDK call must match the Nth scheduling event. This is the
  nondeterminism check.
- **Indexed** — given `ACTIVITY_SCHEDULED` at seq 5, find its `ACTIVITY_COMPLETED`. These are
  **not adjacent**: activities complete out of order, so the outcome for seq 5 may sit at seq
  40, behind the outcomes for 8 and 12. Serving this by scanning forward from the cursor breaks
  the moment two activities run in parallel — the case the design exists to support.

**`DIVERGENT` is deliberately NON-terminal** — a deviation from the spec, which called it
`FAILED_DIVERGENT`. Divergence means the deployed code stopped matching recorded history: a
workflow was edited while instances were mid-flight. **The execution is undamaged**; its history
is valid and its state fully reconstructible. What is broken is the binary. Marking it terminal
would turn a reversible deploy mistake into permanent data loss across every in-flight instance
of that workflow type. So we quarantine: halt, stop burning retries, alert loudly, allow
`resume` once the code is rolled back.

**Blast-radius containment** falls out of the design: quarantine touches one row; `DIVERGENT` is
absent from the dispatch index predicate so the tasks stop being polled; the worker catches
`NonDeterministicError` per task so one poisoned execution cannot kill the dispatch loop.
Quarantine **ACKs** the task rather than nacking — nacking would return it to PENDING for
another worker to diverge on identically, burning a worker slot per attempt until
`max_attempts`.

**`DecisionOutcome.WAITING` is load-bearing.** A workflow parked with *no commands* is waiting
on the outside world. That is **not** the same as progressing with zero commands: the task must
be acked and **not** re-enqueued, or a workflow waiting three days for a human approval has its
decision task consumed and re-created in a tight loop for three days.

### Phase 6 — Durable timers

The spec offered `wf_timers` **or** `wf_tasks` with a timer kind. **Built on `wf_tasks`.**

A timer *is* a task that is not yet visible. `not_before` is already a visibility timestamp,
`kind` already includes `TIMER`, and retry backoff already works this way. A separate table
would mean a second dispatch path, a second reaper, a second set of lease semantics — and
critically, it would split "mark timer fired" and "enqueue decision task" across two tables
where today they are one row transitioning state.

**Cost: one row, zero threads.** A million workflows sleeping for a month are a million rows no
query touches until their deadline. The partial index means index size tracks *pending* timers,
not timers ever created — ten million fired timers cost nothing.

**`fire_at` is computed from replayed `ctx.now()`, not `now() + duration` at write time.** If
the deadline were derived at write time, a decision task that failed and retried thirty seconds
later would produce a timer thirty seconds further out — so a workflow's wake time would depend
on how many times its decision task happened to retry. Anchoring to the replayed marker makes it
a deterministic function of history.

**Sharding (16 shards)** stops N poller replicas contending on the same index pages. Without it
every replica walks the same hot leading edge of the same B-tree; `SKIP LOCKED` keeps that
*correct*, but correctness was never the problem — buffer contention is.

Shard is derived from `execution_id`, so all of an execution's tasks land together and its
timers fire in order under one poller. Assignment is computed locally from
`(replicaIndex, replicaCount)` with **no coordination service**. The asymmetry is critical:

> **Over-coverage is free. Under-coverage is a silent outage.**

Two replicas owning one shard is merely wasteful (`SKIP LOCKED` handles it). A shard owned by
*nobody* means its timers never fire, with no error anywhere. Configure `replicaCount` to the
**minimum** replica count you will ever run.

---

### Phase 7 — Parallel branches (Promise.all)

The phase the whole design was built to enable. Everything before it scheduled **one** command
per decision task and parked; the command list, the block reservation, and the fan-in-safe
`ON CONFLICT` enqueue were all written anticipating N commands, so the engine changes here are
almost nil. The subtlety is entirely in the SDK and one genuine concurrency bug.

**The defining insight:** a `WorkflowFuture` is **not a concurrency primitive — it is a
deferred history lookup**. No thread, no callback, no asynchrony inside the workflow.
`ctx.async` records the *intent* to schedule an activity and returns an inert token; the
workflow keeps running synchronously on its one virtual thread, accumulating more intents;
`ctx.awaitAll` is where it finally parks. The parallelism is real — the activities run
concurrently on different workers — but it happens *between* decision tasks, in the database,
not inside the workflow.

Why the token must be inert: a workflow calling `async` three times must return from all three
*immediately*, so it reaches `awaitAll` holding three tokens. If `async` blocked, the workflow
would park on the first call and never schedule the other two — collapsing the parallelism into
sequential execution. **`async` and the existing `activity` differ in exactly one respect:
when they park.** `activity` schedules-and-parks in one call; `async` schedules-and-returns,
deferring the park to `awaitAll`.

**`awaitAll` is `Promise.all`, not `Promise.race`:** the barrier releases only when the last
branch lands. It mirrors `Promise.all`'s rejection too — if any branch failed, `awaitAll`
throws that failure *as soon as it is known*, without waiting for slower branches, because a
caller waiting on all results cannot proceed with a hole where one should be. The still-pending
branches finish in the background (their side effects are at-least-once regardless); the
workflow just does not wait to learn what it already knows.

**The engine needed no changes to fan out.** When `awaitAll` parks with three accumulated
`ScheduleActivity` commands, `ReplayRunner` returns `progressing([c1, c2, c3])` — the same shape
as for one command. `DecisionCommitter` already reserves a contiguous block of N sequence
numbers in one `UPDATE … RETURNING` and inserts N task rows in one transaction. Requirements 2
and 3 (emit all schedules together, enqueue all tasks at once) were satisfied by code written in
Phase 5; N simply happened to be 1 until now.

**The one real bug: a fan-in lost-wakeup.** This is the phase's substantive engineering. Trace
it:

1. Branches A, B, C are scheduled. Their scheduling decision task is already `COMPLETED`.
2. A completes, appends `ACTIVITY_COMPLETED`, enqueues decision **D**.
3. A worker leases D and replays *outside any transaction* (the Phase 5 connection-pool
   decision). The workflow is still `WAITING` on B and C.
4. B completes **while D is still `LEASED`** — its decision enqueue hits
   `uq_wf_tasks_one_open_decision` and `ON CONFLICT DO NOTHING` absorbs it.
5. D acks itself to `COMPLETED`.

Now B's completion is in history, **no decision task is open, and nothing will ever create
one**. The barrier is satisfiable but the workflow hangs forever.

For signals and timers this could never happen — a *future* external event (the signal
delivery, the timer firing) always re-triggers a decision. **Parallel branches are the last
writers**; if the final completion loses this race, there is no re-trigger. This is the single
place the `ON CONFLICT` decision-enqueue could strand a workflow, and it bites *only* parallel
fan-in.

**The fix (`WaitCommitter`):** replace the bare WAITING ack with an atomic ack-and-recheck. In
one transaction it acks the task **and** compares the execution's `next_sequence` high-water
against the value the replay observed. If history advanced during the replay window, a
completion landed — so it re-enqueues a decision; if not, the ack stands. Because the recheck
reads `next_sequence` in the same transaction that acks the task, and each completion bumps
`next_sequence` in *its* transaction, the two serialize: either B's bump is visible here (we
re-enqueue) or B has not committed yet (and will find our task already gone, so its own enqueue
creates a fresh decision). The window is closed from both sides.

Crucially this does **not** reintroduce the hot-loop the WAITING ack was designed to avoid: a
re-enqueue happens *only when history actually advanced*. A workflow waiting three days for a
human sees no history movement and is acked exactly once.

---

## 4. Complete file inventory

**78 main sources · 16 test sources · 6 migrations · ~11,000 lines**

### Migrations (467 lines)

| File | Contents |
|---|---|
| `V1__init_ironflow_schema.sql` | 3 core tables, partial indexes, append-only trigger, `updated_at` trigger |
| `V2__add_task_uuid.sql` | External `task_uuid` handle (internal `BIGINT id` remains the key) |
| `V3__divergence_quarantine.sql` | `DIVERGENT` status (non-terminal), forensic columns, operator index |
| `V4__timer_sharding.sql` | `shard SMALLINT`, sharded timer index |
| `V5__signals_and_idempotency.sql` | `input_fingerprint`, `wf_signal_dedupe`, `wf_pending_signals` |
| `V6__task_notify.sql` | `wf_tasks_notify()` + INSERT/UPDATE triggers |

### Java packages

| Package | Files | Role |
|---|---|---|
| `io.ironflow.sdk` | 5 | User-facing API: `Workflow`, `WorkflowContext`, `ActivityOptions`, `ActivityFailure`, `WorkflowFuture` |
| `io.ironflow.replay` | 12 | Replay engine: cursor, context, runner, quarantine, signal inbox, registry |
| `io.ironflow.worker` | 13 | Dispatch and execution: pollers, executors, committers (incl. `WaitCommitter`), retry |
| `io.ironflow.queue` | 6 | Native queue, lease reaper, shard assignment |
| `io.ironflow.queue.notify` | 3 | LISTEN/NOTIFY listener, wakeup signal, health |
| `io.ironflow.timer` | 3 | Sharded timer poller and atomic firing |
| `io.ironflow.orchestrator` | 4 | Optimistic fence and its exceptions |
| `io.ironflow.api` | 12 | HTTP surface, signals, idempotency, reapers |
| `io.ironflow.persistence` | 8 | JPA entities, enums, repositories |
| `io.ironflow.config` | 2 | Scheduling, Jackson |

### Tests

| File | Type | Covers |
|---|---|---|
| `RetryPolicyTest` | unit | Backoff maths, jitter distribution, overflow clamp, cause chains |
| `EventHistoryCursorTest` | unit | Out-of-order outcomes, divergence detection, corruption |
| `SignalInboxTest` | unit | Per-name FIFO, determinism across rebuilds |
| `ShardAssignmentTest` | unit | Exactly-once coverage for all fleet sizes, distribution uniformity |
| `PersistenceLayerIT` | Testcontainers | Schema constraints, append-only trigger |
| `PostgresTaskQueueIT` | Testcontainers | SKIP LOCKED dispatch, lease semantics |
| `CrashRecoveryIT` | Testcontainers | Lease expiry, zombie writers, heartbeats, concurrent dispatch |
| `OrchestratorFenceIT` | Testcontainers | Split-brain: 32 concurrent writers, exactly one winner |
| `SignalDeliveryIT` | Testcontainers | Atomicity, dedupe scoping, buffering |
| `DurableTimerIT` | Testcontainers | Firing, shard isolation, index plan, overlap safety |
| `NotifyDispatchIT` | Testcontainers | Commit-only delivery, safety net, reconnect broadcast |
| `ParallelBranchReplayTest` | unit | Fan-out emits N schedules, barrier parks, out-of-order + fail-fast |
| `ParallelBranchIT` | Testcontainers | The fan-in lost-wakeup race, driven to exact interleaving |
| `WalkingSkeletonIT` | Testcontainers | End-to-end smoke |

---

## 5. ⚠️ Verification status — read this before trusting anything

**The code has been syntax-checked and cross-checked, but never compiled or executed.**

The build environment had a JDK **runtime** only — no `javac` binary, no Maven, no Docker, and
package mirrors outside the network allowlist. What *was* done:

| Check | Method | Result |
|---|---|---|
| Java syntax | Full parse of all 90 files via `jdk.compiler` `JavacTask` | **0 errors** |
| Cross-package imports | Every `io.ironflow.*` import resolved against defined types | **0 missing** |
| Stale references | Grep for all deleted Phase-3 skeleton classes | **0 references** |
| SQL `INSERT` arity | 16 statements: column count vs value count, paren-aware | **all balanced** |
| SQL column names | Every `INSERT`/`UPDATE`/`Record.get()` against schema from migrations | **all valid** |
| SQL bind arity | 57 call sites: `?` count vs argument count, comment-stripped | **all match** |
| YAML structure | Parsed; verified all 6 config groups nest under `ironflow:` | **valid** |

**What this does NOT prove:** that it compiles. Type errors, generic-variance problems, and
missing method overloads are invisible to a parser.

**Run `mvn verify` with Docker available — that is the real acceptance gate.**

**Most likely first-fix locations**, in order:

1. **jOOQ plain-SQL `Record.get()` overloads.** `r.get("col", Long.class)` vs `r.get(0, Long.class)`
   have different availability depending on jOOQ version. Expect a handful of adjustments.
2. **`Result<Record>.map()` return types** — inference may need explicit type witnesses.
3. **`ReplayAwareLogger extends AbstractLogger`** — SLF4J 2.x `AbstractLogger` has abstract
   methods that may differ from what is implemented here.
4. **`ActivityRegistry(List<Object> beans, …)`** — injecting every bean is legal but Spring may
   need `ObjectProvider` to avoid a circular-dependency cycle.
5. **`DecisionCommitter.serializeOptions` returning a fully-qualified `ObjectNode`** — verify the
   import resolves.

**Also unverified:** all Testcontainers ITs. They are written against a real PostgreSQL 16 but
have never been run. Timing-sensitive assertions may need tuning on slower CI hardware.

---

## 6. Known gaps and deliberate simplifications

### Not implemented — these matter at scale

| Gap | Impact | Notes |
|---|---|---|
| **Continue-as-new** | **The most important gap.** History grows without bound. A workflow with a million-iteration loop eventually cannot be replayed. | Standard fix: at a threshold, complete the execution and start a fresh one carrying only current state. |
| **Sticky history cache** | `HistoryReader` reads *full* history on *every* decision — O(n) per decision, O(n²) over a workflow's life. **The replay bottleneck.** | Cache by execution id on the worker; read incrementally from the cached high-water mark. |
| **Cancellation API** | `CANCELLED` exists in the schema and is handled everywhere, but nothing sets it. | Small: an endpoint plus a transition, mirroring `SignalService`. |
| **History archival** | `wf_events` grows forever. No retention or cold-storage tiering. | Needed before any long-running production deployment. |
| **Child workflows** | Not supported. | A significant feature, not a small addition. |
| **Search / list API** | Fetch by id or business key only. Cannot query "all failed OrderWorkflows this week". | Needs indexed search attributes. |
| **Metrics** | Health indicators exist; no Micrometer counters or timers. | Queue depth, dispatch latency, replay duration are the ones that matter. |

### Deliberate simplifications

- **`waitForSignal` has no timeout overload.** The `sleep` + `pollSignal` composition is the
  intended pattern and composes cleanly, but it is a two-step idiom rather than an obvious API.
  Deferred until real usage shows whether the ergonomics justify it.
- **Timer identity is a constant (`__timer`).** Two adjacent `sleep` calls are distinguished only
  by cursor position, so swapping them is not caught as divergence (swapping two
  differently-*named* activities would be). Requiring users to name every sleep is a worse trade.
- **16 shards is a schema-level constant.** Changing it requires recomputing every existing row's
  shard. Fine for 1–16 poller replicas; raise it now if you expect more.
- **`ActivityRegistry` uses reflection**, not compile-time codegen. Simpler; costs a reflective
  call per activity, which is noise next to the network I/O activities do.

### Operational hazards — the ones that bite silently

1. **Do not disable the LISTEN/NOTIFY safety net.** It looks redundant; notification delivery in
   testing is essentially perfect. But there is no delivery guarantee, and the failure without a
   safety net is *silent*: tasks sit PENDING forever with no error and no metric moving except
   queue depth. Raise the interval rather than removing it.

2. **PgBouncer in transaction or statement pooling mode breaks LISTEN entirely.** The
   registration is lost when the transaction ends. Notifications silently stop and dispatch falls
   back to the safety net *permanently* — the system works, just slowly, which is the hardest kind
   of problem to notice. Use `session` mode or bypass the pooler for the listener connection.

3. **The Postgres notification queue is a fixed 8GB cluster-wide.** If a listener is down while a
   transaction holds an open snapshot, undelivered notifications accumulate; overflow causes the
   *publishing* transaction to fail, breaking the write path. Monitor
   `pg_notification_queue_usage()` and alert above ~50%.

4. **Timer shard under-coverage is a silent outage.** Monitor:
   ```sql
   SELECT shard, min(not_before) FROM wf_tasks
    WHERE status='PENDING' AND kind='TIMER' AND not_before < now()
    GROUP BY shard;
   ```
   Anything more than a few seconds overdue means a shard has gone dark.

5. **Activity timeouts cannot always stop the work.** Interrupting a thread blocked in a
   non-interruptible native call does nothing. Activities doing network I/O **must** set their own
   socket timeouts. Logged explicitly rather than hidden.

6. **Timeout is retryable by default — a judgement call.** A timeout often means the request *did*
   arrive and the response was lost, so retrying can duplicate a side effect. For anything that
   moves money, set `withNonRetryableErrors("ActivityTimeoutException")` plus a downstream
   idempotency key.

7. **Signal-heavy workflows will see more optimistic-lock retries.** Signals bypass the version
   fence, so one can land between a decision's replay and its commit. Safe, but worth watching.

---

## 7. Design decisions reversed during the build

Recorded because the reasoning matters more than the outcome:

| Original | Changed to | Why |
|---|---|---|
| `BIGGERSERIAL_PLACEHOLDER` in V1 DDL | `GENERATED BY DEFAULT AS IDENTITY` | Placeholder token would have failed the first migration. |
| Lease arithmetic `(? / 1000.0) * INTERVAL '1 second'` | `INTERVAL '1 millisecond' * ?` | Float conversion silently lost sub-second precision. |
| `@Transactional` on a self-invoked method | Separate `DecisionCommitter` bean | Spring's proxy does not intercept self-invocation — every statement would have run in its own autocommit transaction. Invisible normally, catastrophic during a crash. |
| Generated jOOQ `Tables` classes | Plain-SQL jOOQ API | Generated classes require a live database at build time; plain SQL compiles standalone. |
| `FAILED_DIVERGENT` (terminal) | `DIVERGENT` (non-terminal) + quarantine | Terminal status would make a bad deploy permanently destroy every in-flight instance of a workflow type. |
| Version increment `+2` | `+1` | The version is a fence, not a counter. Unpredictable increments make legitimate contention indistinguishable from a caller bug. |
| Phase-3 skeleton classes (`DummyWorkflow`, `StepRecorder`, `DecisionExecutor`, `worker.WorkflowRegistry`) | Deleted | Superseded by the real SDK. `WorkflowRegistry` in particular would have been a duplicate bean definition against `replay.WorkflowRegistry`. |
| `wf_pending_signals` with no reaper | `PendingSignalReaper` implemented | Flagged as a gap during Phase 4 design; an unbounded table with no owner is discovered eighteen months later at 40GB. |
| Bare WAITING ack (Phase 5) | Ack-and-recheck via `WaitCommitter` (Phase 7) | The bare ack strands a workflow whose *last* parallel branch completes during the leased decision's replay window. Harmless for signals/timers (external re-trigger exists); fatal for fan-in. |

---

## 8. Getting started

```bash
# Requires: JDK 21, Maven 3.9+, Docker (for Testcontainers)

docker run -d --name ironflow-pg \
  -e POSTGRES_DB=ironflow -e POSTGRES_USER=ironflow -e POSTGRES_PASSWORD=ironflow \
  -p 5432:5432 postgres:16

mvn verify          # ← the real acceptance gate; runs all Testcontainers ITs
mvn spring-boot:run
```

```bash
# Start a workflow (idempotent on businessKey)
curl -X POST localhost:8080/api/v1/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{"workflowType":"OrderFulfillment","businessKey":"order-1",
       "input":{"orderId":"ORD-1"}}'

# Inspect (use includeHistory=false when polling — history grows unbounded)
curl localhost:8080/api/v1/workflows/{id}?includeHistory=false

# Signal a running workflow
curl -X POST localhost:8080/api/v1/workflows/{id}/signal \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: click-1' \
  -d '{"signalName":"approval","payload":{"approved":true}}'

# Operator: what is quarantined, and resume after a rollback
curl localhost:8080/api/v1/workflows/divergent
curl -X POST localhost:8080/api/v1/workflows/{id}/resume
```

### Deployment notes

- Run timer pollers as a **StatefulSet**, not a Deployment — the pod ordinal supplies
  `IRONFLOW_REPLICA_INDEX` for free.
- Set `IRONFLOW_REPLICA_COUNT` to the **minimum** replica count you will ever run.
- Point the listener connection at PostgreSQL directly, or use `session` pooling mode.
