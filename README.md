# IronFlow

<!-- Replace OWNER/REPO after pushing, so the badge resolves against your fork. -->
[![chaos](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/sandeepsolanki341/ironflow-durable-engine/badges/chaos-badge.json)](../../actions/workflows/nightly-chaos.yml)

**A durable execution engine backed entirely by PostgreSQL. No Kafka. No Redis. No broker.**

Java 21 · Spring Boot 3 · PostgreSQL 16 · Flyway · jOOQ

---

## What it does

Write a multi-step business process as ordinary sequential Java. IronFlow makes it survive
process crashes, deploys, and machine loss — without you writing retry logic, idempotency
handling, or crash recovery.

```java
@Component
public class OrderWorkflow implements Workflow<OrderInput, OrderResult> {

    @Override public String type() { return "OrderFulfillment"; }
    @Override public Class<OrderInput> inputType() { return OrderInput.class; }

    @Override
    public OrderResult run(OrderInput input, WorkflowContext ctx) {
        var reservation = ctx.activity("reserveInventory", input.sku(), Reservation.class);

        // Wait three days for a human. Costs one row and zero threads.
        if (input.amount() > 10_000) {
            ctx.sleep(Duration.ofDays(3));
            var approval = ctx.pollSignal("approval", Approval.class);
            if (approval.isEmpty()) {
                ctx.activity("escalateToManager", input.orderId(), Void.class);
            }
        }

        try {
            var charge = ctx.activity("chargePayment", input.payment(), Charge.class);
            return new OrderResult(input.orderId(), "FULFILLED", charge.id());
        } catch (ActivityFailure e) {
            // Reached after retries are exhausted. Compensation is just a catch block.
            ctx.activity("releaseInventory", reservation.id(), Void.class);
            return new OrderResult(input.orderId(), "PAYMENT_FAILED", null);
        }
    }
}
```

That method may run for three weeks. Kill the worker at any point — it resumes on another
machine, at the same line, with the same local variables, **without re-charging the card**.

Fan out work in parallel when steps are independent:

```java
var inventory = ctx.async("checkInventory", sku);
var pricing   = ctx.async("fetchPricing", sku);
var shipping  = ctx.async("estimateShipping", address);
ctx.awaitAll(inventory, pricing, shipping);   // all three ran concurrently
var quote = ctx.get(pricing).amount() + ctx.get(shipping).amount();
```

---

## Why one database

Most durable execution systems put state in a database and the queue in a broker. That makes
"record that step 3 finished" and "enqueue step 4" two writes to two systems, which cannot be
made atomic. A crash in between leaves you with either a workflow that hangs forever, or a step
that runs against stale state.

IronFlow puts **queue, timers, history, and state in one PostgreSQL database**, so every state
transition is a single ACID transaction. The dual-write hazard does not exist.

| Guarantee | Scope | Mechanism |
|---|---|---|
| Exactly-once | Workflow state transitions | Postgres ACID + partial unique index + optimistic version fence |
| At-least-once | Activity side effects | Lease expiry + reaper redelivery |
| Deterministic | Replay | Event-sourced history + positional command cursor |

**Activities must be idempotent.** At-least-once is not a limitation that can be engineered
away — a worker that dies after calling Stripe is indistinguishable from one that died before.
`ActivityContext.attempt()` lets activity code detect a retry.

---

## Features

- **Durable timers** — `ctx.sleep(Duration.ofDays(30))`. One row, zero threads. A million
  sleeping workflows cost a million rows no query touches until their deadline.
- **External signals** — `POST /signal` wakes a blocked workflow. Deduplicated by
  `Idempotency-Key`, and buffered if the execution does not exist yet.
- **Idempotent start** — same `businessKey` returns the existing execution. Same key with
  *different* input returns `409`, not a silent alias.
- **Automatic retries** — per-activity policy with full-jitter exponential backoff and a
  non-retryable type list.
- **Parallel branches** — `ctx.async()` + `ctx.awaitAll()`, a `Promise.all` for durable
  workflows. Fan out N activities in one atomic decision; they run concurrently on different
  workers and the barrier releases when the last one lands.
- **Replay divergence quarantine** — deploy a breaking workflow change and in-flight instances
  are quarantined, not destroyed. Roll back and resume.
- **Sub-10ms dispatch** — PostgreSQL `LISTEN/NOTIFY` with a safety-net poll, so a dropped
  notification costs latency rather than a stranded task.
- **Horizontal timer sharding** — 16 shards across N replicas, no coordination service.

---

## Run the whole stack (Docker)

The zero-infrastructure path — Postgres, the engine, and the dashboard, one command:

```bash
docker compose up --build
```

Then open:

- **Dashboard** — http://localhost:3000 (executions list, live DAG view, telemetry charts)
- **Engine API** — http://localhost:8080/api/v1
- **Health** — http://localhost:8080/actuator/health

The engine runs its Flyway migrations on startup; Postgres is the only stateful service. See
`docker-compose.yml` and `docker/postgresql.conf` for the tuned high-concurrency configuration.

## Load & chaos testing

- **Throughput** — `k6 run load-test/load-test.js` ramps to 1,000 VUs and asserts the documented
  ~5k–10k state-transitions/sec ceiling. Enable the benchmark workflow with
  `IRONFLOW_BENCHMARK_ENABLED=true` (the compose file already does).
- **Nightly chaos** — `.github/workflows/nightly-chaos.yml` floods 10,000 workflows while a
  Chaos Monkey hard-kills workers every 30s, then verifies three invariants in SQL (zero lost
  transitions, zero duplicated side effects, 100% completion or clean rollback). See
  `.github/chaos/README.md`.

## Quick start

```bash
# Requires JDK 21, Maven 3.9+, Docker

docker run -d --name ironflow-pg \
  -e POSTGRES_DB=ironflow -e POSTGRES_USER=ironflow -e POSTGRES_PASSWORD=ironflow \
  -p 5432:5432 postgres:16

mvn verify          # runs all Testcontainers integration tests
mvn spring-boot:run
```

```bash
# Start (idempotent on businessKey)
curl -X POST localhost:8080/api/v1/workflows/start \
  -H 'Content-Type: application/json' \
  -d '{"workflowType":"OrderFulfillment","businessKey":"order-1",
       "input":{"orderId":"ORD-1"}}'

# Inspect — pass includeHistory=false when polling; history grows unbounded
curl 'localhost:8080/api/v1/workflows/{id}?includeHistory=false'

# Signal
curl -X POST localhost:8080/api/v1/workflows/{id}/signal \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: click-1' \
  -d '{"signalName":"approval","payload":{"approved":true}}'

# Operator: inspect quarantine, resume after a rollback
curl localhost:8080/api/v1/workflows/divergent
curl -X POST localhost:8080/api/v1/workflows/{id}/resume
```

---

## ⚠️ Status

**This code has been syntax-checked and cross-checked, but never compiled or executed.**

It was built in an environment with a JDK runtime but no `javac`, no Maven, and no Docker. All
90 files parse cleanly, all 57 SQL bind sites match their placeholder counts, and every SQL
column reference resolves against the migrations — but a parser cannot see type errors.

**Run `mvn verify` first.** Expect a handful of fixes, most likely in jOOQ's plain-SQL
`Record.get()` overloads. See [`docs/PROJECT_CONTEXT.md`](docs/PROJECT_CONTEXT.md) §5 for the
full verification table and the ranked list of likely first failures.

---

## Documentation

**[`docs/PROJECT_CONTEXT.md`](docs/PROJECT_CONTEXT.md)** — the complete design document:
architecture, phase-by-phase build log with rationale for every decision, the six design
reversals made during the build, known gaps, and operational hazards.

Worth reading before deploying, particularly:

- **§6** — known gaps. Continue-as-new and the sticky history cache are the two that matter at
  scale; `HistoryReader` is currently O(n) per decision.
- **§6, operational hazards** — PgBouncer in transaction pooling mode silently breaks
  `LISTEN/NOTIFY`; timer shard under-coverage is a silent outage.

---

## Layout

```
src/main/java/io/ironflow/
├── sdk/           Workflow, WorkflowContext, ActivityOptions      (user-facing)
├── replay/        Cursor, ReplayContext, ReplayRunner, quarantine (the engine)
├── worker/        Pollers, executors, committers, retry policy
├── queue/         Native queue, lease reaper, shard assignment
│   └── notify/    LISTEN/NOTIFY listener and wakeup signal
├── timer/         Sharded timer poller and atomic firing
├── orchestrator/  Optimistic fence (split-brain protection)
├── api/           HTTP surface, signals, idempotency
└── persistence/   JPA entities, enums, repositories

src/main/resources/db/migration/   V1–V6
```

## License

Apache 2.0
