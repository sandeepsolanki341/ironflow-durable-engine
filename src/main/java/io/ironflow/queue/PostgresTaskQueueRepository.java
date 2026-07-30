package io.ironflow.queue;

import io.ironflow.persistence.model.TaskKind;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Broker-free task queue backed entirely by a single PostgreSQL table.
 *
 * <h2>Why this works without RabbitMQ or SQS</h2>
 *
 * <p>A message broker exists to solve three problems: fan-out to competing consumers,
 * at-least-once redelivery, and durability. Postgres solves all three natively -
 * {@code FOR UPDATE SKIP LOCKED} gives competing consumers, a lease timestamp gives
 * redelivery, and WAL gives durability. What a broker <em>cannot</em> give you is the
 * thing IronFlow actually needs: enqueueing a task and mutating workflow state in the
 * same atomic transaction. With an external broker that is a dual write, and dual
 * writes fail - the process dies between the DB commit and the broker publish, and the
 * workflow is durably advanced with no task to advance it. That workflow is wedged
 * forever and no amount of retry logic recovers it, because the state that would tell
 * you to retry is the state that already committed.</p>
 *
 * <h2>Lease semantics</h2>
 *
 * <p>The lease is <em>logical</em>, not a held database lock. The dispatch transaction
 * commits immediately, leaving behind only a {@code lease_owner} token and a
 * {@code lease_until} deadline. A worker may then take thirty seconds - or thirty
 * minutes, if it heartbeats - to execute the task without pinning a Postgres backend or
 * holding an idle-in-transaction connection. This distinction is the difference between
 * a queue that scales to thousands of concurrent workers and one that exhausts
 * {@code max_connections} at fifty.</p>
 *
 * <h2>Ownership tokens</h2>
 *
 * <p>{@code lease_owner} is a per-lease UUID, deliberately <em>not</em> a stable worker
 * identity. Consider: worker W leases task T, stalls on a long GC pause past
 * {@code lease_until}, the reaper reclaims T, and another worker re-leases it. W then
 * wakes and acks. If the token were W's identity, that ack would succeed and mark
 * complete a task someone else is actively executing. Because the token is per-lease,
 * W's ack matches zero rows and is correctly rejected.</p>
 *
 * <h2>Plain SQL rather than generated jOOQ classes</h2>
 *
 * <p>These statements use jOOQ's plain-SQL API. The dispatch statement is a CTE-driven
 * {@code UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING}, which the
 * jOOQ DSL can express only awkwardly and JPA cannot express at all - Hibernate has no
 * {@code SKIP LOCKED} on a subquery feeding an UPDATE and no {@code RETURNING}
 * projection. Writing it as literal SQL also keeps the exact text visible for
 * {@code EXPLAIN}, which matters for a statement whose query plan is load-bearing.</p>
 */
@Repository
public class PostgresTaskQueueRepository {

    /** Default visibility timeout. */
    public static final Duration DEFAULT_LEASE = Duration.ofSeconds(30);

    /**
     * Dispatch statement. Structured as a CTE feeding an UPDATE...RETURNING so the whole
     * claim is one round trip and one statement - there is no window between selecting a
     * row and marking it leased in which a crash could lose it.
     *
     * <p>{@code SKIP LOCKED} is what makes concurrency work: a poller that encounters a
     * row locked by another poller steps over it rather than blocking. Without it, N
     * pollers serialize into a queue behind whichever one holds the head row, and
     * throughput collapses to single-consumer. With it, N pollers claim N disjoint
     * batches with essentially no contention.</p>
     *
     * <p>{@code ORDER BY not_before, id} matches {@code idx_wf_tasks_poll} exactly, so
     * the planner does an index range scan and stops at LIMIT rather than sorting. The
     * status literal is inlined rather than bound so the planner can match the index's
     * partial predicate - a bind parameter here silently degrades this to a bitmap heap
     * scan over the whole table.</p>
     */
    private static final String DISPATCH_SQL = """
        WITH claimed AS (
            SELECT id
              FROM wf_tasks
             WHERE status = 'PENDING'
               AND task_queue = ?
               AND kind = ?
               AND not_before <= now()
             ORDER BY not_before, id
             LIMIT ?
             FOR UPDATE SKIP LOCKED
        )
        UPDATE wf_tasks t
           SET status      = 'LEASED',
               lease_owner = ?,
               lease_until = now() + (INTERVAL '1 millisecond' * ?),
               updated_at  = now()
          FROM claimed c
         WHERE t.id = c.id
        RETURNING t.id, t.task_uuid, t.execution_id, t.scheduled_event_seq,
                  t.attempt, t.payload, t.lease_owner, t.lease_until
        """;

    private static final String HEARTBEAT_SQL = """
        UPDATE wf_tasks
           SET lease_until = now() + (INTERVAL '1 millisecond' * ?),
               updated_at  = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    private static final String COMPLETE_SQL = """
        UPDATE wf_tasks
           SET status = 'COMPLETED', lease_owner = NULL, lease_until = NULL,
               updated_at = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    private static final String FAIL_SQL = """
        UPDATE wf_tasks
           SET status       = CASE WHEN attempt >= max_attempts THEN 'FAILED'
                                   ELSE 'PENDING' END,
               attempt      = attempt + 1,
               not_before   = now() + (INTERVAL '1 millisecond' * ?),
               last_failure = ?,
               lease_owner  = NULL,
               lease_until  = NULL,
               updated_at   = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    private static final String ENQUEUE_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, ?, 'PENDING', ?, CAST(? AS timestamptz), ?, ?)
        ON CONFLICT DO NOTHING
        """;

    /** Cap on {@code last_failure} text, matching the column's practical usable size. */
    private static final int MAX_FAILURE_CHARS = 4_000;

    private final DSLContext dsl;

    public PostgresTaskQueueRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Atomically claims up to {@code limit} visible tasks and leases them for
     * {@link #DEFAULT_LEASE}.
     *
     * @param queue task queue to poll
     * @param kind  task discriminator
     * @param limit maximum tasks to claim in this round trip
     * @return leased tasks, empty if none are currently visible; never blocks
     */
    public List<LeasedTask> poll(String queue, TaskKind kind, int limit) {
        return poll(queue, kind, limit, DEFAULT_LEASE);
    }

    /**
     * Atomically claims up to {@code limit} visible tasks.
     *
     * <p>Runs in {@link Propagation#REQUIRES_NEW} deliberately. A poller must never join
     * a caller's long-running transaction: the lease would not become visible to other
     * workers until that outer transaction committed, and a rollback upstream would
     * silently un-lease tasks a worker had already begun executing.</p>
     *
     * @param queue    task queue to poll
     * @param kind     task discriminator
     * @param limit    maximum tasks to claim. Callers should size this to their own free
     *                 capacity, since every claimed task starts burning lease time
     *                 immediately whether or not a thread is ready to run it
     * @param leaseFor visibility timeout
     * @return leased tasks, each carrying the {@code leaseOwner} token required to ack
     * @throws IllegalArgumentException if {@code limit < 1} or {@code leaseFor} is below
     *                                  one millisecond
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<LeasedTask> poll(String queue, TaskKind kind, int limit, Duration leaseFor) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1, was " + limit);
        }
        if (leaseFor.toMillis() < 1) {
            throw new IllegalArgumentException("leaseFor must be >= 1ms, was " + leaseFor);
        }

        UUID leaseOwner = UUID.randomUUID();

        return dsl.fetch(DISPATCH_SQL, queue, kind.name(), limit, leaseOwner, leaseFor.toMillis())
                .map(PostgresTaskQueueRepository::toLeasedTask);
    }

    /**
     * Enqueues a task.
     *
     * <p>{@code ON CONFLICT DO NOTHING} rather than a plain insert: the partial unique
     * indexes {@code uq_wf_tasks_scheduled} and {@code uq_wf_tasks_one_open_decision}
     * make duplicate enqueue a normal, expected outcome rather than an error. A signal
     * arriving while a decision task is already pending should be a no-op at the queue
     * level - the pending decision will observe the newly appended history event when it
     * runs.</p>
     *
     * <p>Intended to be called from within a caller's transaction ({@code REQUIRED}, the
     * default), so the enqueue commits atomically with whatever state change scheduled
     * it. That atomicity is the entire reason this queue lives in the database.</p>
     *
     * @param notBefore visibility time; {@code now()} for immediate, future for timers
     * @return {@code true} if a row was inserted, {@code false} if a conflicting task
     *         already existed
     */
    @Transactional
    public boolean enqueue(UUID executionId, String queue, TaskKind kind,
                           long scheduledEventSeq, Instant notBefore,
                           byte[] payload, int maxAttempts) {
        return dsl.execute(ENQUEUE_SQL,
                executionId,
                ShardAssignment.shardFor(executionId),
                queue,
                kind.name(),
                scheduledEventSeq,
                OffsetDateTime.ofInstant(notBefore, java.time.ZoneOffset.UTC),
                payload,
                maxAttempts) == 1;
    }

    /**
     * Extends a lease for a task still being executed.
     *
     * <p>Required for any activity whose runtime may exceed the visibility timeout.
     * Without heartbeating, a legitimately slow activity is reclaimed mid-flight and
     * executed a second time concurrently with the first - at-least-once degenerating
     * into a livelock of duplicate work.</p>
     *
     * @return {@code true} if the lease was extended; {@code false} if it had already
     *         expired and been reclaimed, in which case the caller <em>must</em> abort
     *         its in-flight work rather than continue
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean heartbeat(long taskId, UUID leaseOwner, Duration extendBy) {
        return dsl.execute(HEARTBEAT_SQL, extendBy.toMillis(), taskId, leaseOwner) == 1;
    }

    /**
     * Acks a successfully executed task.
     *
     * @return {@code true} if this caller still owned the lease. A {@code false} return
     *         is not an error to swallow - it means the side effect was executed by a
     *         worker that had already lost ownership, and the task is being retried
     *         elsewhere. Log it; it is the primary signal that leases are too short.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean complete(long taskId, UUID leaseOwner) {
        return dsl.execute(COMPLETE_SQL, taskId, leaseOwner) == 1;
    }

    /**
     * Nacks a failed task, applying backoff or terminally failing it once
     * {@code max_attempts} is exhausted.
     *
     * <p>Backoff is expressed by pushing {@code not_before} forward - the identical
     * mechanism durable timers use. Retry scheduling therefore requires no separate
     * subsystem, no delay queue, and no dead-letter exchange.</p>
     *
     * @param backoff delay before the task becomes visible again; ignored if attempts
     *                are exhausted
     * @return {@code true} if this caller still owned the lease
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean fail(long taskId, UUID leaseOwner, Duration backoff, String failure) {
        return dsl.execute(FAIL_SQL,
                backoff.toMillis(), truncate(failure), taskId, leaseOwner) == 1;
    }

    private static LeasedTask toLeasedTask(Record r) {
        return new LeasedTask(
                r.get("id", Long.class),
                r.get("task_uuid", UUID.class),
                r.get("execution_id", UUID.class),
                r.get("scheduled_event_seq", Long.class),
                r.get("attempt", Integer.class),
                r.get("payload", byte[].class),
                r.get("lease_owner", UUID.class),
                r.get("lease_until", OffsetDateTime.class).toInstant());
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_FAILURE_CHARS ? s : s.substring(0, MAX_FAILURE_CHARS);
    }
}
