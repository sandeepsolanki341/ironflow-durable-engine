package io.ironflow.queue;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reclaims tasks abandoned by crashed or stalled workers.
 *
 * <h2>Why a reaper rather than a timeout on the worker</h2>
 *
 * <p>The failure this exists to handle is a worker that stops executing without being
 * able to tell anyone - SIGKILL, OOM, kernel panic, network partition, a hypervisor that
 * vanished. By definition the dead worker cannot clean up after itself, so recovery must
 * be driven externally by whoever notices the lease deadline pass. This is the mechanism
 * that makes activity execution at-least-once across arbitrary infrastructure failure.</p>
 *
 * <h2>Backoff on reclaim</h2>
 *
 * <p>Reclaimed tasks get exponential backoff rather than immediate visibility. This
 * matters more than it appears: the most common cause of a mass lease expiry is not
 * random crashes but a <em>poison task</em> that OOM-kills every worker that touches it.
 * Without backoff, that task is reclaimed instantly, kills the next worker, and the
 * fleet enters a crash loop that takes down the whole queue. Backoff turns an outage
 * into a slowly-failing task that exhausts {@code max_attempts} and gets out of the
 * way.</p>
 *
 * <h2>Concurrency across instances</h2>
 *
 * <p>Every application instance runs this. That is intentional and safe - the reclaim
 * statement uses {@code SKIP LOCKED} exactly as dispatch does, so concurrent reapers
 * partition the expired set rather than contending over it. No leader election, no
 * distributed lock, no single point of failure in the recovery path.</p>
 */
@Service
public class LeaseReaper {

    private static final Logger log = LoggerFactory.getLogger(LeaseReaper.class);

    /**
     * Reclaim statement.
     *
     * <p>{@code lease_until < now()} is evaluated by the database, not the JVM. The
     * reaper must never compare a database timestamp against a local clock: worker clock
     * skew would then cause either premature reclaim of live leases (duplicate
     * execution) or indefinitely delayed recovery.</p>
     *
     * <p>Backoff is computed in SQL as {@code base * 2^(attempt-1)} capped at
     * {@code maxBackoff}, with a deterministic +/-12.5% jitter derived from the task id.
     * The jitter prevents a thundering herd: without it, ten thousand tasks reclaimed in
     * the same sweep all become visible in the same instant and stampede the pollers.
     * Deriving it from {@code id} rather than {@code random()} keeps the statement
     * reproducible, which matters when reconstructing a reclaim storm from logs.</p>
     *
     * <p>The exponent is clamped at 16 before {@code POWER} is evaluated. Without the
     * clamp a task with a high attempt count overflows the interval arithmetic and
     * throws rather than backing off.</p>
     */
    private static final String RECLAIM_SQL = """
        WITH expired AS (
            SELECT id
              FROM wf_tasks
             WHERE status = 'LEASED'
               AND lease_until < now()
             ORDER BY lease_until
             LIMIT ?
             FOR UPDATE SKIP LOCKED
        )
        UPDATE wf_tasks t
           SET status      = CASE WHEN t.attempt >= t.max_attempts THEN 'FAILED'
                                  ELSE 'PENDING' END,
               attempt     = t.attempt + 1,
               not_before  = now() + (
                   INTERVAL '1 millisecond' * LEAST(
                       ? * POWER(2, LEAST(t.attempt - 1, 16)),
                       ?
                   ) * (0.875 + 0.25 * (
                       ('x' || substr(md5(t.id::text), 1, 8))::bit(32)::bigint % 1000
                   ) / 1000.0)
               ),
               last_failure = COALESCE(t.last_failure,
                                       'lease expired; worker presumed dead'),
               lease_owner = NULL,
               lease_until = NULL,
               updated_at  = now()
          FROM expired e
         WHERE t.id = e.id
        RETURNING t.id, t.execution_id, t.attempt, t.status
        """;

    private final DSLContext dsl;
    private final ReaperProperties props;

    public LeaseReaper(DSLContext dsl, ReaperProperties props) {
        this.dsl = dsl;
        this.props = props;
    }

    /**
     * Scheduled sweep. Fires every 5 seconds by default.
     *
     * <p>Uses {@code fixedDelay} rather than {@code fixedRate}: with {@code fixedRate}, a
     * sweep that runs long under a large backlog causes subsequent sweeps to pile up and
     * execute back-to-back, amplifying load exactly when the database is already
     * struggling. {@code fixedDelay} measures from completion and degrades gracefully.</p>
     */
    @Scheduled(fixedDelayString = "${ironflow.reaper.interval-ms:5000}",
               initialDelayString = "${ironflow.reaper.initial-delay-ms:5000}")
    public void sweep() {
        try {
            int total = 0;
            // Drain in bounded batches so one sweep cannot open an unboundedly long
            // transaction after a mass-crash event, and cap the rounds so a persistent
            // backlog cannot monopolise the scheduler thread indefinitely.
            for (int round = 0; round < props.getMaxRoundsPerSweep(); round++) {
                int reclaimed = reclaimBatch(props.getBatchSize());
                total += reclaimed;
                if (reclaimed < props.getBatchSize()) {
                    break;
                }
            }
            if (total > 0) {
                log.warn("Reclaimed {} expired lease(s); workers presumed dead", total);
            }
        } catch (Exception e) {
            // Never propagate. An exception escaping a @Scheduled method is logged by
            // Spring but leaves no application-level signal, and with fixedDelay the
            // next sweep still runs - so a persistent failure would otherwise look
            // exactly like an idle queue.
            log.error("Lease reaper sweep failed; will retry next interval", e);
        }
    }

    /**
     * Reclaims a single bounded batch of expired leases.
     *
     * <p>{@link Propagation#REQUIRES_NEW} guarantees each batch commits independently, so
     * a failure partway through a multi-round sweep does not roll back the rounds that
     * already succeeded.</p>
     *
     * @param batchSize maximum tasks to reclaim
     * @return number of tasks reclaimed
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int reclaimBatch(int batchSize) {
        Result<Record> reclaimed = dsl.fetch(RECLAIM_SQL,
                batchSize,
                props.getBaseBackoff().toMillis(),
                props.getMaxBackoff().toMillis());

        for (Record r : reclaimed) {
            if ("FAILED".equals(r.get("status", String.class))) {
                log.error("Task {} (execution {}) exhausted retries after {} attempts; "
                                + "moved to FAILED",
                        r.get("id"), r.get("execution_id"), r.get("attempt"));
            } else if (log.isDebugEnabled()) {
                log.debug("Reclaimed task {} (execution {}), now attempt {}",
                        r.get("id"), r.get("execution_id"), r.get("attempt"));
            }
        }
        return reclaimed.size();
    }
}
