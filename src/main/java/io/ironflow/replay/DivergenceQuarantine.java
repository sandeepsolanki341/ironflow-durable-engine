package io.ironflow.replay;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Quarantines executions whose deployed code diverged from recorded history.
 *
 * <h2>Why quarantine rather than fail</h2>
 *
 * <p>A divergent execution is not damaged - its history is valid and its state fully
 * reconstructible. What is wrong is the code that tried to replay it. Terminating the
 * execution would convert a reversible deploy mistake into permanent data loss across every
 * in-flight instance of that workflow type.</p>
 *
 * <p>So {@code DIVERGENT} is non-terminal: progress halts, retries stop, the operator is
 * alerted, and {@link #resume} puts it back to RUNNING once the code is fixed.</p>
 *
 * <h2>Blast radius containment</h2>
 *
 * <p>Divergence not affecting other workflows falls out of the design rather than needing
 * special handling, and it is worth being explicit about why:</p>
 *
 * <ul>
 *   <li>Quarantine touches exactly one {@code wf_executions} row and that execution's own
 *       tasks. Nothing global, no shared flag, no circuit breaker.</li>
 *   <li>{@code DIVERGENT} is absent from the dispatch index predicate, so a quarantined
 *       execution's tasks stop being polled without any other queue being affected.</li>
 *   <li>The worker catches {@link NonDeterministicError} per task, so one poisoned
 *       execution does not kill the dispatch loop.</li>
 * </ul>
 *
 * <p>The contrast worth noting: without quarantine, a divergent workflow throws on every
 * replay, exhausts its retries, and - because each attempt occupies a worker slot for the
 * full decision timeout - steals throughput from healthy workflows for as long as it takes
 * to burn through {@code max_attempts}. Quarantine stops that on the first occurrence.</p>
 */
@Service
public class DivergenceQuarantine {

    private static final Logger log = LoggerFactory.getLogger(DivergenceQuarantine.class);

    /**
     * Guarded on {@code status = 'RUNNING'} so a concurrent quarantine or a cancellation
     * that landed first is not overwritten. A second detector finding the same divergence
     * is expected - several workers may replay the same execution before the first
     * quarantine commits.
     */
    private static final String QUARANTINE_SQL = """
        UPDATE wf_executions
           SET status = 'DIVERGENT',
               divergence_detail = ?,
               divergence_detected_at = now(),
               divergence_count = divergence_count + 1,
               current_version = current_version + 1
         WHERE id = ? AND status = 'RUNNING'
        """;

    /**
     * Cancels the execution's open tasks.
     *
     * <p>Necessary, not cosmetic. An open decision task left PENDING would be re-polled
     * immediately, diverge again, and re-quarantine - a spin loop consuming a worker slot
     * every few milliseconds. Cancelling removes the rows from the dispatch index
     * entirely.</p>
     *
     * <p>Activity tasks are cancelled too: their results have nowhere to go while the
     * execution cannot advance, and leaving them running means side effects continuing to
     * fire for a workflow that is administratively stopped.</p>
     */
    private static final String CANCEL_OPEN_TASKS_SQL = """
        UPDATE wf_tasks
           SET status = 'CANCELLED', lease_owner = NULL, lease_until = NULL,
               last_failure = 'execution quarantined: replay divergence',
               updated_at = now()
         WHERE execution_id = ? AND status IN ('PENDING','LEASED')
        """;

    private static final String RESUME_SQL = """
        UPDATE wf_executions
           SET status = 'RUNNING',
               divergence_detail = NULL,
               divergence_detected_at = NULL,
               current_version = current_version + 1
         WHERE id = ? AND status = 'DIVERGENT'
        """;

    private static final String REENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        SELECT ?, ?, ?, 'WORKFLOW', 'PENDING',
               COALESCE((SELECT max(sequence_number) FROM wf_events
                          WHERE execution_id = ?), 0),
               now(), NULL, 5
        ON CONFLICT DO NOTHING
        """;

    private static final String LIST_DIVERGENT_SQL = """
        SELECT id, workflow_type, divergence_detail, divergence_detected_at,
               divergence_count
          FROM wf_executions
         WHERE status = 'DIVERGENT'
         ORDER BY divergence_detected_at DESC
         LIMIT ?
        """;

    private final DSLContext dsl;

    public DivergenceQuarantine(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Quarantines a divergent execution and cancels its open tasks.
     *
     * <p>{@link Propagation#REQUIRES_NEW} because this runs on a failure path where the
     * caller's transaction is being rolled back. Joining it would roll the quarantine back
     * too, and the execution would diverge again on the very next poll - the failure would
     * be invisible except as an unexplained hot loop.</p>
     *
     * @return {@code true} if this call performed the quarantine, {@code false} if another
     *         detector got there first or the execution had already left RUNNING
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean quarantine(NonDeterministicError error) {
        UUID execId = error.getExecutionId();

        int quarantined = dsl.execute(QUARANTINE_SQL, error.toDetail(), execId);
        if (quarantined != 1) {
            log.debug("Execution {} already quarantined or closed; skipping", execId);
            return false;
        }

        int cancelled = dsl.execute(CANCEL_OPEN_TASKS_SQL, execId);

        // ERROR, not WARN: this always requires human action. It means a deploy broke
        // in-flight workflows, and every minute it goes unnoticed is another minute of
        // executions piling up in quarantine.
        log.error("QUARANTINED execution {} after replay divergence ({} open task(s) "
                        + "cancelled). {} - roll back the deployment and resume.",
                execId, cancelled, error.getMessage());

        return true;
    }

    /**
     * Returns a quarantined execution to RUNNING and re-enqueues its decision task.
     *
     * <p>Call after the offending code is rolled back or patched. If the code still
     * diverges, the execution simply quarantines again on the next replay and
     * {@code divergence_count} increments - which is the signal that the fix did not work,
     * rather than an infinite retry.</p>
     *
     * @throws IllegalStateException if the execution is not currently DIVERGENT
     */
    @Transactional
    public void resume(UUID execId, String taskQueue) {
        int resumed = dsl.execute(RESUME_SQL, execId);
        if (resumed != 1) {
            throw new IllegalStateException(
                    "Execution " + execId + " is not DIVERGENT; nothing to resume");
        }
        dsl.execute(REENQUEUE_DECISION_SQL,
                execId,
                io.ironflow.queue.ShardAssignment.shardFor(execId),
                taskQueue,
                execId);
        log.info("Resumed quarantined execution {} on queue {}", execId, taskQueue);
    }

    /** Operator view of the quarantine. */
    @Transactional(readOnly = true)
    public List<DivergentExecution> listDivergent(int limit) {
        return dsl.fetch(LIST_DIVERGENT_SQL, limit).map(r -> new DivergentExecution(
                r.get("id", UUID.class),
                r.get("workflow_type", String.class),
                r.get("divergence_detail", String.class),
                r.get("divergence_detected_at", OffsetDateTime.class).toInstant(),
                r.get("divergence_count", Integer.class)));
    }

    public record DivergentExecution(UUID executionId, String workflowType,
                                     String detail, Instant detectedAt,
                                     int divergenceCount) { }
}
