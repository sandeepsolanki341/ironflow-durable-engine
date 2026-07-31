package io.ironflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.queue.LeaseLostException;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.EventTypes;
import io.ironflow.sdk.ActivityOptions;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Commits a terminal activity failure atomically.
 *
 * <h2>Why all five operations must be one transaction</h2>
 *
 * <p>The commit bumps the execution version, marks the activity task FAILED, appends
 * {@code ACTIVITY_FAILED} to history, appends {@code WORKFLOW_TASK_SCHEDULED}, and enqueues
 * a decision task.</p>
 *
 * <p>Splitting them produces silent, unrecoverable bugs:</p>
 *
 * <ul>
 *   <li><b>Event without decision task</b> - the workflow has durably failed an activity and
 *       will never find out. Its {@code catch} block never runs. The execution sits RUNNING
 *       forever with no task to advance it.</li>
 *   <li><b>Decision task without event</b> - the workflow replays, finds no outcome for the
 *       activity, and parks again. The decision task is consumed and re-created in a loop,
 *       burning worker slots indefinitely.</li>
 *   <li><b>Task marked FAILED without either</b> - the activity is gone from the queue and
 *       its failure exists nowhere. Silent data loss.</li>
 * </ul>
 *
 * <p>This is the same dual-write hazard the whole engine is built to avoid, at a smaller
 * scale.</p>
 */
@Service
public class ActivityFailureCommitter {

    private static final Logger log =
            LoggerFactory.getLogger(ActivityFailureCommitter.class);

    private static final int MAX_FAILURE_CHARS = 4_000;

    private static final String BUMP_VERSION_SQL = """
        UPDATE wf_executions
           SET current_version = current_version + 1
         WHERE id = ? AND status = 'RUNNING'
        """;

    /**
     * Marks the activity task terminally failed.
     *
     * <p>Gated on {@code lease_owner}: a worker whose lease expired mid-execution must not
     * be able to permanently fail a task another worker is now retrying.</p>
     */
    private static final String FAIL_TASK_SQL = """
        UPDATE wf_tasks
           SET status = 'FAILED', lease_owner = NULL, lease_until = NULL,
               last_failure = ?, updated_at = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    private static final String RESERVE_SEQ_SQL = """
        UPDATE wf_executions
           SET next_sequence = next_sequence + ?
         WHERE id = ?
        RETURNING next_sequence - ?
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    /**
     * {@code ON CONFLICT DO NOTHING} because parallel activities may already have caused a
     * decision task to be open. That is the correct outcome, not an error: the pending
     * decision will observe this failure event when it replays.
     */
    private static final String ENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'WORKFLOW', 'PENDING', ?, now(), NULL, 5)
        ON CONFLICT DO NOTHING
        """;

    private static final String SOURCE_QUEUE_SQL = """
        SELECT task_queue FROM wf_tasks WHERE id = ?
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public ActivityFailureCommitter(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    /**
     * Records a terminal activity failure and wakes the workflow so it can react.
     *
     * <p>{@link Propagation#REQUIRES_NEW} so this commits independently of any caller
     * transaction, and {@code READ_COMMITTED} because the version bump provides all the
     * serialization needed.</p>
     *
     * @param options the recorded options, for the attempt/maxAttempts context an operator
     *                needs when reading history
     * @return {@code false} if the lease was already lost, meaning another worker owns this
     *         task and this failure must be discarded
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public boolean commitFailure(LeasedTask task, String failure, ActivityOptions options) {

        // 1. Version bump. First, so it takes the row lock that serializes concurrent
        //    commits on this execution, and consistently before any wf_tasks write.
        if (dsl.execute(BUMP_VERSION_SQL, task.executionId()) != 1) {
            log.debug("Execution {} no longer RUNNING; discarding activity failure",
                    task.executionId());
            return false;
        }

        // 2. Terminally fail the activity task, under our lease.
        if (dsl.execute(FAIL_TASK_SQL, truncate(failure), task.taskId(),
                task.leaseOwner()) != 1) {
            // Rolls back the version bump too.
            throw new LeaseLostException("Task %d no longer owned by %s"
                    .formatted(task.taskId(), task.leaseOwner()));
        }

        // 3. Reserve two sequence numbers: ACTIVITY_FAILED + WORKFLOW_TASK_SCHEDULED.
        long baseSeq = reserveSequenceBlock(task.executionId(), 2);

        // 4. Append ACTIVITY_FAILED. This is what ReplayRunner turns into an
        //    ActivityFailure thrown into workflow code, so the workflow's catch block can
        //    run compensation.
        var failedPayload = mapper.createObjectNode();
        failedPayload.put("scheduledEventSeq", task.scheduledEventSeq());
        failedPayload.put("failure", truncate(failure));
        failedPayload.put("attempts", task.attempt());
        failedPayload.put("maxAttempts", options.maxAttempts());
        appendEvent(task.executionId(), baseSeq,
                EventTypes.ACTIVITY_FAILED, failedPayload.toString());

        appendEvent(task.executionId(), baseSeq + 1, EventTypes.WORKFLOW_TASK_SCHEDULED,
                mapper.createObjectNode().put("triggeredBySeq", baseSeq).toString());

        // 5. Wake the workflow.
        boolean enqueued = enqueueDecisionTask(task, baseSeq + 1);

        log.info("Committed ACTIVITY_FAILED for execution {} (task {}, seq {}); "
                        + "decision task enqueued={}",
                task.executionId(), task.taskId(), baseSeq, enqueued);
        return true;
    }

    private long reserveSequenceBlock(UUID executionId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, executionId, count);
        if (row == null) {
            // Unreachable: the version bump above proved the row exists.
            throw new IllegalStateException(
                    "Execution " + executionId + " vanished mid-transaction");
        }
        return row.get(0, Long.class);
    }

    private void appendEvent(UUID executionId, long seq, String type, String payload) {
        dsl.execute(APPEND_EVENT_SQL, executionId, seq, type, payload);
    }

    /**
     * Enqueues the decision task, inheriting the queue from the failed activity.
     *
     * <p>Inheriting rather than defaulting keeps the execution pinned to the worker pool it
     * was started on. Falling back to {@code "default"} would silently migrate executions
     * off dedicated queues the first time an activity failed - a routing bug that only
     * appears under failure conditions, which is the worst time to discover it.</p>
     */
    private boolean enqueueDecisionTask(LeasedTask task, long scheduledSeq) {
        Record source = dsl.fetchOne(SOURCE_QUEUE_SQL, task.taskId());
        String taskQueue = source == null ? "default" : source.get("task_queue", String.class);
        return dsl.execute(ENQUEUE_DECISION_SQL,
                task.executionId(),
                ShardAssignment.shardFor(task.executionId()),
                taskQueue,
                scheduledSeq) == 1;
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_FAILURE_CHARS ? s : s.substring(0, MAX_FAILURE_CHARS);
    }
}
