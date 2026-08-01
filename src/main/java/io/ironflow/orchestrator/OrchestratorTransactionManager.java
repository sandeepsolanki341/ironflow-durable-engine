package io.ironflow.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.EventTypes;
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
 * Applies workflow state transitions under optimistic concurrency control.
 */
@Service
public class OrchestratorTransactionManager {

    private static final Logger log =
            LoggerFactory.getLogger(OrchestratorTransactionManager.class);

    static final long VERSION_INCREMENT = 1L;
    private static final int EVENTS_PER_COMPLETION = 2;

    private static final String FENCE_SQL = """
        UPDATE wf_executions
           SET current_version = current_version + ?
         WHERE id = ?
           AND current_version = ?
           AND status = 'RUNNING'
        """;

    private static final String DIAGNOSE_SQL = """
        SELECT current_version, status FROM wf_executions WHERE id = ?
        """;

    private static final String ACK_TASK_SQL = """
        UPDATE wf_tasks
           SET status = 'COMPLETED', lease_owner = NULL, lease_until = NULL,
               updated_at = now()
         WHERE task_uuid = ? AND execution_id = ? AND status = 'LEASED'
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

    private static final String ENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'WORKFLOW', 'PENDING', ?, now(), NULL, ?)
        ON CONFLICT DO NOTHING
        """;

    private static final String SOURCE_TASK_SQL = """
        SELECT task_queue, max_attempts FROM wf_tasks WHERE task_uuid = ?
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public OrchestratorTransactionManager(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public ActivityCompletionResult applyActivityCompletion(UUID execId, UUID taskId,
                                                            long scheduledEventSeq, JsonNode result,
                                                            long expectedVersion) {
        return applyActivityCompletionWithIncrement(
                execId, taskId, scheduledEventSeq, result, expectedVersion, VERSION_INCREMENT);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW,
            isolation = Isolation.READ_COMMITTED)
    public ActivityCompletionResult applyActivityCompletionWithIncrement(
            UUID execId, UUID taskId, long scheduledEventSeq, JsonNode result,
            long expectedVersion, long versionIncrement) {

        if (versionIncrement < 1) {
            throw new IllegalArgumentException(
                    "versionIncrement must be positive, was " + versionIncrement);
        }
        if (expectedVersion < 0) {
            throw new IllegalArgumentException(
                    "expectedVersion must be non-negative, was " + expectedVersion);
        }

        // ---- 1. The fence. Everything below is conditional on winning this. -----------
        if (dsl.execute(FENCE_SQL, versionIncrement, execId, expectedVersion) != 1) {
            throw diagnoseStale(execId, expectedVersion);
        }
        long newVersion = expectedVersion + versionIncrement;

        // ---- 2. Ack the completed activity task. -------------------------------------
        if (dsl.execute(ACK_TASK_SQL, taskId, execId) != 1) {
            throw new TaskNotOwnedException(taskId, execId);
        }

        // ---- 3. Reserve a contiguous block of sequence numbers. ----------------------
        long baseSeq = reserveSequenceBlock(execId, EVENTS_PER_COMPLETION);
        long activityCompletedSeq = baseSeq;
        long workflowTaskScheduledSeq = baseSeq + 1;

        // ---- 4. Append ACTIVITY_COMPLETED. -------------------------------------------
        var completedPayload = mapper.createObjectNode();
        completedPayload.put("scheduledEventSeq", scheduledEventSeq); // <-- THIS IS THE FIX
        completedPayload.put("taskId", taskId.toString());
        completedPayload.set("result", result == null ? mapper.nullNode() : result);
        appendEvent(execId, activityCompletedSeq,
                EventTypes.ACTIVITY_COMPLETED, completedPayload.toString());

        // ---- 5. Append WORKFLOW_TASK_SCHEDULED. --------------------------------------
        appendEvent(execId, workflowTaskScheduledSeq, EventTypes.WORKFLOW_TASK_SCHEDULED,
                mapper.createObjectNode()
                        .put("triggeredBySeq", activityCompletedSeq).toString());

        // ---- 6. Enqueue the next decision task. --------------------------------------
        boolean enqueued = enqueueDecisionTask(execId, taskId, workflowTaskScheduledSeq);

        if (log.isDebugEnabled()) {
            log.debug("Applied activity completion: execution={} task={} version {}->{} "
                            + "seq={},{} decisionEnqueued={}",
                    execId, taskId, expectedVersion, newVersion,
                    activityCompletedSeq, workflowTaskScheduledSeq, enqueued);
        }

        return new ActivityCompletionResult(execId, newVersion,
                activityCompletedSeq, workflowTaskScheduledSeq, enqueued);
    }

    private StaleExecutionException diagnoseStale(UUID execId, long expectedVersion) {
        Record row = dsl.fetchOne(DIAGNOSE_SQL, execId);
        if (row == null) {
            return new StaleExecutionException(execId, expectedVersion, null, null);
        }
        return new StaleExecutionException(execId, expectedVersion,
                row.get("current_version", Long.class),
                row.get("status", String.class));
    }

    private long reserveSequenceBlock(UUID execId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, execId, count);
        if (row == null) {
            throw new IllegalStateException(
                    "Execution " + execId + " vanished mid-transaction");
        }
        return row.get(0, Long.class);
    }

    private void appendEvent(UUID execId, long seq, String type, String payload) {
        dsl.execute(APPEND_EVENT_SQL, execId, seq, type, payload);
    }

    private boolean enqueueDecisionTask(UUID execId, UUID sourceTaskId, long scheduledSeq) {
        Record source = dsl.fetchOne(SOURCE_TASK_SQL, sourceTaskId);
        String taskQueue = source == null ? "default" : source.get("task_queue", String.class);
        int maxAttempts = source == null ? 5 : source.get("max_attempts", Integer.class);

        return dsl.execute(ENQUEUE_DECISION_SQL,
                execId, ShardAssignment.shardFor(execId), taskQueue,
                scheduledSeq, maxAttempts) == 1;
    }
}