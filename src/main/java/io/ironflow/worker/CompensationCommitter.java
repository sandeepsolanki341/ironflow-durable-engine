package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.persistence.model.ExecutionStatus;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.CompensationStack;
import io.ironflow.replay.EventTypes;
import io.ironflow.replay.HistoryEvent;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * Drives the saga rollback once an execution is in {@code COMPENSATING}.
 *
 * <h2>Why this is separate from the forward-path committers</h2>
 *
 * <p>Compensation is history-driven mechanics, not workflow code. In the COMPENSATING state
 * the engine does NOT replay the workflow body - there is nothing to replay, the forward path
 * already failed. Instead it walks the derived {@link CompensationStack} in LIFO order,
 * scheduling one compensation activity per cycle, and closes as {@code FAILED_COMPENSATED}
 * when the stack empties. Keeping that logic out of {@link io.ironflow.replay.ReplayRunner}
 * means the rollback cannot accidentally re-execute forward activities.</p>
 *
 * <h2>The completion loop</h2>
 *
 * <p>Each compensation activity completes like any other activity, but its completion is
 * recognised as a compensation (the activity task carries {@code isCompensation=true} and the
 * {@code registrationSeq} it discharges). {@link #applyCompensationCompletion} appends
 * {@code COMPENSATION_COMPLETED}, then either schedules the next outstanding compensation or,
 * when none remain, closes the execution {@code FAILED_COMPENSATED}. One compensation per
 * transaction, exactly like forward activities - so a crash mid-rollback resumes from the
 * last committed compensation.</p>
 */
@Service
public class CompensationCommitter {

    private static final Logger log = LoggerFactory.getLogger(CompensationCommitter.class);

    private static final int MAX_FAILURE_CHARS = 4_000;

    private static final String READ_HISTORY_SQL = """
        SELECT sequence_number, event_type, payload, created_at
          FROM wf_events
         WHERE execution_id = ?
         ORDER BY sequence_number ASC
        """;

    private static final String BUMP_VERSION_COMPENSATING_SQL = """
        UPDATE wf_executions SET current_version = current_version + 1
         WHERE id = ? AND status = 'COMPENSATING'
        """;

    private static final String RESERVE_SEQ_SQL = """
        UPDATE wf_executions SET next_sequence = next_sequence + ?
         WHERE id = ? RETURNING next_sequence - ?
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    private static final String ENQUEUE_COMPENSATION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'ACTIVITY', 'PENDING', ?, now(), ?, ?)
        ON CONFLICT DO NOTHING
        """;

    private static final String ACK_TASK_SQL = """
        UPDATE wf_tasks
           SET status = 'COMPLETED', lease_owner = NULL, lease_until = NULL, updated_at = now()
         WHERE task_uuid = ? AND execution_id = ? AND status = 'LEASED'
        """;

    private static final String CLOSE_FAILED_COMPENSATED_SQL = """
        UPDATE wf_executions
           SET status = 'FAILED_COMPENSATED', failure = ?, end_time = now()
         WHERE id = ? AND status = 'COMPENSATING'
        """;

    /** Default retry budget for a compensation activity. Rollback should try hard. */
    private static final int COMPENSATION_MAX_ATTEMPTS = 5;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public CompensationCommitter(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    /**
     * Enqueues a compensation activity task.
     *
     * <p>The task payload carries {@code isCompensation=true} and the {@code registrationSeq}
     * it discharges, so when it completes the worker routes it here rather than through the
     * forward completion path. {@code scheduled_event_seq} is the registration's own sequence
     * number, which keeps the one-open-decision-style uniqueness clean and gives the
     * completion a stable back-reference.</p>
     */
    void enqueueCompensationActivity(UUID executionId, String taskQueue,
                                     CompensationStack.Entry entry) {
        var payload = mapper.createObjectNode();
        payload.put("activityType", entry.compensationType());
        payload.set("input", entry.input());
        payload.put("isCompensation", true);
        payload.put("registrationSeq", entry.registrationSeq());

        dsl.execute(ENQUEUE_COMPENSATION_SQL,
                executionId,
                ShardAssignment.shardFor(executionId),
                taskQueue,
                entry.registrationSeq(),
                payload.toString().getBytes(StandardCharsets.UTF_8),
                COMPENSATION_MAX_ATTEMPTS);

        log.debug("Scheduled compensation '{}' (registrationSeq {}) for execution {}",
                entry.compensationType(), entry.registrationSeq(), executionId);
    }

    /**
     * Applies a compensation activity's successful completion.
     *
     * <p>Appends {@code COMPENSATION_COMPLETED}, then advances the rollback: schedule the next
     * outstanding compensation, or close {@code FAILED_COMPENSATED} when the stack is empty.
     * All in one transaction, so the rollback is crash-safe step by step.</p>
     *
     * @param registrationSeq the registration this compensation discharged
     * @return {@code true} if applied; {@code false} if the lease was lost or the execution
     *         is no longer COMPENSATING (result discarded)
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public boolean applyCompensationCompletion(UUID executionId, UUID taskId,
                                               long registrationSeq) {
        // Version bump gated on COMPENSATING: if the execution already left that state
        // (concurrent completion closed it), this result is stale and must be discarded.
        if (dsl.execute(BUMP_VERSION_COMPENSATING_SQL, executionId) != 1) {
            log.debug("Execution {} no longer COMPENSATING; discarding compensation result",
                    executionId);
            return false;
        }

        if (dsl.execute(ACK_TASK_SQL, taskId, executionId) != 1) {
            throw new io.ironflow.queue.LeaseLostException(
                    "Compensation task %s no longer LEASED".formatted(taskId));
        }

        long seq = reserveSequenceBlock(executionId, 1);
        appendEvent(executionId, seq, EventTypes.COMPENSATION_COMPLETED,
                mapper.createObjectNode()
                        .put("registrationSeq", registrationSeq)
                        .toString());

        // Re-derive the stack from the history we just extended. The COMPENSATION_COMPLETED
        // above is committed within this transaction and visible to this connection, so the
        // just-discharged compensation is correctly excluded.
        CompensationStack stack = CompensationStack.from(readHistory(executionId));

        String taskQueue = deriveQueue(executionId);
        stack.nextOutstanding().ifPresentOrElse(
                next -> enqueueCompensationActivity(executionId, taskQueue, next),
                () -> {
                    String failure = originalFailure(executionId);
                    closeAsFailedCompensated(executionId, failure);
                    log.info("Execution {} rollback complete; closed FAILED_COMPENSATED",
                            executionId);
                });
        return true;
    }

    /** Closes a COMPENSATING execution as terminal FAILED_COMPENSATED. */
    void closeAsFailedCompensated(UUID executionId, String failure) {
        int seqCount = 1;
        long seq = reserveSequenceBlock(executionId, seqCount);
        appendEvent(executionId, seq, EventTypes.WORKFLOW_FAILED,
                mapper.createObjectNode()
                        .put("failure", truncate(failure))
                        .put("compensated", true)
                        .toString());
        dsl.execute(CLOSE_FAILED_COMPENSATED_SQL, truncate(failure), executionId);
    }

    /**
     * The queue to schedule the next compensation on: the execution's most recent task queue.
     * Keeps the rollback pinned to the same worker pool the forward path used.
     */
    private String deriveQueue(UUID executionId) {
        Record row = dsl.fetchOne("""
                SELECT task_queue FROM wf_tasks
                 WHERE execution_id = ? ORDER BY id DESC LIMIT 1
                """, executionId);
        return row == null ? "default" : row.get("task_queue", String.class);
    }

    List<HistoryEvent> readHistory(UUID executionId) {
        return dsl.fetch(READ_HISTORY_SQL, executionId).map(r -> {
            try {
                return new HistoryEvent(
                        r.get("sequence_number", Long.class),
                        r.get("event_type", String.class),
                        mapper.readTree(r.get("payload", String.class)),
                        r.get("created_at", OffsetDateTime.class).toInstant());
            } catch (Exception e) {
                throw new io.ironflow.replay.CorruptHistoryException(
                        "Cannot parse history for execution " + executionId, e);
            }
        });
    }

    /** Recovers the triggering failure from the COMPENSATION_TRIGGERED event for the close. */
    private String originalFailure(UUID executionId) {
        Record row = dsl.fetchOne("""
                SELECT payload FROM wf_events
                 WHERE execution_id = ? AND event_type = ?
                 ORDER BY sequence_number LIMIT 1
                """, executionId, EventTypes.COMPENSATION_TRIGGERED);
        if (row == null) {
            return "compensated";
        }
        try {
            JsonNode payload = mapper.readTree(row.get("payload", String.class));
            return payload.path("failure").asText("compensated");
        } catch (Exception e) {
            return "compensated";
        }
    }

    private long reserveSequenceBlock(UUID executionId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, executionId, count);
        if (row == null) {
            throw new IllegalStateException("Execution " + executionId + " vanished");
        }
        return row.get(0, Long.class);
    }

    private void appendEvent(UUID executionId, long seq, String type, String payload) {
        dsl.execute(APPEND_EVENT_SQL, executionId, seq, type, payload);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_FAILURE_CHARS ? s : s.substring(0, MAX_FAILURE_CHARS);
    }
}
