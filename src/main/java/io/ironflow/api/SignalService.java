package io.ironflow.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.persistence.model.ExecutionStatus;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.EventTypes;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Delivers external signals into running workflows.
 *
 * <h2>Atomicity</h2>
 *
 * <p>A signal delivery does four things: bump the execution version, record the dedupe key,
 * append {@code SIGNAL_RECEIVED}, and enqueue a decision task. All four or none.</p>
 *
 * <p>The failure modes from splitting them are the same shape as everywhere else in this
 * engine, and worth naming because they are all silent:</p>
 *
 * <ul>
 *   <li><b>Event without decision task</b> - the signal is durably recorded and the workflow
 *       never wakes to see it. An approval that vanishes.</li>
 *   <li><b>Decision task without event</b> - the workflow replays, finds nothing new, parks,
 *       and the task is consumed and re-created in a loop.</li>
 *   <li><b>Dedupe key without event</b> - the signal is recorded as delivered but is not in
 *       history, and the caller's retry is now rejected as a duplicate. The signal is lost
 *       permanently.</li>
 * </ul>
 *
 * <h2>No fence on the version</h2>
 *
 * <p>Unlike a decision commit, signal delivery does <em>not</em> take an
 * {@code expectedVersion}. An external caller has no way to know the current version and
 * should not have to - requiring one would make signal delivery fail spuriously whenever the
 * workflow happened to be mid-decision, which is exactly when signals are most likely to
 * arrive.</p>
 *
 * <p>Safety instead comes from history being append-only and the decision task being
 * idempotent to enqueue. A signal racing a decision commit simply lands at a later sequence
 * number and is observed by the next replay.</p>
 *
 * <p><b>Known consequence:</b> signal-heavy workflows will see more optimistic-lock retries
 * on decisions, since a signal can land between a decision's replay and its commit. Safe,
 * but worth watching if you expect high signal volume.</p>
 */
@Service
public class SignalService {

    private static final Logger log = LoggerFactory.getLogger(SignalService.class);

    private static final String LOAD_TARGET_SQL = """
        SELECT e.status,
               COALESCE((SELECT t.task_queue FROM wf_tasks t
                          WHERE t.execution_id = e.id
                          ORDER BY t.id DESC LIMIT 1), 'default') AS task_queue_hint
          FROM wf_executions e WHERE e.id = ?
        """;

    private static final String BUMP_VERSION_SQL = """
        UPDATE wf_executions SET current_version = current_version + 1
         WHERE id = ? AND status = 'RUNNING'
        """;

    private static final String DEDUPE_SQL = """
        INSERT INTO wf_signal_dedupe (execution_id, signal_id, sequence_number)
        VALUES (?, ?, ?)
        """;

    private static final String RESERVE_SEQ_SQL = """
        UPDATE wf_executions SET next_sequence = next_sequence + ?
         WHERE id = ? RETURNING next_sequence - ?
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    private static final String ENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'WORKFLOW', 'PENDING', ?, now(), NULL, 5)
        ON CONFLICT DO NOTHING
        """;

    private static final String BUFFER_SIGNAL_SQL = """
        INSERT INTO wf_pending_signals (business_key, signal_name, signal_id, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        ON CONFLICT DO NOTHING
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public SignalService(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    /**
     * Delivers a signal to a running execution.
     *
     * @param signalId optional caller-supplied delivery id. Strongly recommended: HTTP
     *                 clients retry, and without this a timed-out request that actually
     *                 succeeded delivers the signal twice - one human click becoming two
     *                 approvals.
     * @throws ExecutionNotFoundException   if the execution does not exist
     * @throws ExecutionNotRunningException if it is closed
     * @throws SignalAlreadyDeliveredException if this signal id was already delivered
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public SignalResult signal(UUID executionId, String signalName,
                               JsonNode payload, String signalId) {

        Record target = dsl.fetchOne(LOAD_TARGET_SQL, executionId);
        if (target == null) {
            throw new ExecutionNotFoundException(executionId);
        }
        String status = target.get("status", String.class);
        if (!ExecutionStatus.RUNNING.name().equals(status)) {
            throw new ExecutionNotRunningException(executionId, status);
        }

        // 1. Version bump. First, for the row lock and for consistent lock ordering
        //    (wf_executions before wf_tasks) with every other writer.
        if (dsl.execute(BUMP_VERSION_SQL, executionId) != 1) {
            // Closed between our read and here.
            throw new ExecutionNotRunningException(executionId, "CLOSED");
        }

        // 2. Reserve sequence numbers: SIGNAL_RECEIVED + WORKFLOW_TASK_SCHEDULED.
        long baseSeq = reserveSequenceBlock(executionId, 2);

        // 3. Dedupe, if the caller supplied an id. Done after reserving so the recorded
        //    sequence number is the real one - an operator investigating a duplicate can
        //    jump straight to the original event.
        if (signalId != null) {
            try {
                dsl.execute(DEDUPE_SQL, executionId, signalId, baseSeq);
            } catch (DuplicateKeyException e) {
                // Already delivered. Roll back and report the original rather than
                // appending a second copy.
                throw new SignalAlreadyDeliveredException(executionId, signalId);
            }
        }

        // 4. Append SIGNAL_RECEIVED. This is what ctx.waitForSignal() consumes.
        var signalPayload = mapper.createObjectNode();
        signalPayload.put("signalName", signalName);
        signalPayload.set("payload", payload == null ? mapper.nullNode() : payload);
        if (signalId != null) {
            signalPayload.put("signalId", signalId);
        }
        dsl.execute(APPEND_EVENT_SQL, executionId, baseSeq,
                EventTypes.SIGNAL_RECEIVED, signalPayload.toString());

        dsl.execute(APPEND_EVENT_SQL, executionId, baseSeq + 1,
                EventTypes.WORKFLOW_TASK_SCHEDULED,
                mapper.createObjectNode().put("triggeredBySeq", baseSeq).toString());

        // 5. Wake the workflow. ON CONFLICT DO NOTHING because a decision may already be
        //    pending - correct, not an error: it will observe this signal when it runs.
        boolean enqueued = dsl.execute(ENQUEUE_DECISION_SQL,
                executionId,
                ShardAssignment.shardFor(executionId),
                target.get("task_queue_hint", String.class),
                baseSeq + 1) == 1;

        log.info("Delivered signal '{}' to execution {} at seq {} (decision enqueued={})",
                signalName, executionId, baseSeq, enqueued);

        return new SignalResult(executionId, signalName, baseSeq, false, enqueued);
    }

    /**
     * Buffers a signal for an execution that does not exist yet.
     *
     * <p>Handles a real ordering hazard: "create order" and "cancel order" are separate calls
     * over separate connections, and nothing guarantees the create commits first. Rejecting
     * the early signal would push a distributed-systems problem onto every caller.</p>
     *
     * <p>Buffered signals are drained into history by {@code WorkflowService.start()}, in the
     * same transaction that creates the execution - so a signal can never be lost in the
     * gap.</p>
     */
    @Transactional
    public void bufferForFutureExecution(String businessKey, String signalName,
                                         JsonNode payload, String signalId) {
        dsl.execute(BUFFER_SIGNAL_SQL, businessKey, signalName, signalId,
                payload == null ? "null" : payload.toString());
        log.info("Buffered signal '{}' for not-yet-created business key '{}'",
                signalName, businessKey);
    }

    private long reserveSequenceBlock(UUID executionId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, executionId, count);
        if (row == null) {
            throw new IllegalStateException("Execution " + executionId + " vanished");
        }
        return row.get(0, Long.class);
    }

    /**
     * @param deduplicated {@code true} if this delivery was suppressed as a retry
     * @param woken        {@code false} when a decision task was already pending
     */
    public record SignalResult(UUID executionId, String signalName, long sequenceNumber,
                               boolean deduplicated, boolean woken) { }
}
