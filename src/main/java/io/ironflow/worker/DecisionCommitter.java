package io.ironflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.persistence.model.ExecutionStatus;
import io.ironflow.queue.LeaseLostException;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.Command;
import io.ironflow.replay.CompensationStack;
import io.ironflow.replay.DecisionOutcome;
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

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * Commits the outcome of a decision task atomically.
 *
 * <h2>Why this is a separate bean from the executor</h2>
 *
 * <p>Spring's proxy-based {@code @Transactional} does not intercept self-invocation: if the
 * executor called {@code commit()} on {@code this}, the annotation would be silently ignored
 * and every statement below would run in its own autocommit transaction. That failure is
 * invisible in normal operation and catastrophic during a crash - a worker dying mid-commit
 * would leave events appended with the task unacked, and the redelivered task would append
 * them a second time.</p>
 *
 * <p>Splitting the beans makes the proxy boundary real rather than relying on the next
 * reader knowing this rule.</p>
 *
 * <h2>What the commit transaction contains</h2>
 *
 * <ol>
 *   <li>Optimistic version bump on {@code wf_executions} - the serialization point.</li>
 *   <li>Ack of the decision task, gated on {@code lease_owner}.</li>
 *   <li>Reservation of a gap-free block of history sequence numbers.</li>
 *   <li>Append of one event per command, plus a terminal event if the workflow finished.</li>
 *   <li>Queue rows for each command: activity tasks, timer rows.</li>
 *   <li>Terminal state transition, if the workflow completed or failed.</li>
 * </ol>
 *
 * <p>All of it or none.</p>
 */
@Service
public class DecisionCommitter {

    private static final Logger log = LoggerFactory.getLogger(DecisionCommitter.class);

    private static final int MAX_FAILURE_CHARS = 4_000;
    private static final String TIMER_IDENTITY = "__timer";

    private static final String BUMP_VERSION_SQL = """
        UPDATE wf_executions SET current_version = current_version + 1
         WHERE id = ? AND status = 'RUNNING'
        """;

    private static final String ACK_TASK_SQL = """
        UPDATE wf_tasks
           SET status = 'COMPLETED', lease_owner = NULL, lease_until = NULL,
               updated_at = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    private static final String RESERVE_SEQ_SQL = """
        UPDATE wf_executions SET next_sequence = next_sequence + ?
         WHERE id = ? RETURNING next_sequence - ?
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    private static final String ENQUEUE_TASK_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, ?, 'PENDING', ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """;

    private static final String TRANSITION_TO_COMPENSATING_SQL = """
        UPDATE wf_executions SET status = 'COMPENSATING'
         WHERE id = ? AND status = 'RUNNING'
        """;

    private static final String CLOSE_EXECUTION_SQL = """
        UPDATE wf_executions
           SET status = ?, result = ?, failure = ?, end_time = now()
         WHERE id = ?
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;
    private final CompensationCommitter compensationCommitter;
    private final io.ironflow.metrics.TransitionMetrics metrics;

    public DecisionCommitter(DSLContext dsl, ObjectMapper mapper,
            io.ironflow.metrics.TransitionMetrics metrics,
                             CompensationCommitter compensationCommitter) {
        this.dsl = dsl;
        this.mapper = mapper;
        this.compensationCommitter = compensationCommitter;
        this.metrics = metrics;
    }

    /**
     * Atomically commits a decision outcome.
     *
     * <p>Statement order is load-bearing. The version bump comes first because it takes the
     * row lock that serializes concurrent commits on this execution; taking it last would
     * allow two committers to interleave their event appends before either discovered the
     * conflict. Consistent lock ordering across all writers - always {@code wf_executions}
     * before {@code wf_tasks} - is also what keeps this deadlock-free.</p>
     *
     * @throws LeaseLostException if the execution is no longer RUNNING or this worker no
     *         longer owns the task. Rolls the whole transaction back; this is the
     *         exactly-once gate for decisions.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public void commit(LeasedTask task, DecisionOutcome outcome, String taskQueue) {

        if (dsl.execute(BUMP_VERSION_SQL, task.executionId()) != 1) {
            throw new LeaseLostException(
                    "Execution %s is no longer RUNNING".formatted(task.executionId()));
        }

        if (dsl.execute(ACK_TASK_SQL, task.taskId(), task.leaseOwner()) != 1) {
            throw new LeaseLostException("Task %d no longer owned by %s"
                    .formatted(task.taskId(), task.leaseOwner()));
        }

        List<Command> commands = outcome.commands();

        if (outcome.kind() == DecisionOutcome.Kind.COMPENSATION_REQUIRED) {
            commitCompensationTrigger(task, outcome, taskQueue, commands);
            return;
        }

        // One event per command, plus one terminal event if the workflow finished.
        int eventCount = commands.size() + (outcome.isTerminal() ? 1 : 0);

        if (eventCount > 0) {
            long seq = reserveSequenceBlock(task.executionId(), eventCount);

            for (Command command : commands) {
                applyCommand(task.executionId(), taskQueue, seq++, command);
            }

            if (outcome.isTerminal()) {
                appendTerminalEvent(task.executionId(), seq, outcome);
            }
        }

        if (outcome.isTerminal()) {
            closeExecution(task.executionId(), outcome);
        }

        // One durable state transition (the version bump above) committed successfully. This
        // is the unit the ~5k-10k/sec Postgres ceiling is expressed in and what the k6 load
        // test differentiates to compute throughput.
        metrics.recordTransition();

        if (log.isDebugEnabled()) {
            log.debug("Committed decision for execution {}: kind={} commands={}",
                    task.executionId(), outcome.kind(), commands.size());
        }
    }

    /**
     * Enters the {@code COMPENSATING} state after a forward failure.
     *
     * <p>Persists any compensation registrations recorded on this same decision, appends
     * {@code COMPENSATION_TRIGGERED}, transitions the execution to {@code COMPENSATING}, and
     * enqueues the FIRST compensation activity - the most recently registered one, since
     * rollback is LIFO.</p>
     *
     * <p>Everything is in one transaction with the version bump and task ack already done
     * above, so a crash here leaves the execution untouched and the decision is simply
     * redelivered. The transition to COMPENSATING is what makes subsequent activity
     * completions route through the rollback path instead of the forward path.</p>
     */
    private void commitCompensationTrigger(LeasedTask task, DecisionOutcome outcome,
                                           String taskQueue, List<Command> registrations) {
        UUID executionId = task.executionId();

        // Persist this-turn registrations first (a step that registered its compensation and
        // then failed in the same decision), plus the COMPENSATION_TRIGGERED marker.
        int eventCount = registrations.size() + 1;
        long seq = reserveSequenceBlock(executionId, eventCount);

        for (Command command : registrations) {
            applyCommand(executionId, taskQueue, seq++, command);
        }

        appendEvent(executionId, seq, EventTypes.COMPENSATION_TRIGGERED,
                mapper.createObjectNode()
                        .put("failure", truncate(outcome.failure()))
                        .toString());

        // Transition RUNNING -> COMPENSATING. Non-terminal: no end_time, execution stays live
        // to run its rollback activities.
        int moved = dsl.execute(TRANSITION_TO_COMPENSATING_SQL, executionId);
        if (moved != 1) {
            // Version was already bumped and gated on RUNNING above, so this should be
            // unreachable; if it fires, something transitioned the execution concurrently and
            // rolling back is the safe response.
            throw new LeaseLostException(
                    "Execution %s left RUNNING before COMPENSATING transition"
                            .formatted(executionId));
        }

        // Schedule the first compensation. Read the derived stack from the history we just
        // extended (the registrations above are now committed within this transaction and
        // visible to this connection), so LIFO order is authoritative.
        CompensationStack stack = CompensationStack.from(
                compensationCommitter.readHistory(executionId));
        stack.nextOutstanding().ifPresentOrElse(
                entry -> compensationCommitter.enqueueCompensationActivity(
                        executionId, taskQueue, entry),
                () ->
                    // No compensations after all (all already completed, or registrations
                    // were empty). Nothing to roll back - close FAILED_COMPENSATED directly.
                    compensationCommitter.closeAsFailedCompensated(
                            executionId, outcome.failure()));

        log.info("Execution {} entered COMPENSATING ({} compensation(s) to run)",
                executionId, stack.outstandingCount());
    }

    /**
     * Writes one command's history event and its queue row.
     *
     * <p>The switch is exhaustive over the sealed {@link Command} hierarchy, so adding a
     * command type without handling it here is a compile error rather than a silently
     * dropped command.</p>
     */
    private void applyCommand(UUID executionId, String taskQueue, long seq, Command command) {
        switch (command) {

            case Command.ScheduleActivity sa -> {
                var payload = mapper.createObjectNode();
                payload.put("identity", sa.activityType());
                payload.set("input", sa.input());
                payload.set("options", serializeOptions(sa.options()));
                appendEvent(executionId, seq, EventTypes.ACTIVITY_SCHEDULED,
                        payload.toString());

                // The task payload carries the options so a retry uses the policy the
                // workflow was scheduled with, not whatever config says today.
                var taskPayload = mapper.createObjectNode();
                taskPayload.put("activityType", sa.activityType());
                taskPayload.set("input", sa.input());
                taskPayload.set("options", serializeOptions(sa.options()));

                String queue = sa.options().taskQueue() != null
                        ? sa.options().taskQueue() : taskQueue;

                dsl.execute(ENQUEUE_TASK_SQL,
                        executionId, ShardAssignment.shardFor(executionId), queue,
                        "ACTIVITY", seq,
                        OffsetDateTime.now(ZoneOffset.UTC),
                        taskPayload.toString().getBytes(StandardCharsets.UTF_8),
                        sa.options().maxAttempts());
            }

            case Command.StartTimer st -> {
                appendEvent(executionId, seq, EventTypes.TIMER_STARTED,
                        mapper.createObjectNode()
                                .put("identity", TIMER_IDENTITY)
                                .put("durationMillis", st.duration().toMillis())
                                .put("fireAt", st.fireAt().toString())
                                .toString());

                // status PENDING + not_before in the future means invisible to every
                // poller until the deadline - which is the entire timer mechanism.
                //
                // max_attempts = 1: a timer has no work to retry. If firing it fails, the
                // lease expires and the reaper redelivers, which is the correct recovery.
                dsl.execute(ENQUEUE_TASK_SQL,
                        executionId, ShardAssignment.shardFor(executionId), taskQueue,
                        "TIMER", seq,
                        OffsetDateTime.ofInstant(st.fireAt(), ZoneOffset.UTC),
                        null, 1);
            }

            case Command.RecordMarker rm -> {
                // Markers have no queue row - the value is already known, we only need to
                // remember it so replay reproduces it.
                var payload = mapper.createObjectNode();
                payload.put("identity", rm.name());
                payload.set("value", rm.value());
                appendEvent(executionId, seq, EventTypes.MARKER_RECORDED, payload.toString());
            }

            case Command.RecordCompensation rc -> {
                // A compensation registration is a marker with no queue row: nothing runs
                // now. It carries the compensating activity's type and input so the
                // COMPENSATING state can later schedule it without re-running workflow code.
                // 'identity' is the compensationType so the replay cursor matches it
                // positionally, exactly like an activity's identity.
                var payload = mapper.createObjectNode();
                payload.put("identity", rc.compensationType());
                payload.put("compensationType", rc.compensationType());
                payload.set("input", rc.input());
                appendEvent(executionId, seq, EventTypes.COMPENSATION_REGISTERED,
                        payload.toString());
            }
        }
    }

    private void appendTerminalEvent(UUID executionId, long seq, DecisionOutcome outcome) {
        if (outcome.kind() == DecisionOutcome.Kind.COMPLETED) {
            var payload = mapper.createObjectNode();
            payload.set("result", outcome.result());
            appendEvent(executionId, seq, EventTypes.WORKFLOW_COMPLETED, payload.toString());
        } else {
            appendEvent(executionId, seq, EventTypes.WORKFLOW_FAILED,
                    mapper.createObjectNode()
                            .put("failure", truncate(outcome.failure())).toString());
        }
    }

    private void closeExecution(UUID executionId, DecisionOutcome outcome) {
        boolean succeeded = outcome.kind() == DecisionOutcome.Kind.COMPLETED;
        dsl.execute(CLOSE_EXECUTION_SQL,
                succeeded ? ExecutionStatus.COMPLETED.name() : ExecutionStatus.FAILED.name(),
                succeeded && outcome.result() != null
                        ? outcome.result().toString().getBytes(StandardCharsets.UTF_8) : null,
                truncate(outcome.failure()),
                executionId);
    }

    /**
     * Serializes activity options into the event and task payloads.
     *
     * <p>Durations become millis rather than ISO-8601 strings: the value is read back on
     * every retry, and a numeric field avoids a parse and a whole class of format-drift
     * bugs across engine versions.</p>
     */
    private com.fasterxml.jackson.databind.node.ObjectNode serializeOptions(
            ActivityOptions options) {
        var node = mapper.createObjectNode();
        node.put("taskQueue", options.taskQueue());
        node.put("maxAttempts", options.maxAttempts());
        node.put("initialIntervalMillis", options.initialInterval().toMillis());
        node.put("backoffCoefficient", options.backoffCoefficient());
        node.put("maxIntervalMillis", options.maxInterval().toMillis());
        node.put("timeoutMillis", options.timeout().toMillis());
        var errors = node.putArray("nonRetryableErrors");
        options.nonRetryableErrors().forEach(errors::add);
        return node;
    }

    private long reserveSequenceBlock(UUID executionId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, executionId, count);
        if (row == null) {
            throw new IllegalStateException(
                    "Execution " + executionId + " vanished mid-transaction");
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
