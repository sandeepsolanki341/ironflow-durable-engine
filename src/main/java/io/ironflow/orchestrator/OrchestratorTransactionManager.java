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
 *
 * <h2>The split-brain problem this solves</h2>
 *
 * <p>Two orchestrator nodes can believe they own the same execution simultaneously - a
 * network partition heals, a GC pause outlasts a lease, a deploy overlaps old and new pods.
 * Without a fence, both apply their transition, and history ends up with interleaved events
 * describing a sequence the workflow never actually experienced. Replay then produces a
 * different result than the original run, and the determinism guarantee - the property the
 * entire engine rests on - is gone.</p>
 *
 * <p>The conventional fix is a distributed lock (etcd, ZooKeeper, Redis Redlock). We do not
 * use one, for the same reason we do not use a broker: a lock held in a system other than
 * the database cannot be acquired in the same transaction as the write it guards. The lock
 * holder can lose its lock between checking and writing, and no amount of careful ordering
 * closes that window - it is a dual-write in disguise.</p>
 *
 * <p>A compare-and-swap on {@code current_version} closes it completely. The check
 * <em>is</em> the write. There is no window.</p>
 *
 * <h2>Fencing, not locking</h2>
 *
 * <p>This does not <em>prevent</em> a second orchestrator from trying. It guarantees only
 * one <em>succeeds</em>, and that the loser's work is discarded in full. That is a stronger
 * and cheaper property than mutual exclusion - nobody blocks, nobody waits on a lease, and
 * there is no lock service to fail independently of the database.</p>
 *
 * <h2>On the version increment</h2>
 *
 * <p>The increment is one, not two. The version is a fence, not a counter of anything - its
 * only job is to change. Callers must be able to predict the post-transition version to
 * drive the next transition, and any increment other than one forces them to know which
 * transition type just ran; a mismatch then surfaces as a {@link StaleExecutionException}
 * indistinguishable from genuine contention. Event counting is {@code next_sequence}'s job,
 * and it is reserved as a block precisely so the two concerns stay separate.</p>
 *
 * <p>{@link #applyActivityCompletionWithIncrement} exists for callers with a specific
 * reason to deviate.</p>
 */
@Service
public class OrchestratorTransactionManager {

    private static final Logger log =
            LoggerFactory.getLogger(OrchestratorTransactionManager.class);

    /** See the class Javadoc for why this is one. */
    static final long VERSION_INCREMENT = 1L;

    /** ACTIVITY_COMPLETED + WORKFLOW_TASK_SCHEDULED. */
    private static final int EVENTS_PER_COMPLETION = 2;

    /**
     * The fence. A single atomic compare-and-swap.
     *
     * <p>{@code status = 'RUNNING'} is part of the predicate, not a separate check. A
     * SELECT-then-UPDATE would leave a window in which the execution is cancelled between
     * the two statements, and this transition would then resurrect a closed workflow.
     * Folding it into the CAS makes that impossible.</p>
     */
    private static final String FENCE_SQL = """
        UPDATE wf_executions
           SET current_version = current_version + ?
         WHERE id = ?
           AND current_version = ?
           AND status = 'RUNNING'
        """;

    /**
     * Diagnostic read, on the failure path only.
     *
     * <p>Runs after the CAS returned zero rows, to distinguish "someone else won" from
     * "this workflow is closed". Costs one extra query on a path that is already
     * exceptional, and buys callers the ability to stop retrying work that can never
     * succeed.</p>
     */
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

    /**
     * Enqueue of the next decision task.
     *
     * <p>{@code ON CONFLICT DO NOTHING} is load-bearing. The
     * {@code uq_wf_tasks_one_open_decision} partial unique index permits at most one open
     * WORKFLOW task per execution. When several activities complete in parallel, the second
     * and subsequent completions find a decision task already pending - and that is the
     * correct outcome, not an error. Without {@code ON CONFLICT}, parallel activity fan-in
     * would fail the whole transaction and roll back a legitimate completion.</p>
     */
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

    /**
     * Applies an activity completion, fencing against concurrent orchestrators.
     *
     * <h3>Isolation</h3>
     *
     * <p>{@code READ_COMMITTED} is sufficient and deliberate. The CAS provides the
     * serialization; raising to {@code REPEATABLE_READ} or {@code SERIALIZABLE} would add
     * no safety and would introduce serialization failures for callers to handle - trading
     * a clean, diagnosable {@link StaleExecutionException} for an opaque
     * {@code SQLState 40001}.</p>
     *
     * <h3>Propagation</h3>
     *
     * <p>{@code REQUIRES_NEW}. This method's guarantee is that its six operations commit
     * together and independently. Joining a caller's transaction would let unrelated
     * upstream work roll back a successfully fenced transition - the version bump undone
     * while the caller believed it had won.</p>
     *
     * <h3>Lock ordering</h3>
     *
     * <p>The fence runs first, before any {@code wf_tasks} write. Every writer in the
     * engine takes {@code wf_executions} before {@code wf_tasks}, and that consistent
     * ordering is what keeps concurrent transitions deadlock-free rather than
     * merely usually-fine.</p>
     *
     * @param execId          execution to advance
     * @param taskId          the completed activity task's {@code task_uuid}
     * @param result          activity result, embedded in the ACTIVITY_COMPLETED payload
     * @param expectedVersion version this caller last observed. The fence.
     * @throws StaleExecutionException if the fence rejects the transition. Check
     *         {@link StaleExecutionException#isRetryable()} before retrying.
     * @throws TaskNotOwnedException if the task was not LEASED
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public ActivityCompletionResult applyActivityCompletion(UUID execId, UUID taskId,
                                                            JsonNode result,
                                                            long expectedVersion) {
        return applyActivityCompletionWithIncrement(
                execId, taskId, result, expectedVersion, VERSION_INCREMENT);
    }

    /**
     * As {@link #applyActivityCompletion}, with an explicit version increment.
     *
     * <p>Be aware of the cost: every caller in the chain must then know which increment
     * this transition used in order to predict the next {@code expectedVersion}. Prefer
     * threading {@link ActivityCompletionResult#newVersion()} through the call chain.</p>
     *
     * @param versionIncrement must be positive; a fence that does not advance is not a fence
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public ActivityCompletionResult applyActivityCompletionWithIncrement(
            UUID execId, UUID taskId, JsonNode result,
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
        // execution_id is in the predicate as well as task_uuid: a task id from a different
        // execution would otherwise be acked here, silently completing unrelated work.
        if (dsl.execute(ACK_TASK_SQL, taskId, execId) != 1) {
            throw new TaskNotOwnedException(taskId, execId);
        }

        // ---- 3. Reserve a contiguous block of sequence numbers. ----------------------
        // Both events must be contiguous and correctly ordered: replay reads history by
        // sequence number, so ACTIVITY_COMPLETED must precede WORKFLOW_TASK_SCHEDULED.
        long baseSeq = reserveSequenceBlock(execId, EVENTS_PER_COMPLETION);
        long activityCompletedSeq = baseSeq;
        long workflowTaskScheduledSeq = baseSeq + 1;

        // ---- 4. Append ACTIVITY_COMPLETED. -------------------------------------------
        var completedPayload = mapper.createObjectNode();
        completedPayload.put("taskId", taskId.toString());
        completedPayload.set("result", result == null ? mapper.nullNode() : result);
        appendEvent(execId, activityCompletedSeq,
                EventTypes.ACTIVITY_COMPLETED, completedPayload.toString());

        // ---- 5. Append WORKFLOW_TASK_SCHEDULED. --------------------------------------
        // Appended unconditionally, even when the enqueue below is absorbed by the
        // one-open-decision index. History records what the workflow logically decided;
        // whether the queue needed a new row is a physical detail. Replay must see the same
        // event stream either way, or a workflow whose activities happened to complete in
        // parallel would replay differently from one whose did not.
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

    /**
     * Builds a diagnostic exception after the fence rejected the transition.
     *
     * <p>Runs inside the transaction that is about to roll back, which is fine - the read
     * sees committed state from other transactions under READ_COMMITTED, which is exactly
     * what we want to report.</p>
     */
    private StaleExecutionException diagnoseStale(UUID execId, long expectedVersion) {
        Record row = dsl.fetchOne(DIAGNOSE_SQL, execId);
        if (row == null) {
            return new StaleExecutionException(execId, expectedVersion, null, null);
        }
        return new StaleExecutionException(execId, expectedVersion,
                row.get("current_version", Long.class),
                row.get("status", String.class));
    }

    /**
     * Atomically reserves a contiguous block of history sequence numbers.
     *
     * <p>A single {@code UPDATE ... RETURNING}, never a read-then-write: two committers
     * could both read {@code next_sequence = 5} and both write 7, producing colliding event
     * ids that violate {@code uq_wf_events_seq}.</p>
     */
    private long reserveSequenceBlock(UUID execId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, execId, count);
        if (row == null) {
            // Unreachable: the fence already proved this row exists and is RUNNING.
            throw new IllegalStateException(
                    "Execution " + execId + " vanished mid-transaction");
        }
        return row.get(0, Long.class);
    }

    private void appendEvent(UUID execId, long seq, String type, String payload) {
        dsl.execute(APPEND_EVENT_SQL, execId, seq, type, payload);
    }

    /**
     * Enqueues the next decision task, inheriting queue and retry policy from the activity
     * task that triggered it.
     *
     * <p>Inheriting {@code task_queue} rather than defaulting keeps an execution pinned to
     * the worker pool it was started on. Defaulting to {@code "default"} here would
     * silently migrate executions off dedicated queues the first time an activity
     * completed - the kind of routing bug that only shows up under load in production.</p>
     */
    private boolean enqueueDecisionTask(UUID execId, UUID sourceTaskId, long scheduledSeq) {
        Record source = dsl.fetchOne(SOURCE_TASK_SQL, sourceTaskId);
        String taskQueue = source == null ? "default" : source.get("task_queue", String.class);
        int maxAttempts = source == null ? 5 : source.get("max_attempts", Integer.class);

        return dsl.execute(ENQUEUE_DECISION_SQL,
                execId, ShardAssignment.shardFor(execId), taskQueue,
                scheduledSeq, maxAttempts) == 1;
    }
}
