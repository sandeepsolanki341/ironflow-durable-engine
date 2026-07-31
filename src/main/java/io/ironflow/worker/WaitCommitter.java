package io.ironflow.worker;

import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.ShardAssignment;
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
 * Commits the ack of a decision task that parked in a WAITING state, closing a lost-wakeup
 * race that is unique to parallel branches.
 *
 * <h2>The race this exists to close</h2>
 *
 * <p>A decision task replays the workflow OUTSIDE any transaction - a deliberate choice, so
 * the connection is not held for the duration of the replay. It reads history at time T,
 * concludes the workflow is WAITING (some parallel branch has not completed yet), and then
 * acks the task in a separate statement at time T+1.</p>
 *
 * <p>For a workflow waiting on a signal or a timer, that gap is harmless: a future external
 * event - the signal delivery, the timer firing - will re-enqueue a decision task. There is
 * always something left to wake it.</p>
 *
 * <p>Parallel branches are different, because <b>the branches are the last writers</b>. Trace
 * the failure:</p>
 *
 * <ol>
 *   <li>Decision D reads history: branches A and B done, C still running. WAITING.</li>
 *   <li>Branch C commits its {@code ACTIVITY_COMPLETED} and tries to enqueue a decision.
 *       But D is still {@code LEASED}, so {@code uq_wf_tasks_one_open_decision} makes C's
 *       {@code ON CONFLICT DO NOTHING} absorb the insert.</li>
 *   <li>D acks itself to {@code COMPLETED}.</li>
 * </ol>
 *
 * <p>Now C's completion is in history, no decision task is open, and nothing will ever create
 * one. The barrier is satisfiable but the workflow hangs forever. This is the one place the
 * {@code ON CONFLICT} decision-enqueue could strand a workflow, and it only bites parallel
 * fan-in.</p>
 *
 * <h2>The fix: ack and re-check atomically</h2>
 *
 * <p>Instead of a bare ack, this method acks the task AND, in the same transaction, checks
 * whether the execution's history high-water mark ({@code next_sequence}) advanced past what
 * the replay observed. If it did, a completion landed during the replay window, so we
 * re-enqueue a decision to re-evaluate the barrier. If it did not, the ack stands and the
 * workflow genuinely waits on something external.</p>
 *
 * <p>Because the re-check reads {@code next_sequence} inside the same transaction that acks
 * the task, and branch C's completion bumped {@code next_sequence} inside ITS transaction,
 * the two serialize: either C's bump is visible here (we re-enqueue) or C has not yet
 * committed (and will find our task already gone, so its own enqueue creates a fresh
 * decision). The window is closed from both sides.</p>
 *
 * <p>This does not reintroduce the hot-loop the WAITING ack was designed to avoid. A
 * re-enqueue happens ONLY when history actually advanced; a workflow waiting on a human for
 * three days sees no history movement and is acked exactly once, never re-enqueued.</p>
 */
@Service
public class WaitCommitter {

    private static final Logger log = LoggerFactory.getLogger(WaitCommitter.class);

    /**
     * Acks the decision task, gated on lease ownership. Returns 0 if the lease was lost, in
     * which case another worker owns the task and we must not touch it.
     */
    private static final String ACK_TASK_SQL = """
        UPDATE wf_tasks
           SET status = 'COMPLETED', lease_owner = NULL, lease_until = NULL, updated_at = now()
         WHERE id = ? AND lease_owner = ? AND status = 'LEASED'
        """;

    /**
     * Reads the execution's current high-water sequence. This is the number the NEXT event
     * will get, so it strictly increases every time any writer appends history. Comparing it
     * against the value observed during replay tells us whether history advanced.
     */
    private static final String READ_HIGHWATER_SQL = """
        SELECT next_sequence, status FROM wf_executions WHERE id = ?
        """;

    /**
     * Re-enqueues a decision to re-evaluate the barrier. {@code ON CONFLICT DO NOTHING}
     * because a concurrent branch may already have created one in the instant after our ack -
     * which is exactly the outcome we want, so it is not an error.
     */
    private static final String REENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'WORKFLOW', 'PENDING', ?, now(), NULL, 5)
        ON CONFLICT DO NOTHING
        """;

    private final DSLContext dsl;

    public WaitCommitter(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Acks a WAITING decision task and re-enqueues iff history advanced during the replay.
     *
     * @param task            the decision task being acked
     * @param taskQueue       the execution's queue, for any re-enqueue
     * @param observedNextSeq the {@code next_sequence} high-water the replay saw, i.e. one
     *                        past the highest event the replay's history read contained
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW,
                   isolation = Isolation.READ_COMMITTED)
    public void commitWait(LeasedTask task, String taskQueue, long observedNextSeq) {
        if (dsl.execute(ACK_TASK_SQL, task.taskId(), task.leaseOwner()) != 1) {
            // Lease lost; another worker owns this task now. Leave it entirely alone.
            log.debug("Wait-ack skipped for task {}: lease already lost", task.taskId());
            return;
        }

        Record exec = dsl.fetchOne(READ_HIGHWATER_SQL, task.executionId());
        if (exec == null) {
            return;   // execution vanished; nothing to re-evaluate
        }

        String status = exec.get("status", String.class);
        if (!"RUNNING".equals(status)) {
            // Closed (completed/failed/cancelled/divergent) - no barrier left to evaluate.
            return;
        }

        long currentNextSeq = exec.get("next_sequence", Long.class);
        if (currentNextSeq > observedNextSeq) {
            // History advanced during the replay window: at least one completion landed after
            // our replay read its history but before (or racing) our ack. The barrier may now
            // be satisfiable and there may be no open decision, so re-enqueue one.
            long scheduledSeq = currentNextSeq - 1;
            boolean enqueued = dsl.execute(REENQUEUE_DECISION_SQL,
                    task.executionId(),
                    ShardAssignment.shardFor(task.executionId()),
                    taskQueue,
                    scheduledSeq) == 1;
            log.debug("Re-enqueued decision for execution {} after history advanced "
                            + "{}->{} during replay (enqueued={})",
                    task.executionId(), observedNextSeq, currentNextSeq, enqueued);
        }
        // else: history did not move. The workflow genuinely waits on something external
        // (a signal, a not-yet-fired timer). Acked exactly once, no re-enqueue, no hot loop.
    }
}
