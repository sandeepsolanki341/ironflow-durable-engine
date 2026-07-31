package io.ironflow.worker;

import io.ironflow.queue.LeaseLostException;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import io.ironflow.replay.CorruptHistoryException;
import io.ironflow.replay.DecisionOutcome;
import io.ironflow.replay.DecisionTimeoutException;
import io.ironflow.replay.DivergenceQuarantine;
import io.ironflow.replay.HistoryEvent;
import io.ironflow.replay.NonDeterministicError;
import io.ironflow.replay.ReplayRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

/**
 * Executes one decision task: replay the workflow, then commit its outcome atomically.
 *
 * <h2>The two-phase split</h2>
 *
 * <p>{@link #execute} replays the workflow <b>outside</b> any transaction, then calls
 * {@link DecisionCommitter#commit} which does everything transactionally. One transaction
 * wrapping both would hold a Postgres backend, a row lock on {@code wf_executions}, and a
 * pooled connection for the whole replay - and at a few dozen concurrent decisions the pool
 * is exhausted and the engine deadlocks against itself.</p>
 *
 * <h2>The exception handling is the containment boundary</h2>
 *
 * <p>Each branch corresponds to a different fault with a different correct response.
 * Collapsing them is how a single bad workflow takes down a fleet.</p>
 */
@Service
public class DecisionTaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(DecisionTaskExecutor.class);

    private static final Duration INFRA_RETRY_BACKOFF = Duration.ofSeconds(5);
    private static final Duration TIMEOUT_BACKOFF = Duration.ofMinutes(5);
    private static final int MAX_FAILURE_CHARS = 4_000;

    private final ExecutionLoader loader;
    private final HistoryReader historyReader;
    private final ReplayRunner replayRunner;
    private final DecisionCommitter committer;
    private final DivergenceQuarantine quarantine;
    private final PostgresTaskQueueRepository queue;
    private final WaitCommitter waitCommitter;

    public DecisionTaskExecutor(ExecutionLoader loader,
                                HistoryReader historyReader,
                                ReplayRunner replayRunner,
                                DecisionCommitter committer,
                                DivergenceQuarantine quarantine,
                                PostgresTaskQueueRepository queue,
                                WaitCommitter waitCommitter) {
        this.loader = loader;
        this.historyReader = historyReader;
        this.replayRunner = replayRunner;
        this.committer = committer;
        this.quarantine = quarantine;
        this.queue = queue;
        this.waitCommitter = waitCommitter;
    }

    /**
     * Runs one decision task. Never throws.
     */
    public void execute(LeasedTask task) {
        try {
            ExecutionLoader.ExecutionContext context = loader.load(task.executionId());
            List<HistoryEvent> history = historyReader.read(task.executionId());

            DecisionOutcome outcome = replayRunner.replay(
                    task.executionId(), context.workflowType(), context.input(), history);

            if (outcome.kind() == DecisionOutcome.Kind.WAITING) {
                // The workflow parked. For a signal or timer wait this is harmless to ack:
                // a future external event will re-enqueue a decision. But parallel branches
                // are the LAST writers, so a completion landing during this replay's window
                // could be absorbed by ON CONFLICT against our still-leased task and then
                // stranded when we ack. WaitCommitter acks AND, atomically, re-enqueues iff
                // history advanced past what this replay observed - closing that race without
                // reintroducing the hot-loop the plain ack avoids (it re-enqueues only when
                // history actually moved, never for a genuine external wait).
                //
                // observedNextSeq is one past the highest event our history read contained,
                // matching wf_executions.next_sequence semantics.
                long observedNextSeq = history.isEmpty()
                        ? 0
                        : history.get(history.size() - 1).sequenceNumber() + 1;
                waitCommitter.commitWait(task, context.taskQueue(), observedNextSeq);
                return;
            }

            committer.commit(task, outcome, context.taskQueue());

        } catch (NonDeterministicError e) {
            // Poison workflow. Quarantine and ACK the task - deliberately not a nack.
            //
            // Nacking would return the task to PENDING, another worker would poll it,
            // replay it, diverge identically, and repeat until max_attempts. Every one of
            // those attempts occupies a worker slot for a full replay, which is throughput
            // stolen from healthy workflows. Since the outcome is deterministic - the same
            // code diverges the same way every time - retrying has no upside.
            quarantine.quarantine(e);
            queue.complete(task.taskId(), task.leaseOwner());

        } catch (CorruptHistoryException e) {
            // History itself is malformed, which implies an engine bug rather than a code
            // change. Retrying cannot help, and quarantining under the divergence banner
            // would send operators hunting for a code mismatch that does not exist.
            log.error("Corrupt history for execution {}; failing task permanently",
                    task.executionId(), e);
            queue.fail(task.taskId(), task.leaseOwner(), TIMEOUT_BACKOFF, describe(e));

        } catch (DecisionTimeoutException e) {
            // Workflow spun without reaching an SDK call. Also poison, also pointless to
            // retry on the same binary, but not a divergence.
            log.error("Decision timeout on execution {}; failing task",
                    task.executionId(), e);
            queue.fail(task.taskId(), task.leaseOwner(), TIMEOUT_BACKOFF, e.getMessage());

        } catch (LeaseLostException e) {
            // Another worker owns this task now. Discard silently; retrying would race them.
            log.warn("Lost lease on task {} before commit: {}", task.taskId(), e.getMessage());

        } catch (Exception e) {
            // Infrastructure. Nothing committed, so retry is safe and correct.
            log.error("Infrastructure failure on decision task {}; nacking",
                    task.taskId(), e);
            try {
                queue.fail(task.taskId(), task.leaseOwner(), INFRA_RETRY_BACKOFF, describe(e));
            } catch (Exception nackFailure) {
                // The database is likely unreachable. Do nothing further: the lease will
                // expire and the reaper reclaims the task once connectivity returns. This
                // is exactly the case the reaper exists for.
                log.error("Could not nack task {}; relying on lease expiry",
                        task.taskId(), nackFailure);
            }
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getClass().getSimpleName()
                + (t.getMessage() == null ? "" : ": " + t.getMessage());
        return msg.length() > MAX_FAILURE_CHARS ? msg.substring(0, MAX_FAILURE_CHARS) : msg;
    }
}
