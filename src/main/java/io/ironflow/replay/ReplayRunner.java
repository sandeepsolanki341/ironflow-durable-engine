package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.sdk.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executes one decision task by replaying a workflow over its history.
 *
 * <h2>The lifecycle of a decision</h2>
 *
 * <ol>
 *   <li>Read the execution's full history and build an {@link EventHistoryCursor} and a
 *       {@link SignalInbox}.</li>
 *   <li>Instantiate a <em>fresh</em> workflow object.</li>
 *   <li>Run {@code run()} from the top on a virtual thread. Recorded outcomes return
 *       immediately; the first unrecorded step parks the thread.</li>
 *   <li>Collect the accumulated commands and return them for atomic commit.</li>
 * </ol>
 *
 * <p>Every decision replays the entire history. That sounds wasteful and mostly is not:
 * replay is pure in-process computation over an already-loaded list, with no I/O. The cost
 * that <em>does</em> matter is reading history from the database, which grows without bound
 * - hence continue-as-new, and hence the sticky-cache work listed in the roadmap.</p>
 *
 * <h2>Thread safety</h2>
 *
 * <p>This service holds no per-decision state; everything lives in the
 * {@link ReplayContext} created per call. One bean serves all concurrent decision tasks.</p>
 */
@Service
public class ReplayRunner {

    private static final Logger log = LoggerFactory.getLogger(ReplayRunner.class);

    private static final int MAX_FAILURE_CHARS = 4_000;

    private final WorkflowRegistry registry;
    private final ObjectMapper mapper;
    private final Duration decisionTimeout;

    public ReplayRunner(WorkflowRegistry registry,
                        ObjectMapper mapper,
                        @Value("${ironflow.replay.decision-timeout:30s}")
                        Duration decisionTimeout) {
        this.registry = registry;
        this.mapper = mapper;
        this.decisionTimeout = decisionTimeout;
    }

    /**
     * Replays a workflow and returns the commands it wants executed next.
     *
     * @param executionId  the execution being advanced
     * @param workflowType registered type name
     * @param input        the execution's original input
     * @param history      full history in sequence order
     * @return the decision outcome: commands to schedule, a wait, or a terminal result
     * @throws NonDeterministicError if the workflow diverged from history. Deliberately not
     *         caught here: the caller quarantines the execution rather than terminating it,
     *         because the execution is recoverable once the code is rolled back.
     * @throws DecisionTimeoutException if the workflow body never reached an SDK call
     */
    public DecisionOutcome replay(UUID executionId, String workflowType,
                                  JsonNode input, List<HistoryEvent> history) {

        Workflow<Object, Object> workflow = registry.resolve(workflowType);
        EventHistoryCursor cursor = EventHistoryCursor.from(executionId, history);
        SignalInbox signals = SignalInbox.from(history);
        CompensationStack compensations = CompensationStack.from(history);

        var resultRef = new AtomicReference<Object>();
        var failureRef = new AtomicReference<Throwable>();
        var contextRef = new AtomicReference<ReplayContext>();

        Thread workflowThread = Thread.ofVirtual()
                .name("ironflow-wf-" + executionId)
                .unstarted(() -> {
                    ReplayContext ctx = contextRef.get();
                    try {
                        Object typedInput = mapper.treeToValue(input, workflow.inputType());
                        resultRef.set(workflow.run(typedInput, ctx));
                    } catch (WorkflowAbandonedException e) {
                        // Normal shutdown of a parked thread. Not a failure.
                    } catch (Throwable t) {
                        failureRef.set(t);
                    } finally {
                        // Releases the decision thread whether the workflow completed,
                        // failed, or was abandoned. Without this in a finally, a workflow
                        // that throws before parking hangs the decision thread until the
                        // timeout - turning a fast failure into a 30-second stall.
                        ReplayContext c = contextRef.get();
                        if (c != null) {
                            c.decisionReady().countDown();
                        }
                    }
                });

        ReplayContext ctx = new ReplayContext(executionId, cursor, signals, compensations,
                mapper,
                LoggerFactory.getLogger("workflow." + workflowType), workflowThread);
        contextRef.set(ctx);

        workflowThread.start();

        boolean settled;
        try {
            settled = ctx.decisionReady().await(
                    decisionTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ctx.abandon();
            throw new IllegalStateException("Interrupted during decision", e);
        }

        if (!settled) {
            ctx.abandon();
            throw new DecisionTimeoutException(executionId, decisionTimeout);
        }

        Throwable failure = failureRef.get();
        if (failure != null) {
            if (failure instanceof NonDeterministicError nd) {
                throw nd;
            }
            if (failure instanceof CorruptHistoryException ch) {
                throw ch;
            }

            // The workflow threw. Whether this becomes a plain FAILED or a saga rollback
            // depends on whether any compensations are still outstanding.
            //
            // Forward-progress commands (new activity schedules, timers) are discarded - the
            // workflow is terminating, so scheduling more forward work would be incoherent.
            // But COMPENSATION_REGISTERED commands from this same decision MUST be kept: a
            // step that registered its compensation and then failed in the same turn needs
            // that registration persisted, or the derived rollback stack loses an entry.
            List<Command> compensationRegistrations = ctx.commands().stream()
                    .filter(c -> c instanceof Command.RecordCompensation)
                    .toList();

            if (ctx.hasOutstandingCompensations()) {
                log.warn("Workflow {} ({}) failed with compensations outstanding; entering "
                        + "COMPENSATING", executionId, workflowType, failure);
                return DecisionOutcome.compensationRequired(
                        describe(failure), compensationRegistrations);
            }

            log.warn("Workflow {} ({}) failed with no compensations to run", executionId,
                    workflowType, failure);
            return DecisionOutcome.failed(describe(failure));
        }

        if (ctx.isParked()) {
            // A workflow parked with NO commands is waiting on the outside world - a
            // signal, or a timer already scheduled. This is NOT the same as progressing
            // with zero commands: the decision task must be acked and NOT re-enqueued, or
            // the worker consumes and re-creates it in a tight loop while the workflow
            // waits days for a human to click approve.
            return ctx.commands().isEmpty()
                    ? DecisionOutcome.waiting()
                    : DecisionOutcome.progressing(ctx.commands());
        }

        // run() returned: the workflow is done. Any markers it recorded on the way out
        // still need committing, so they ride along with the completion.
        return DecisionOutcome.completed(
                mapper.valueToTree(resultRef.get()), ctx.commands());
    }

    private static String describe(Throwable t) {
        String msg = t.getClass().getSimpleName()
                + (t.getMessage() == null ? "" : ": " + t.getMessage());
        return msg.length() > MAX_FAILURE_CHARS ? msg.substring(0, MAX_FAILURE_CHARS) : msg;
    }
}
