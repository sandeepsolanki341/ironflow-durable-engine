package io.ironflow.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.DecisionOutcome;
import io.ironflow.replay.HistoryEvent;
import io.ironflow.replay.ReplayRunner;
import io.ironflow.replay.WorkflowRegistry;
import io.ironflow.sdk.ActivityFailure;
import io.ironflow.sdk.Workflow;
import io.ironflow.sdk.WorkflowContext;
import io.ironflow.sdk.WorkflowFuture;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for parallel-branch replay semantics, driving ReplayRunner against synthetic
 * histories. No database - this isolates the fan-out/fan-in logic from the queue so the
 * assertions attribute cleanly.
 *
 * <p>These use a hand-built ReplayRunner with a one-workflow registry, so the tests exercise
 * the real ReplayContext, cursor, and command emission.</p>
 */
class ParallelBranchReplayTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // A workflow that fans out three activities and awaits all of them.
    static final class FanOutWorkflow implements Workflow<Void, String> {
        @Override public String type() { return "FanOut"; }
        @Override public Class<Void> inputType() { return Void.class; }
        @Override public String run(Void in, WorkflowContext ctx) {
            WorkflowFuture<Integer> a = ctx.async("branchA", Integer.class);
            WorkflowFuture<Integer> b = ctx.async("branchB", Integer.class);
            WorkflowFuture<Integer> c = ctx.async("branchC", Integer.class);
            ctx.awaitAll(a, b, c);
            return "sum=" + (ctx.get(a) + ctx.get(b) + ctx.get(c));
        }
    }

    private ReplayRunner runnerFor(Workflow<?, ?> wf) {
        var registry = new WorkflowRegistry(List.of(wf));
        return new ReplayRunner(registry, MAPPER, java.time.Duration.ofSeconds(30));
    }

    private HistoryEvent ev(long seq, String type, String json) {
        try {
            return new HistoryEvent(seq, type, MAPPER.readTree(json), Instant.now());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    /**
     * The headline: the first decision must emit all THREE schedule commands at once, in one
     * outcome. This is what makes the fan-out atomic and parallel.
     */
    @Test
    void firstDecisionEmitsAllThreeSchedulesTogether() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = new ArrayList<>(List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}")));

        var outcome = runnerFor(new FanOutWorkflow())
                .replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(outcome.kind()).isEqualTo(DecisionOutcome.Kind.PROGRESSING);
        assertThat(outcome.commands())
                .as("all three async branches must be scheduled in one decision")
                .hasSize(3);
        assertThat(outcome.commands())
                .allMatch(c -> c instanceof io.ironflow.replay.Command.ScheduleActivity);
    }

    /** With only two of three branches complete, awaitAll must park (WAITING). */
    @Test
    void barrierParksUntilAllBranchesComplete() {
        UUID exec = UUID.randomUUID();
        // Branches were scheduled at seqs 2,3,4. Two have completed; branchC has not.
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchA\"}"),
                ev(3, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchB\"}"),
                ev(4, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchC\"}"),
                ev(5, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":10}"),
                ev(6, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":3,\"result\":20}"));

        var outcome = runnerFor(new FanOutWorkflow())
                .replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(outcome.kind())
                .as("the barrier must hold until the slowest branch lands")
                .isEqualTo(DecisionOutcome.Kind.WAITING);
        assertThat(outcome.commands()).isEmpty();
    }

    /** Completions arriving OUT OF ORDER must still resolve correctly by scheduledEventSeq. */
    @Test
    void outOfOrderCompletionsResolveAndReleaseBarrier() {
        UUID exec = UUID.randomUUID();
        // branchC (seq 4) completes first, branchA (seq 2) last. Barrier still releases and
        // each future resolves to ITS OWN result, not by arrival order.
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchA\"}"),
                ev(3, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchB\"}"),
                ev(4, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchC\"}"),
                ev(20, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":4,\"result\":3}"),
                ev(21, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":1}"),
                ev(22, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":3,\"result\":2}"));

        var outcome = runnerFor(new FanOutWorkflow())
                .replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(outcome.kind()).isEqualTo(DecisionOutcome.Kind.COMPLETED);
        assertThat(outcome.result().asText())
                .as("get(a)+get(b)+get(c) = 1+2+3, matched by seq not arrival order")
                .isEqualTo("sum=6");
    }

    /** A failed branch must make awaitAll throw, surfacing as workflow failure here. */
    @Test
    void failedBranchPropagatesThroughAwaitAll() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchA\"}"),
                ev(3, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchB\"}"),
                ev(4, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchC\"}"),
                ev(5, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":10}"),
                ev(6, "ACTIVITY_FAILED",
                        "{\"scheduledEventSeq\":3,\"failure\":\"branchB blew up\"}"),
                ev(7, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":4,\"result\":30}"));

        var outcome = runnerFor(new FanOutWorkflow())
                .replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(outcome.kind()).isEqualTo(DecisionOutcome.Kind.FAILED);
        assertThat(outcome.failure()).contains("ActivityFailure");
    }

    /**
     * Fail-fast: a failure must throw even while a sibling branch is still pending, rather
     * than waiting for the slow branch to land first.
     */
    @Test
    void failurePropagatesBeforeSlowSiblingCompletes() {
        UUID exec = UUID.randomUUID();
        // branchB failed; branchC has NOT completed. awaitAll must still throw now.
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchA\"}"),
                ev(3, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchB\"}"),
                ev(4, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchC\"}"),
                ev(5, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":10}"),
                ev(6, "ACTIVITY_FAILED",
                        "{\"scheduledEventSeq\":3,\"failure\":\"fast failure\"}"));
        // note: no completion for branchC (seq 4)

        var outcome = runnerFor(new FanOutWorkflow())
                .replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(outcome.kind())
                .as("Promise.all rejects as soon as any branch fails")
                .isEqualTo(DecisionOutcome.Kind.FAILED);
    }

    /**
     * Determinism: replaying the same completed history twice must produce the identical
     * result. This is the property the whole scheme rests on.
     */
    @Test
    void replayIsDeterministic() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchA\"}"),
                ev(3, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchB\"}"),
                ev(4, "ACTIVITY_SCHEDULED", "{\"identity\":\"branchC\"}"),
                ev(5, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":7}"),
                ev(6, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":3,\"result\":8}"),
                ev(7, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":4,\"result\":9}"));

        var runner = runnerFor(new FanOutWorkflow());
        var first = runner.replay(exec, "FanOut", MAPPER.nullNode(), history);
        var second = runner.replay(exec, "FanOut", MAPPER.nullNode(), history);

        assertThat(second.result()).isEqualTo(first.result());
        assertThat(first.result().asText()).isEqualTo("sum=24");
    }
}
