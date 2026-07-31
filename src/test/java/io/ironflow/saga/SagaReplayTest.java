package io.ironflow.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.Command;
import io.ironflow.replay.DecisionOutcome;
import io.ironflow.replay.HistoryEvent;
import io.ironflow.replay.ReplayRunner;
import io.ironflow.replay.WorkflowRegistry;
import io.ironflow.sdk.ActivityFailure;
import io.ironflow.sdk.Workflow;
import io.ironflow.sdk.WorkflowContext;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Replay-level tests for the saga failure path, driving ReplayRunner against synthetic
 * histories. Proves that a forward failure with registered compensations produces a
 * COMPENSATION_REQUIRED outcome rather than a plain FAILED.
 */
class SagaReplayTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * A workflow that reserves inventory (registering a release compensation), charges a
     * card (registering a refund), then ships - and shipping fails.
     */
    static final class SagaWorkflow implements Workflow<Void, String> {
        @Override public String type() { return "Saga"; }
        @Override public Class<Void> inputType() { return Void.class; }
        @Override public String run(Void in, WorkflowContext ctx) {
            ctx.activity("reserveInventory", "sku-1", String.class);
            ctx.compensateWith("releaseInventory", "sku-1");

            ctx.activity("chargeCard", "cust-1", String.class);
            ctx.compensateWith("refundCard", "cust-1");

            // This one fails permanently. The engine must roll back refund then release.
            ctx.activity("shipOrder", "order-1", String.class);
            return "shipped";
        }
    }

    /** A workflow that registers no compensations - a failure stays a plain FAILED. */
    static final class NoCompensationWorkflow implements Workflow<Void, String> {
        @Override public String type() { return "NoComp"; }
        @Override public Class<Void> inputType() { return Void.class; }
        @Override public String run(Void in, WorkflowContext ctx) {
            ctx.activity("stepA", "x", String.class);   // fails, nothing registered
            return "done";
        }
    }

    private ReplayRunner runnerFor(Workflow<?, ?> wf) {
        return new ReplayRunner(new WorkflowRegistry(List.of(wf)), MAPPER,
                java.time.Duration.ofSeconds(30));
    }

    private HistoryEvent ev(long seq, String type, String json) {
        try {
            return new HistoryEvent(seq, type, MAPPER.readTree(json), Instant.now());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    /**
     * The headline: when a downstream activity fails after compensations were registered,
     * the outcome is COMPENSATION_REQUIRED, not FAILED.
     */
    @Test
    void downstreamFailureWithRegistrationsRequiresCompensation() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                // reserveInventory scheduled + completed
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"reserveInventory\"}"),
                ev(3, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":\"ok\"}"),
                // its compensation registered
                ev(4, "COMPENSATION_REGISTERED",
                        "{\"identity\":\"releaseInventory\",\"compensationType\":\"releaseInventory\",\"input\":\"sku-1\"}"),
                // chargeCard scheduled + completed
                ev(5, "ACTIVITY_SCHEDULED", "{\"identity\":\"chargeCard\"}"),
                ev(6, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":5,\"result\":\"ok\"}"),
                ev(7, "COMPENSATION_REGISTERED",
                        "{\"identity\":\"refundCard\",\"compensationType\":\"refundCard\",\"input\":\"cust-1\"}"),
                // shipOrder scheduled + FAILED (retries exhausted)
                ev(8, "ACTIVITY_SCHEDULED", "{\"identity\":\"shipOrder\"}"),
                ev(9, "ACTIVITY_FAILED",
                        "{\"scheduledEventSeq\":8,\"failure\":\"carrier rejected\"}"));

        var outcome = runnerFor(new SagaWorkflow())
                .replay(exec, "Saga", MAPPER.nullNode(), history);

        assertThat(outcome.kind())
                .as("a failure with outstanding compensations must trigger rollback")
                .isEqualTo(DecisionOutcome.Kind.COMPENSATION_REQUIRED);
        assertThat(outcome.failure()).contains("ActivityFailure");
    }

    /** A failure with no registered compensations stays a plain FAILED. */
    @Test
    void failureWithoutRegistrationsIsPlainFailed() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"stepA\"}"),
                ev(3, "ACTIVITY_FAILED", "{\"scheduledEventSeq\":2,\"failure\":\"boom\"}"));

        var outcome = runnerFor(new NoCompensationWorkflow())
                .replay(exec, "NoComp", MAPPER.nullNode(), history);

        assertThat(outcome.kind())
                .as("no compensations means an ordinary failure")
                .isEqualTo(DecisionOutcome.Kind.FAILED);
    }

    /**
     * compensateWith emits a RecordCompensation command on first execution (when the
     * registration is not yet in history).
     */
    @Test
    void compensateWithEmitsRegistrationCommand() {
        UUID exec = UUID.randomUUID();
        // History up to just after reserveInventory completed; the compensateWith call is new.
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"reserveInventory\"}"),
                ev(3, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":\"ok\"}"));

        var outcome = runnerFor(new SagaWorkflow())
                .replay(exec, "Saga", MAPPER.nullNode(), history);

        // The workflow runs reserveInventory (recorded), then compensateWith (new command),
        // then schedules chargeCard (new command) and parks awaiting it.
        assertThat(outcome.commands())
                .anyMatch(c -> c instanceof Command.RecordCompensation rc
                        && rc.compensationType().equals("releaseInventory"));
    }

    /**
     * Determinism: replaying the same failure history twice yields the same
     * COMPENSATION_REQUIRED outcome.
     */
    @Test
    void compensationDecisionIsDeterministic() {
        UUID exec = UUID.randomUUID();
        List<HistoryEvent> history = List.of(
                ev(1, "WORKFLOW_STARTED", "{\"input\":null}"),
                ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"reserveInventory\"}"),
                ev(3, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":2,\"result\":\"ok\"}"),
                ev(4, "COMPENSATION_REGISTERED",
                        "{\"identity\":\"releaseInventory\",\"compensationType\":\"releaseInventory\",\"input\":\"sku-1\"}"),
                ev(5, "ACTIVITY_SCHEDULED", "{\"identity\":\"chargeCard\"}"),
                ev(6, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":5,\"result\":\"ok\"}"),
                ev(7, "COMPENSATION_REGISTERED",
                        "{\"identity\":\"refundCard\",\"compensationType\":\"refundCard\",\"input\":\"cust-1\"}"),
                ev(8, "ACTIVITY_SCHEDULED", "{\"identity\":\"shipOrder\"}"),
                ev(9, "ACTIVITY_FAILED", "{\"scheduledEventSeq\":8,\"failure\":\"boom\"}"));

        var runner = runnerFor(new SagaWorkflow());
        var first = runner.replay(exec, "Saga", MAPPER.nullNode(), history);
        var second = runner.replay(exec, "Saga", MAPPER.nullNode(), history);

        assertThat(second.kind()).isEqualTo(first.kind())
                .isEqualTo(DecisionOutcome.Kind.COMPENSATION_REQUIRED);
    }
}
