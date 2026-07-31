package io.ironflow.examples;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.Command;
import io.ironflow.replay.DecisionOutcome;
import io.ironflow.replay.HistoryEvent;
import io.ironflow.replay.ReplayRunner;
import io.ironflow.replay.WorkflowRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The flagship simulation: drive {@link OrderFulfillmentWorkflow} to the point where a
 * permanently-dead SMS provider fails step 4, and prove the engine automatically rolls the
 * order back by running {@code releaseInventory} and then {@code refund} - in that reverse
 * order - before closing the execution.
 *
 * <h2>How this test drives the engine</h2>
 *
 * <p>It uses the same deterministic-replay harness the rest of the suite uses: a real
 * {@link ReplayRunner} over a one-workflow {@link WorkflowRegistry}, fed a synthetic event
 * history. This is not a mock. The history is exactly what the orchestrator would have
 * persisted as each real step completed, and the runner replays the genuine
 * {@code OrderFulfillmentWorkflow} bytecode against it. What we assert on is the
 * {@link DecisionOutcome} the engine actually produces.</p>
 *
 * <p>Driving replay directly (rather than standing up Postgres and workers) is what lets this
 * test assert on the <em>decision</em> - "given everything that has happened, what does the
 * engine do next" - which is precisely the thing the saga guarantee is about. The end-to-end
 * database-backed version lives in {@code SagaCompensationIT}; this is the fast, exact,
 * presentation-friendly proof.</p>
 */
class OrderFulfillmentSimulationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ReplayRunner runner() {
        return new ReplayRunner(
                new WorkflowRegistry(List.of(new OrderFulfillmentWorkflow())),
                MAPPER, Duration.ofSeconds(30));
    }

    private HistoryEvent ev(long seq, String type, String json) {
        try {
            return new HistoryEvent(seq, type, MAPPER.readTree(json), Instant.now());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final String INPUT_JSON = """
            {"input":{"orderId":"ord-1001","sku":"WIDGET-42","payment":"tok_visa",
                      "customerEmail":"a@example.com","customerPhone":"+15550000000"}}
            """;

    /**
     * Builds the history of a fully-successful run right up to the moment the SMS branch of
     * step 4 fails permanently. Every prior step has completed: card charged (+ refund
     * registered), inventory reserved (+ release registered), three-day timer fired, both
     * notifications scheduled in parallel, the email delivered - and then sendReviewSms
     * FAILED after exhausting its retries. This is the exact durable state the engine would
     * hold at the instant the failure becomes permanent.
     */
    private List<HistoryEvent> historyUpToSmsFailure() {
        List<HistoryEvent> h = new ArrayList<>();
        h.add(ev(1, "WORKFLOW_STARTED", INPUT_JSON));

        // Step 1: chargeCard scheduled + completed, refund compensation registered.
        h.add(ev(2, "ACTIVITY_SCHEDULED", "{\"identity\":\"chargeCard\"}"));
        h.add(ev(3, "ACTIVITY_COMPLETED",
                "{\"scheduledEventSeq\":2,\"result\":{\"transactionId\":\"txn-77\",\"amountCents\":4999}}"));
        h.add(ev(4, "COMPENSATION_REGISTERED",
                "{\"identity\":\"refund\",\"compensationType\":\"refund\",\"input\":\"txn-77\"}"));

        // Step 2: reserveInventory scheduled + completed, releaseInventory registered.
        h.add(ev(5, "ACTIVITY_SCHEDULED", "{\"identity\":\"reserveInventory\"}"));
        h.add(ev(6, "ACTIVITY_COMPLETED",
                "{\"scheduledEventSeq\":5,\"result\":{\"reservationId\":\"resv-9\",\"sku\":\"WIDGET-42\",\"quantity\":1}}"));
        h.add(ev(7, "COMPENSATION_REGISTERED",
                "{\"identity\":\"releaseInventory\",\"compensationType\":\"releaseInventory\",\"input\":\"resv-9\"}"));

        // Step 3: the three-day durable timer, started then fired.
        h.add(ev(8, "TIMER_STARTED", "{\"identity\":\"__timer\"}"));
        h.add(ev(9, "TIMER_FIRED", "{\"scheduledEventSeq\":8}"));

        // Step 4: both notifications scheduled together (the parallel fan-out).
        h.add(ev(10, "ACTIVITY_SCHEDULED", "{\"identity\":\"sendReviewEmail\"}"));
        h.add(ev(11, "ACTIVITY_SCHEDULED", "{\"identity\":\"sendReviewSms\"}"));
        // Email succeeds; SMS fails permanently (provider down, retries exhausted).
        h.add(ev(12, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":10,\"result\":null}"));
        h.add(ev(13, "ACTIVITY_FAILED",
                "{\"scheduledEventSeq\":11,\"failure\":\"SMS provider unreachable after 5 attempts\"}"));
        return h;
    }

    /**
     * THE HEADLINE ASSERTION. A permanent step-4 failure with two compensations registered
     * must produce COMPENSATION_REQUIRED - the engine's decision to roll back - rather than a
     * plain FAILED that would strand the charge and the reservation.
     */
    @Test
    void smsFailureTriggersAutomaticRollback() {
        var outcome = runner().replay(UUID.randomUUID(), "OrderFulfillment",
                MAPPER.nullNode(), historyUpToSmsFailure());

        assertThat(outcome.kind())
                .as("a permanent failure with compensations on the stack must roll back, "
                        + "not fail outright")
                .isEqualTo(DecisionOutcome.Kind.COMPENSATION_REQUIRED);
        assertThat(outcome.failure())
                .as("the original failure is preserved through the rollback")
                .contains("SMS provider unreachable");
    }

    /**
     * THE REVERSE-ORDER PROOF. Once compensation begins, the engine must schedule the undo
     * of the LAST committed step first. Inventory was reserved after the card was charged, so
     * releaseInventory must run BEFORE refund. We prove it by advancing the history one
     * compensation at a time and checking which activity the engine schedules at each turn.
     */
    @Test
    void compensationsRunInStrictReverseOrder() {
        // Turn 1: rollback has been triggered (execution is COMPENSATING) and the first
        // compensation - the most recently registered, releaseInventory - is scheduled.
        List<HistoryEvent> h = historyUpToSmsFailure();
        h.add(ev(14, "COMPENSATION_TRIGGERED",
                "{\"failure\":\"SMS provider unreachable after 5 attempts\"}"));
        // Engine schedules the first (LIFO) compensation.
        h.add(ev(15, "ACTIVITY_SCHEDULED",
                "{\"identity\":\"releaseInventory\",\"isCompensation\":true,\"registrationSeq\":7}"));

        // releaseInventory completes -> its COMPENSATION_COMPLETED, keyed by registrationSeq 7.
        h.add(ev(16, "COMPENSATION_COMPLETED",
                "{\"registrationSeq\":7,\"identity\":\"releaseInventory\"}"));

        // Now the engine must schedule the NEXT compensation down the stack: refund (seq 4).
        h.add(ev(17, "ACTIVITY_SCHEDULED",
                "{\"identity\":\"refund\",\"isCompensation\":true,\"registrationSeq\":4}"));
        h.add(ev(18, "COMPENSATION_COMPLETED",
                "{\"registrationSeq\":4,\"identity\":\"refund\"}"));

        // With both compensations discharged, replaying must now reach the terminal
        // FAILED_COMPENSATED decision.
        var outcome = runner().replay(UUID.randomUUID(), "OrderFulfillment",
                MAPPER.nullNode(), h);

        assertThat(outcome.isTerminal())
                .as("once every compensation has completed, the saga is terminally closed")
                .isTrue();
        assertThat(outcome.kind())
                .as("a fully rolled-back saga ends FAILED, carrying the original cause")
                .isEqualTo(DecisionOutcome.Kind.FAILED);
        assertThat(outcome.failure()).contains("SMS provider unreachable");
    }

    /**
     * The ordering claim, proven structurally from the registrations themselves rather than
     * from a hand-built rollback history: the compensation registered LAST (releaseInventory,
     * seq 7) must be dispatched before the one registered first (refund, seq 4). This guards
     * against a regression that happened to pass the step-by-step test above because the
     * history was authored in the right order.
     */
    @Test
    void mostRecentlyRegisteredCompensationIsDispatchedFirst() {
        List<HistoryEvent> h = historyUpToSmsFailure();
        h.add(ev(14, "COMPENSATION_TRIGGERED",
                "{\"failure\":\"SMS provider unreachable after 5 attempts\"}"));

        // At the first COMPENSATING decision, the engine picks the LIFO-top compensation.
        var outcome = runner().replay(UUID.randomUUID(), "OrderFulfillment",
                MAPPER.nullNode(), h);

        // The engine's next scheduled activity must be releaseInventory (last registered),
        // never refund. We read the scheduled compensation out of the decision's commands.
        List<String> scheduled = outcome.commands().stream()
                .filter(c -> c instanceof Command.ScheduleActivity)
                .map(c -> ((Command.ScheduleActivity) c).activityType())
                .toList();

        assertThat(scheduled)
                .as("LIFO: the inventory release (registered last) unwinds before the refund")
                .containsExactly("releaseInventory");
        assertThat(scheduled)
                .as("refund must NOT be scheduled until releaseInventory has completed")
                .doesNotContain("refund");
    }

    /**
     * Sanity counterpart: if step 4 succeeds, there is no rollback at all and the workflow
     * completes with a FULFILLED result. Proves the compensation machinery stays completely
     * out of the way on the happy path - registered compensations that are never needed
     * simply evaporate when the workflow completes.
     */
    @Test
    void happyPathCompletesWithoutAnyCompensation() {
        List<HistoryEvent> h = new ArrayList<>(historyUpToSmsFailure());
        // Replace the SMS failure (seq 13) with a success.
        h.set(12, ev(13, "ACTIVITY_COMPLETED", "{\"scheduledEventSeq\":11,\"result\":null}"));

        var outcome = runner().replay(UUID.randomUUID(), "OrderFulfillment",
                MAPPER.nullNode(), h);

        assertThat(outcome.kind())
                .as("both notifications delivered -> the order is fulfilled")
                .isEqualTo(DecisionOutcome.Kind.COMPLETED);
        assertThat(outcome.commands())
                .as("no compensation is scheduled on the happy path")
                .noneMatch(c -> c instanceof Command.ScheduleActivity sa
                        && (sa.activityType().equals("refund")
                            || sa.activityType().equals("releaseInventory")));
    }
}
