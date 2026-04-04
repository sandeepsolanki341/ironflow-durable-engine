package io.ironflow.signal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.api.ExecutionNotRunningException;
import io.ironflow.api.SignalAlreadyDeliveredException;
import io.ironflow.api.SignalService;
import io.ironflow.replay.EventTypes;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Signal delivery atomicity and deduplication.
 */
@SpringBootTest
@Import(TestFixtures.class)
class SignalDeliveryIT extends AbstractPostgresIT {

    @Autowired SignalService signals;
    @Autowired TestFixtures fixtures;
    @Autowired ObjectMapper mapper;

    private UUID execId;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
        execId = fixtures.newExecution("signal-test");
    }

    /** All four operations must land together. */
    @Test
    void deliveryAppendsEventAndEnqueuesDecisionAtomically() {
        long versionBefore = fixtures.versionOf(execId);

        var result = signals.signal(execId, "approval",
                mapper.valueToTree(Map.of("approved", true)), null);

        assertThat(fixtures.versionOf(execId)).isEqualTo(versionBefore + 1);
        assertThat(fixtures.eventTypesFor(execId))
                .containsSubsequence(EventTypes.SIGNAL_RECEIVED,
                        EventTypes.WORKFLOW_TASK_SCHEDULED);
        assertThat(result.woken()).isTrue();
        assertThat(fixtures.countTasksByKind("WORKFLOW")).isEqualTo(1);

        var payload = fixtures.firstEventPayloadOfType(execId, EventTypes.SIGNAL_RECEIVED);
        assertThat(payload.path("signalName").asText()).isEqualTo("approval");
        assertThat(payload.path("payload").path("approved").asBoolean()).isTrue();
    }

    /** One human click must not become two approvals. */
    @Test
    void duplicateSignalIdIsRejected() {
        signals.signal(execId, "increment", mapper.valueToTree(Map.of("by", 1)), "click-1");

        assertThatThrownBy(() ->
                signals.signal(execId, "increment", mapper.valueToTree(Map.of("by", 1)),
                        "click-1"))
                .isInstanceOf(SignalAlreadyDeliveredException.class);

        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn(EventTypes.SIGNAL_RECEIVED::equals)
                .as("the duplicate must not be appended")
                .hasSize(1);
    }

    /** Dedupe is scoped per execution: the same id on another execution is fine. */
    @Test
    void dedupeIsScopedPerExecution() {
        UUID other = fixtures.newExecution("signal-test-2");

        signals.signal(execId, "x", mapper.createObjectNode(), "shared-id");
        signals.signal(other, "x", mapper.createObjectNode(), "shared-id");

        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn(EventTypes.SIGNAL_RECEIVED::equals).hasSize(1);
        assertThat(fixtures.eventTypesFor(other))
                .filteredOn(EventTypes.SIGNAL_RECEIVED::equals).hasSize(1);
    }

    /** Several signals may accumulate before the workflow reads any. */
    @Test
    void multipleSignalsAccumulateInOrder() {
        for (int i = 1; i <= 3; i++) {
            signals.signal(execId, "value", mapper.valueToTree(Map.of("n", i)), "s" + i);
        }

        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn(EventTypes.SIGNAL_RECEIVED::equals)
                .hasSize(3);

        // Exactly one decision task despite three signals - the one-open-decision index
        // absorbed the extras, which is correct: one replay observes all three.
        assertThat(fixtures.countTasksByKind("WORKFLOW"))
                .as("three signals need only one decision task")
                .isEqualTo(1);
    }

    /** Signalling a closed workflow is a caller error worth surfacing. */
    @Test
    void signalToClosedExecutionIsRejected() {
        fixtures.closeExecution(execId, "COMPLETED");

        assertThatThrownBy(() ->
                signals.signal(execId, "approval", mapper.createObjectNode(), null))
                .isInstanceOf(ExecutionNotRunningException.class);

        assertThat(fixtures.eventTypesFor(execId))
                .doesNotContain(EventTypes.SIGNAL_RECEIVED);
    }

    /** A signal for a not-yet-created execution must be held, not rejected. */
    @Test
    void signalForUnknownBusinessKeyIsBuffered() {
        signals.bufferForFutureExecution("order-not-yet-created", "cancel",
                mapper.valueToTree(Map.of("reason", "user")), "cancel-1");

        assertThat(fixtures.countPendingSignals("order-not-yet-created"))
                .as("the ordering hazard between create and cancel must be absorbed")
                .isEqualTo(1);
    }

    /** Buffering is itself idempotent, so a client retry does not double-buffer. */
    @Test
    void bufferedSignalIsDeduplicatedByKey() {
        signals.bufferForFutureExecution("k", "cancel", mapper.createObjectNode(), "c1");
        signals.bufferForFutureExecution("k", "cancel", mapper.createObjectNode(), "c1");

        assertThat(fixtures.countPendingSignals("k")).isEqualTo(1);
    }
}
