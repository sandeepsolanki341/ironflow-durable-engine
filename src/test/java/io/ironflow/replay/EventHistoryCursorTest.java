package io.ironflow.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the replay cursor. No database - this is pure data-structure logic, and
 * the properties it guarantees are the ones the whole engine rests on.
 */
class EventHistoryCursorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final UUID EXEC = UUID.randomUUID();

    private static HistoryEvent event(long seq, String type, String json) {
        try {
            return new HistoryEvent(seq, type, MAPPER.readTree(json), Instant.now());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The out-of-order case. An activity scheduled at seq 5 completes at seq 40, behind the
     * outcomes for 8 and 12. An implementation that scans forward from the cursor instead of
     * using the index fails here and only here.
     */
    @Test
    void outcomesResolveOutOfOrder() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(1, EventTypes.WORKFLOW_STARTED, "{}"),
                event(5, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"slow\"}"),
                event(8, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"fast\"}"),
                event(12, EventTypes.ACTIVITY_COMPLETED,
                        "{\"scheduledEventSeq\":8,\"result\":\"fast-done\"}"),
                event(40, EventTypes.ACTIVITY_COMPLETED,
                        "{\"scheduledEventSeq\":5,\"result\":\"slow-done\"}")));

        assertThat(cursor.outcomeFor(5)).isPresent()
                .get().extracting(e -> e.payload().path("result").asText())
                .isEqualTo("slow-done");
        assertThat(cursor.outcomeFor(8)).isPresent()
                .get().extracting(e -> e.payload().path("result").asText())
                .isEqualTo("fast-done");
    }

    /** A mismatched command type must be caught, not silently accepted. */
    @Test
    void divergentCommandTypeIsRejected() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(2, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"stepA\"}")));

        assertThatThrownBy(() ->
                cursor.nextCommand(EventTypes.TIMER_STARTED, "__timer"))
                .isInstanceOf(NonDeterministicError.class)
                .hasMessageContaining("ACTIVITY_SCHEDULED")
                .hasMessageContaining("TIMER_STARTED");
    }

    /** A mismatched identity at the same position is equally divergent. */
    @Test
    void divergentCommandIdentityIsRejected() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(2, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"chargeCard\"}")));

        assertThatThrownBy(() ->
                cursor.nextCommand(EventTypes.ACTIVITY_SCHEDULED, "reserveInventory"))
                .isInstanceOfSatisfying(NonDeterministicError.class, e -> {
                    assertThat(e.getExpectedFromHistory()).contains("chargeCard");
                    assertThat(e.getAttemptedByCode()).contains("reserveInventory");
                    assertThat(e.getHistorySeq()).isEqualTo(2);
                });
    }

    /** Exhausted cursor means new work, not an error. */
    @Test
    void exhaustedCursorReturnsEmpty() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(1, EventTypes.WORKFLOW_STARTED, "{}")));

        assertThat(cursor.nextCommand(EventTypes.ACTIVITY_SCHEDULED, "anything")).isEmpty();
        assertThat(cursor.isReplaying()).isFalse();
    }

    /**
     * Duplicate outcomes mean the engine double-applied a completion. That is an engine bug,
     * not a code-change problem, so it must be distinguishable from divergence.
     */
    @Test
    void duplicateOutcomeIsRejectedAsCorruption() {
        assertThatThrownBy(() -> EventHistoryCursor.from(EXEC, List.of(
                event(2, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"a\"}"),
                event(3, EventTypes.ACTIVITY_COMPLETED,
                        "{\"scheduledEventSeq\":2,\"result\":1}"),
                event(4, EventTypes.ACTIVITY_COMPLETED,
                        "{\"scheduledEventSeq\":2,\"result\":2}"))))
                .isInstanceOf(CorruptHistoryException.class)
                .hasMessageContaining("Duplicate outcome");
    }

    @Test
    void outcomeWithoutScheduledSeqIsRejected() {
        assertThatThrownBy(() -> EventHistoryCursor.from(EXEC, List.of(
                event(3, EventTypes.ACTIVITY_COMPLETED, "{\"result\":1}"))))
                .isInstanceOf(CorruptHistoryException.class)
                .hasMessageContaining("scheduledEventSeq");
    }

    /** Signals must not consume cursor positions - they have no causing command. */
    @Test
    void signalsAreNotCommandEvents() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(1, EventTypes.WORKFLOW_STARTED, "{}"),
                event(2, EventTypes.SIGNAL_RECEIVED, "{\"signalName\":\"approval\"}"),
                event(3, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"stepA\"}")));

        assertThat(cursor.commandCount())
                .as("only ACTIVITY_SCHEDULED counts as a command")
                .isEqualTo(1);
        assertThat(cursor.nextCommand(EventTypes.ACTIVITY_SCHEDULED, "stepA")).isPresent();
    }

    @Test
    void highWaterSeqTracksMaximum() {
        var cursor = EventHistoryCursor.from(EXEC, List.of(
                event(1, EventTypes.WORKFLOW_STARTED, "{}"),
                event(40, EventTypes.ACTIVITY_SCHEDULED, "{\"identity\":\"x\"}")));

        assertThat(cursor.highWaterSeq()).isEqualTo(40);
    }
}
