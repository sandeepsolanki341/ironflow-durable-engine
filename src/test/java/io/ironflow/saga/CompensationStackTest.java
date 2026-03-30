package io.ironflow.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.CompensationStack;
import io.ironflow.replay.CorruptHistoryException;
import io.ironflow.replay.EventTypes;
import io.ironflow.replay.HistoryEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the derived LIFO compensation stack. No database - this is the pure
 * event-sourcing logic the whole saga engine rests on, and it must be reconstructed
 * identically from the same history on every replay.
 */
class CompensationStackTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static HistoryEvent registered(long seq, String type) {
        try {
            return new HistoryEvent(seq, EventTypes.COMPENSATION_REGISTERED,
                    MAPPER.readTree("{\"compensationType\":\"" + type
                            + "\",\"input\":null}"), Instant.now());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static HistoryEvent completed(long seq, long registrationSeq) {
        try {
            return new HistoryEvent(seq, EventTypes.COMPENSATION_COMPLETED,
                    MAPPER.readTree("{\"registrationSeq\":" + registrationSeq + "}"),
                    Instant.now());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static HistoryEvent other(long seq) {
        return new HistoryEvent(seq, EventTypes.ACTIVITY_COMPLETED,
                MAPPER.createObjectNode(), Instant.now());
    }

    /** The defining property: rollback order is the reverse of registration order. */
    @Test
    void outstandingOrderIsLifo() {
        var stack = CompensationStack.from(List.of(
                registered(10, "releaseInventory"),   // registered first
                registered(20, "refundCard"),         // registered second
                registered(30, "cancelShipping")));   // registered third

        // Nothing completed yet: the most recent registration comes off first.
        assertThat(stack.nextOutstanding()).isPresent()
                .get().extracting(CompensationStack.Entry::compensationType)
                .isEqualTo("cancelShipping");
        assertThat(stack.outstandingCount()).isEqualTo(3);
    }

    /** As completions accrue, the next outstanding walks backward through registrations. */
    @Test
    void completionsAdvanceTheStackInReverse() {
        // cancelShipping (30) already compensated; next should be refundCard (20).
        var stack = CompensationStack.from(List.of(
                registered(10, "releaseInventory"),
                registered(20, "refundCard"),
                registered(30, "cancelShipping"),
                completed(40, 30)));

        assertThat(stack.nextOutstanding()).isPresent()
                .get().extracting(CompensationStack.Entry::compensationType)
                .isEqualTo("refundCard");
        assertThat(stack.outstandingCount()).isEqualTo(2);
    }

    /** When every registration has a completion, the rollback is done. */
    @Test
    void fullyCompensatedStackIsEmpty() {
        var stack = CompensationStack.from(List.of(
                registered(10, "releaseInventory"),
                registered(20, "refundCard"),
                completed(30, 20),
                completed(40, 10)));

        assertThat(stack.hasOutstanding()).isFalse();
        assertThat(stack.nextOutstanding()).isEmpty();
        assertThat(stack.outstandingCount()).isZero();
        assertThat(stack.totalRegistered()).isEqualTo(2);
    }

    /**
     * The crash-recovery property: a stack derived from partial-rollback history resumes
     * exactly where it left off. This is why the stack must be derived, not held in memory.
     */
    @Test
    void partialRollbackResumesDeterministically() {
        List<HistoryEvent> history = List.of(
                registered(10, "a"),
                registered(20, "b"),
                registered(30, "c"),
                completed(40, 30));   // only c compensated; crash here

        // Two independent derivations (as two replays would do) must agree.
        var first = CompensationStack.from(history);
        var second = CompensationStack.from(history);

        assertThat(first.nextOutstanding().orElseThrow().compensationType())
                .isEqualTo(second.nextOutstanding().orElseThrow().compensationType())
                .isEqualTo("b");
    }

    /** No compensations registered means nothing to roll back. */
    @Test
    void emptyHistoryHasNoOutstanding() {
        var stack = CompensationStack.from(List.of(other(1), other(2)));
        assertThat(stack.hasOutstanding()).isFalse();
        assertThat(stack.totalRegistered()).isZero();
    }

    /** Non-compensation events are ignored when deriving the stack. */
    @Test
    void interleavedNonCompensationEventsAreIgnored() {
        var stack = CompensationStack.from(List.of(
                other(1),
                registered(10, "a"),
                other(11),
                registered(20, "b"),
                other(21)));
        assertThat(stack.outstandingCount()).isEqualTo(2);
        assertThat(stack.nextOutstanding().orElseThrow().compensationType()).isEqualTo("b");
    }

    /** A registration with no type is corrupt history, not a silently-skipped entry. */
    @Test
    void registrationWithoutTypeIsRejected() {
        var bad = new HistoryEvent(5, EventTypes.COMPENSATION_REGISTERED,
                MAPPER.createObjectNode(), Instant.now());
        assertThatThrownBy(() -> CompensationStack.from(List.of(bad)))
                .isInstanceOf(CorruptHistoryException.class)
                .hasMessageContaining("compensationType");
    }

    /** A completion referencing an unregistered seq means the engine miscounted - reject. */
    @Test
    void completionForUnknownRegistrationIsRejected() {
        assertThatThrownBy(() -> CompensationStack.from(List.of(
                registered(10, "a"),
                completed(20, 999))))   // 999 was never registered
                .isInstanceOf(CorruptHistoryException.class)
                .hasMessageContaining("unknown registration");
    }

    /** Input payloads are preserved so the compensation can be scheduled with its args. */
    @Test
    void entryPreservesInputPayload() throws Exception {
        var withInput = new HistoryEvent(10, EventTypes.COMPENSATION_REGISTERED,
                MAPPER.readTree("{\"compensationType\":\"refund\",\"input\":{\"amount\":42}}"),
                Instant.now());
        var stack = CompensationStack.from(List.of(withInput));

        assertThat(stack.nextOutstanding().orElseThrow().input().path("amount").asInt())
                .isEqualTo(42);
    }
}
