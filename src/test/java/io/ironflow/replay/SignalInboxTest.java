package io.ironflow.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the signal inbox. */
class SignalInboxTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static HistoryEvent signal(long seq, String name, String payloadJson) {
        try {
            return new HistoryEvent(seq, EventTypes.SIGNAL_RECEIVED,
                    MAPPER.readTree("{\"signalName\":\"" + name
                            + "\",\"payload\":" + payloadJson + "}"),
                    Instant.now());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Signals of one name are consumed oldest-first. */
    @Test
    void signalsOfSameNameAreFifo() {
        var inbox = SignalInbox.from(List.of(
                signal(10, "approval", "{\"n\":1}"),
                signal(20, "approval", "{\"n\":2}"),
                signal(30, "approval", "{\"n\":3}")));

        assertThat(inbox.consume("approval").orElseThrow().payload().path("n").asInt())
                .isEqualTo(1);
        assertThat(inbox.consume("approval").orElseThrow().payload().path("n").asInt())
                .isEqualTo(2);
        assertThat(inbox.consume("approval").orElseThrow().payload().path("n").asInt())
                .isEqualTo(3);
        assertThat(inbox.consume("approval")).isEmpty();
    }

    /** Different names are independent channels. */
    @Test
    void namesAreIndependentChannels() {
        var inbox = SignalInbox.from(List.of(
                signal(10, "approval", "{}"),
                signal(20, "cancellation", "{}")));

        assertThat(inbox.has("approval")).isTrue();
        assertThat(inbox.has("cancellation")).isTrue();
        inbox.consume("approval");
        assertThat(inbox.has("approval")).isFalse();
        assertThat(inbox.has("cancellation"))
                .as("consuming one name must not drain another")
                .isTrue();
    }

    /** Rebuilding from the same history must produce the same inbox - the determinism check. */
    @Test
    void inboxIsDeterministicAcrossRebuilds() {
        List<HistoryEvent> history = List.of(
                signal(10, "x", "{\"v\":1}"),
                signal(20, "x", "{\"v\":2}"));

        var first = SignalInbox.from(history);
        var second = SignalInbox.from(history);

        assertThat(first.consume("x").orElseThrow().sequenceNumber())
                .isEqualTo(second.consume("x").orElseThrow().sequenceNumber());
    }

    @Test
    void unnamedSignalIsRejected() {
        var bad = new HistoryEvent(5, EventTypes.SIGNAL_RECEIVED,
                MAPPER.createObjectNode(), Instant.now());

        assertThatThrownBy(() -> SignalInbox.from(List.of(bad)))
                .isInstanceOf(CorruptHistoryException.class)
                .hasMessageContaining("signalName");
    }

    @Test
    void hasDoesNotConsume() {
        var inbox = SignalInbox.from(List.of(signal(10, "x", "{}")));
        assertThat(inbox.has("x")).isTrue();
        assertThat(inbox.has("x")).isTrue();
        assertThat(inbox.consume("x")).isPresent();
    }
}
