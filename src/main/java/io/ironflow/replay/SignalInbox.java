package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Name-keyed inbox of signals recorded in history.
 *
 * <h2>Why a separate structure from the command cursor</h2>
 *
 * <p>Every other SDK method pairs a command the workflow issued with an outcome it caused:
 * {@code ACTIVITY_SCHEDULED} then {@code ACTIVITY_COMPLETED}, matched by cursor position.
 * Signals have no preceding command. They arrive unbidden, at whatever sequence number
 * their wall-clock arrival lands on, and - critically - a signal can arrive <em>before</em>
 * the workflow calls {@code waitForSignal} for it.</p>
 *
 * <p>Matching them positionally would break immediately: the same workflow code would
 * consume a different command index depending on when an external caller happened to POST.
 * So signals get their own name-keyed channel.</p>
 *
 * <h2>Determinism still holds</h2>
 *
 * <p>The inbox is rebuilt from the same history on every replay. A signal at sequence 40 is
 * at sequence 40 every time, so the Nth {@code waitForSignal("approval")} returns the Nth
 * recorded approval every time.</p>
 *
 * <h2>Per-name FIFO</h2>
 *
 * <p>Signals of one name are consumed oldest-first. Three approvals arriving before the
 * workflow reads any of them are delivered as three sequential
 * {@code waitForSignal("approval")} calls. Draining a name to empty is what lets the
 * workflow's next call park rather than re-consuming.</p>
 *
 * <p>Not thread-safe; owned by one decision task.</p>
 */
public final class SignalInbox {

    private final Map<String, Deque<SignalRecord>> byName;

    private SignalInbox(Map<String, Deque<SignalRecord>> byName) {
        this.byName = byName;
    }

    /**
     * Builds the inbox from history.
     *
     * @throws CorruptHistoryException if a SIGNAL_RECEIVED event carries no name
     */
    public static SignalInbox from(List<HistoryEvent> history) {
        Map<String, Deque<SignalRecord>> byName = new HashMap<>();
        for (HistoryEvent e : history) {
            if (!EventTypes.SIGNAL_RECEIVED.equals(e.eventType())) {
                continue;
            }
            String name = e.payload().path("signalName").asText();
            if (name.isEmpty()) {
                throw new CorruptHistoryException(
                        "SIGNAL_RECEIVED at seq %d has no signalName"
                                .formatted(e.sequenceNumber()));
            }
            byName.computeIfAbsent(name, k -> new ArrayDeque<>())
                    .addLast(new SignalRecord(e.sequenceNumber(),
                            e.payload().path("payload")));
        }
        return new SignalInbox(byName);
    }

    /** Consumes the oldest unread signal of this name, if any. */
    public Optional<SignalRecord> consume(String signalName) {
        Deque<SignalRecord> queue = byName.get(signalName);
        if (queue == null || queue.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(queue.removeFirst());
    }

    /** Non-consuming check. */
    public boolean has(String signalName) {
        Deque<SignalRecord> queue = byName.get(signalName);
        return queue != null && !queue.isEmpty();
    }

    public record SignalRecord(long sequenceNumber, JsonNode payload) { }
}
