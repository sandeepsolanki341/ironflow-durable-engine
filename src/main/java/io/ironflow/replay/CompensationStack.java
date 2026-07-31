package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The saga's rollback stack, DERIVED from history rather than held in memory.
 *
 * <h2>Why derived, not stored</h2>
 *
 * <p>A workflow that completes six steps and then fails must roll those six steps back - even
 * if the worker running it crashed at the moment of failure. If the compensation stack lived
 * in a field on the workflow object, it would be rebuilt empty on the next replay, and the
 * rollback the saga exists to perform would never happen. So the stack is reconstructed by
 * scanning history: every {@code COMPENSATION_REGISTERED} is a push, and the LIFO order is
 * simply reverse registration order.</p>
 *
 * <h2>What "outstanding" means during a rollback</h2>
 *
 * <p>Rollback runs one compensation per decision cycle. As each finishes, a
 * {@code COMPENSATION_COMPLETED} event records the registration sequence it discharged. So the
 * <em>outstanding</em> stack is: all registrations, minus those already completed, in reverse
 * registration order. When it is empty, the rollback is done and the execution becomes
 * {@code FAILED_COMPENSATED}.</p>
 *
 * <p>This is the same event-sourced discipline as {@link SignalInbox} and
 * {@link EventHistoryCursor}: rebuilt identically from the same history on every replay, so a
 * partially-completed rollback resumes exactly where it left off after a crash.</p>
 */
public final class CompensationStack {

    /** One registered compensation, in registration order (index 0 = earliest). */
    public record Entry(long registrationSeq, String compensationType, JsonNode input) { }

    private final List<Entry> registrations;
    private final Set<Long> completedSeqs;

    private CompensationStack(List<Entry> registrations, Set<Long> completedSeqs) {
        this.registrations = registrations;
        this.completedSeqs = completedSeqs;
    }

    /**
     * Derives the stack from history.
     *
     * @throws CorruptHistoryException if a COMPENSATION_REGISTERED lacks a type, or a
     *         COMPENSATION_COMPLETED references a registration seq that was never registered
     */
    public static CompensationStack from(List<HistoryEvent> history) {
        List<Entry> registrations = new ArrayList<>();
        Set<Long> registeredSeqs = new HashSet<>();
        Set<Long> completed = new HashSet<>();

        for (HistoryEvent e : history) {
            switch (e.eventType()) {
                case EventTypes.COMPENSATION_REGISTERED -> {
                    String type = e.payload().path("compensationType").asText();
                    if (type.isEmpty()) {
                        throw new CorruptHistoryException(
                                "COMPENSATION_REGISTERED at seq %d has no compensationType"
                                        .formatted(e.sequenceNumber()));
                    }
                    registrations.add(new Entry(e.sequenceNumber(), type,
                            e.payload().path("input")));
                    registeredSeqs.add(e.sequenceNumber());
                }
                case EventTypes.COMPENSATION_COMPLETED -> {
                    long regSeq = e.payload().path("registrationSeq").asLong(-1);
                    if (regSeq < 0 || !registeredSeqs.contains(regSeq)) {
                        throw new CorruptHistoryException(
                                "COMPENSATION_COMPLETED at seq %d references unknown "
                                        + "registration seq %d".formatted(
                                                e.sequenceNumber(), regSeq));
                    }
                    completed.add(regSeq);
                }
                default -> { /* not a compensation event */ }
            }
        }
        return new CompensationStack(registrations, completed);
    }

    /** @return {@code true} if any registered compensation has not yet completed. */
    public boolean hasOutstanding() {
        return registrations.stream().anyMatch(e -> !completedSeqs.contains(e.registrationSeq()));
    }

    /**
     * The next compensation to run: the most-recently-registered one not yet completed.
     *
     * <p>LIFO - reverse registration order. If A then B then C were registered and none
     * completed, this returns C first, then B, then A on successive calls (as each prior one
     * gains a COMPENSATION_COMPLETED event in the history a later derivation reads).</p>
     *
     * @return the next entry, or empty if the rollback is complete
     */
    public java.util.Optional<Entry> nextOutstanding() {
        for (int i = registrations.size() - 1; i >= 0; i--) {
            Entry e = registrations.get(i);
            if (!completedSeqs.contains(e.registrationSeq())) {
                return java.util.Optional.of(e);
            }
        }
        return java.util.Optional.empty();
    }

    /** @return how many compensations remain to run. */
    public int outstandingCount() {
        return (int) registrations.stream()
                .filter(e -> !completedSeqs.contains(e.registrationSeq()))
                .count();
    }

    /** @return total registrations, completed or not. */
    public int totalRegistered() {
        return registrations.size();
    }
}
