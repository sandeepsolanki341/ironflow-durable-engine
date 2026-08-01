package io.ironflow.replay;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * A forward-only cursor over one execution's history, with an index for outcome lookup.
 *
 * <h2>The two access patterns</h2>
 *
 * <p>Replay needs both, and conflating them is the classic bug in this component:</p>
 *
 * <ul>
 *   <li><b>Sequential</b> - the workflow's Nth SDK call must match the Nth scheduling event
 *       in history. This is what detects nondeterminism: if the workflow issues a timer
 *       where history records an activity, the code changed underneath a running
 *       workflow.</li>
 *   <li><b>Indexed</b> - given an {@code ACTIVITY_SCHEDULED} at sequence N, find its
 *       {@code ACTIVITY_COMPLETED}. These are <em>not</em> adjacent: activities complete
 *       out of order, so the outcome for seq 5 may sit at seq 40, behind the outcomes for
 *       seqs 8 and 12.</li>
 * </ul>
 *
 * <p>Hence a cursor for the first and a resolved map for the second. Serving outcome
 * lookup by scanning forward from the cursor breaks the moment two activities run in
 * parallel - which is the case the whole design exists to support.</p>
 *
 * <p>Not thread-safe; owned by a single decision task.</p>
 */
public final class EventHistoryCursor {

    /** Scheduling events, in order. The workflow's calls must line up with these. */
    private final List<HistoryEvent> commandEvents;

    /** scheduledEventSeq -> its terminal outcome event, if history has one yet. */
    private final Map<Long, HistoryEvent> outcomes;

    /** Highest sequence number in history; the base for newly reserved ids. */
    private final long highWaterSeq;

    private final UUID executionId;

    private int position;

    private EventHistoryCursor(UUID executionId, List<HistoryEvent> commandEvents,
                               Map<Long, HistoryEvent> outcomes, long highWaterSeq) {
        this.executionId = executionId;
        this.commandEvents = commandEvents;
        this.outcomes = outcomes;
        this.highWaterSeq = highWaterSeq;
    }

    /**
     * Builds a cursor from raw history.
     *
     * <p>Partitions events into commands (things the workflow asked for) and outcomes
     * (things that happened as a result), indexing the latter by the sequence number of the
     * command that caused them. Done once per decision task rather than per SDK call, so a
     * workflow with ten thousand history events still resolves each lookup in O(1).</p>
     *
     * <p>{@code SIGNAL_RECEIVED} is deliberately absent from both partitions - signals have
     * no causing command and are handled by {@link SignalInbox}.</p>
     *
     * @throws CorruptHistoryException if history is internally inconsistent
     */
    public static EventHistoryCursor from(UUID executionId, List<HistoryEvent> history) {
        List<HistoryEvent> commands = new ArrayList<>();
        Map<Long, HistoryEvent> outcomes = new HashMap<>();
        long high = 0;

        for (HistoryEvent e : history) {
            high = Math.max(high, e.sequenceNumber());
            switch (e.eventType()) {
                case EventTypes.ACTIVITY_SCHEDULED, EventTypes.TIMER_STARTED, EventTypes.MARKER_RECORDED, EventTypes.COMPENSATION_REGISTERED -> commands.add(e);
                case EventTypes.ACTIVITY_COMPLETED, EventTypes.ACTIVITY_FAILED, EventTypes.TIMER_FIRED -> {
                    long scheduledSeq = e.payload().path("scheduledEventSeq").asLong(-1);
                    if (scheduledSeq < 0) {
                        throw new CorruptHistoryException(
                                "Outcome event at seq %d has no scheduledEventSeq"
                                        .formatted(e.sequenceNumber()));
                    }
                    HistoryEvent prior = outcomes.put(scheduledSeq, e);
                    if (prior != null) {
                        // Two terminal outcomes for one command means the engine
                        // double-applied a completion. Replay cannot proceed sanely, and
                        // this is an engine bug rather than a code-change problem.
                        throw new CorruptHistoryException(
                                "Duplicate outcome for scheduled seq %d (events %d and %d)"
                                        .formatted(scheduledSeq, prior.sequenceNumber(),
                                                e.sequenceNumber()));
                    }
                }
                default -> {
                    // WORKFLOW_STARTED, WORKFLOW_TASK_SCHEDULED, SIGNAL_RECEIVED etc.
                    // carry no positional replay semantics.
                }
            }
        }
        return new EventHistoryCursor(executionId, commands, outcomes, high);
    }

    /**
     * Advances the cursor, asserting the workflow issued the command history expects.
     *
     * <p>This is the nondeterminism check, and it is deliberately strict. If the workflow
     * issues a different command than history records, the code changed while a workflow
     * was mid-flight, and continuing would produce a history describing a sequence that
     * never happened.</p>
     *
     * @param expectedType the event type the workflow's current call would produce
     * @param identity     command identity (activity type, marker name) for the message
     * @return the recorded scheduling event, or empty if the cursor is exhausted - meaning
     *         this is genuinely new work
     * @throws NonDeterministicError if the recorded command does not match
     */
    public Optional<HistoryEvent> nextCommand(String expectedType, String identity) {
        if (position >= commandEvents.size()) {
            return Optional.empty();
        }
        HistoryEvent recorded = commandEvents.get(position);

        String recordedIdentity = recorded.payload().path("identity").asText("");
        boolean typeMatches = recorded.eventType().equals(expectedType);
        boolean identityMatches = recordedIdentity.isEmpty()
                || recordedIdentity.equals(identity);

        if (!typeMatches || !identityMatches) {
            throw new NonDeterministicError(
                    executionId,
                    position,
                    recorded.sequenceNumber(),
                    describe(recorded.eventType(), recordedIdentity),
                    describe(expectedType, identity));
        }

        position++;
        return Optional.of(recorded);
    }

    /**
     * @return the terminal outcome for a scheduled command, or empty if it is still in
     *         flight - the normal case for whatever the workflow is currently waiting on
     */
    public Optional<HistoryEvent> outcomeFor(long scheduledEventSeq) {
        return Optional.ofNullable(outcomes.get(scheduledEventSeq));
    }

    /**
     * @return {@code true} if the cursor still has recorded commands ahead. Equivalent to
     *         "we are replaying known history" rather than "we are producing new work".
     */
    public boolean isReplaying() {
        return position < commandEvents.size();
    }

    public long highWaterSeq() {
        return highWaterSeq;
    }

    public int position() {
        return position;
    }

    public int commandCount() {
        return commandEvents.size();
    }

    private static String describe(String eventType, String identity) {
        return identity == null || identity.isEmpty()
                ? eventType : "%s[%s]".formatted(eventType, identity);
    }
}
