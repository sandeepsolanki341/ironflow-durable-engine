package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/**
 * What a decision task concluded.
 *
 * <p>The {@link Kind#WAITING} case is subtle and load-bearing. A workflow parked with no
 * commands is waiting on the outside world - a signal, or a timer already scheduled. That
 * is NOT the same as making progress with zero commands: the decision task must be acked
 * and NOT re-enqueued, or the worker consumes and re-creates it in a tight loop while the
 * workflow waits days for a human to click approve.</p>
 *
 * @param commands work to schedule. Non-empty for PROGRESSING; may be non-empty for
 *                 COMPLETED, since markers recorded before the final return must still be
 *                 persisted for future replays to reproduce.
 */
public record DecisionOutcome(
        Kind kind,
        List<Command> commands,
        JsonNode result,
        String failure) {

    public enum Kind {
        /** Emitted new commands; the engine should schedule them. */
        PROGRESSING,
        /** Parked with nothing to schedule; waiting on an external event. */
        WAITING,
        COMPLETED,
        FAILED,
        /**
         * The forward path failed but the workflow registered compensations that have not all
         * run. The engine must enter the {@code COMPENSATING} state and roll them back in
         * reverse, rather than closing as {@code FAILED} immediately. Carries the triggering
         * failure in {@link #failure()} plus any final markers in {@link #commands()}.
         */
        COMPENSATION_REQUIRED
    }

    public static DecisionOutcome progressing(List<Command> commands) {
        return new DecisionOutcome(Kind.PROGRESSING, commands, null, null);
    }

    public static DecisionOutcome waiting() {
        return new DecisionOutcome(Kind.WAITING, List.of(), null, null);
    }

    public static DecisionOutcome completed(JsonNode result, List<Command> markers) {
        return new DecisionOutcome(Kind.COMPLETED, markers, result, null);
    }

    public static DecisionOutcome failed(String failure) {
        return new DecisionOutcome(Kind.FAILED, List.of(), null, failure);
    }

    /**
     * The forward workflow failed and rollback is required. {@code commands} carries any
     * compensation registrations recorded on this same decision (so a step that registered
     * its compensation and then immediately failed still has that registration persisted
     * before the rollback derives its stack).
     */
    public static DecisionOutcome compensationRequired(String failure, List<Command> commands) {
        return new DecisionOutcome(Kind.COMPENSATION_REQUIRED, commands, null, failure);
    }

    public boolean isTerminal() {
        return kind == Kind.COMPLETED || kind == Kind.FAILED;
    }
}
