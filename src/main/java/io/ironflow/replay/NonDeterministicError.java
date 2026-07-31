package io.ironflow.replay;

import java.util.UUID;

/**
 * Raised when replayed workflow code diverges from recorded history.
 *
 * <p>Carries the full mismatch - position, expected, actual - because the operator's first
 * question is always "what changed?", and answering it from logs alone means correlating a
 * stack trace against a deploy timeline.</p>
 *
 * <p><b>This is a code problem, not a data problem.</b> The execution's history is valid
 * and its state fully reconstructible. The correct remediation is almost always to roll
 * back the deployment, after which the quarantined executions resume normally.</p>
 */
public class NonDeterministicError extends RuntimeException {

    private final UUID executionId;
    private final int commandIndex;
    private final long historySeq;
    private final String expectedFromHistory;
    private final String attemptedByCode;

    public NonDeterministicError(UUID executionId, int commandIndex, long historySeq,
                                 String expectedFromHistory, String attemptedByCode) {
        super(("Replay divergence in execution %s at command index %d (history seq %d): "
                + "history records %s but the deployed code attempted %s. "
                + "The workflow definition changed while this execution was in flight. "
                + "Roll back the deployment, or ship a version-gated patch, then resume.")
                .formatted(executionId, commandIndex, historySeq,
                        expectedFromHistory, attemptedByCode));
        this.executionId = executionId;
        this.commandIndex = commandIndex;
        this.historySeq = historySeq;
        this.expectedFromHistory = expectedFromHistory;
        this.attemptedByCode = attemptedByCode;
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public int getCommandIndex() {
        return commandIndex;
    }

    public long getHistorySeq() {
        return historySeq;
    }

    /** @return what history recorded, e.g. {@code ACTIVITY_SCHEDULED[chargeCard]}. */
    public String getExpectedFromHistory() {
        return expectedFromHistory;
    }

    /** @return what the deployed code tried, e.g. {@code ACTIVITY_SCHEDULED[reserve]}. */
    public String getAttemptedByCode() {
        return attemptedByCode;
    }

    /** Compact form for the {@code divergence_detail} column. */
    public String toDetail() {
        return "index=%d seq=%d expected=%s actual=%s"
                .formatted(commandIndex, historySeq, expectedFromHistory, attemptedByCode);
    }
}
