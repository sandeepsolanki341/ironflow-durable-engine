package io.ironflow.orchestrator;

import java.util.UUID;

/**
 * Thrown when an optimistic-lock fence rejects a state transition.
 *
 * <p><b>This is not an error condition.</b> It is the fence working correctly: another
 * orchestrator advanced this execution first, and this node's view of the world is now
 * stale. The losing node must roll back and let the task be redelivered, at which point it
 * recomputes against current state.</p>
 *
 * <h2>Why this is safe to lose</h2>
 *
 * <p>Because the entire transition - version bump, event appends, task ack, next-task
 * enqueue - is one transaction, losing the fence rolls back <em>all</em> of it. There is no
 * partial application to clean up. The loser leaves no trace, which is precisely what makes
 * retry harmless.</p>
 *
 * <h2>Distinguishing contention from a dead end</h2>
 *
 * <p>{@link #getActualVersion()} carries what the row actually held. Two shapes matter
 * operationally:</p>
 *
 * <ul>
 *   <li><b>actual &gt; expected</b> - normal contention. Another orchestrator won. Retry.</li>
 *   <li><b>actual == expected</b> - the version matched but the row was still not updated,
 *       meaning the execution left RUNNING. The workflow is closed; this transition will
 *       never apply and retrying is pointless.</li>
 * </ul>
 *
 * <p>Collapsing those two into one opaque failure is how a terminated workflow ends up
 * retried until {@code max_attempts} is exhausted, burning the retry budget on work that
 * could never succeed.</p>
 */
public class StaleExecutionException extends RuntimeException {

    private final UUID executionId;
    private final long expectedVersion;
    private final Long actualVersion;
    private final String actualStatus;

    public StaleExecutionException(UUID executionId, long expectedVersion,
                                   Long actualVersion, String actualStatus) {
        super(buildMessage(executionId, expectedVersion, actualVersion, actualStatus));
        this.executionId = executionId;
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
        this.actualStatus = actualStatus;
    }

    private static String buildMessage(UUID executionId, long expected,
                                       Long actual, String status) {
        if (actual == null) {
            return "Execution %s not found (expected version %d)"
                    .formatted(executionId, expected);
        }
        if (actual == expected) {
            return ("Execution %s is %s, not RUNNING; transition rejected at version %d. "
                    + "Retrying will not help.").formatted(executionId, status, expected);
        }
        return ("Execution %s advanced concurrently: expected version %d, found %d "
                + "(status %s). Another orchestrator won; retry with fresh state.")
                .formatted(executionId, expected, actual, status);
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public long getExpectedVersion() {
        return expectedVersion;
    }

    /** @return the version actually held, or {@code null} if the row was gone. */
    public Long getActualVersion() {
        return actualVersion;
    }

    public String getActualStatus() {
        return actualStatus;
    }

    /**
     * @return {@code true} if this transition can never succeed, because the execution is
     *         closed or gone. Callers should ack the task and stop rather than retrying.
     */
    public boolean isPermanent() {
        return actualVersion == null || actualVersion == expectedVersion;
    }

    /** @return {@code true} if a retry against fresh state is expected to succeed. */
    public boolean isRetryable() {
        return !isPermanent();
    }
}
