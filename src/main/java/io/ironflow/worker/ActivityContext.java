package io.ironflow.worker;

import java.util.UUID;

/**
 * Execution context passed to activity implementations.
 *
 * @param attempt 1-based attempt counter. {@code > 1} means this is a redelivery, which is
 *                the signal that idempotency matters on this invocation. Activities that
 *                can cheaply check "did I already do this?" should branch on it and skip
 *                the check on first attempt.
 */
public record ActivityContext(
        UUID executionId,
        UUID taskId,
        String activityType,
        int attempt,
        int maxAttempts) {

    /** @return {@code true} if a previous attempt of this activity already ran. */
    public boolean isRetry() {
        return attempt > 1;
    }

    /** @return {@code true} if this is the last attempt before the failure is surfaced. */
    public boolean isFinalAttempt() {
        return attempt >= maxAttempts;
    }
}
