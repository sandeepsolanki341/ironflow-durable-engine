package io.ironflow.worker;

import java.time.Duration;

/**
 * Thrown when an activity exceeds its start-to-close timeout.
 *
 * <p>Retryable by default: a timeout usually means a slow dependency, and the next attempt
 * may well succeed.</p>
 *
 * <p><b>But consider carefully for anything that moves money.</b> A timeout often means the
 * request DID arrive and the response was lost, so retrying can duplicate a side effect
 * that already happened. For those activities, mark this non-retryable via
 * {@code ActivityOptions.withNonRetryableErrors("ActivityTimeoutException")} and use an
 * idempotency key on the downstream call.</p>
 */
public class ActivityTimeoutException extends RuntimeException {

    public ActivityTimeoutException(String activityType, Duration timeout) {
        super("Activity '%s' exceeded its start-to-close timeout of %s"
                .formatted(activityType, timeout));
    }
}
