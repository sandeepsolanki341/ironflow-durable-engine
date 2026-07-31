package io.ironflow.sdk;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Retry and timeout policy for an activity.
 *
 * <h2>Recorded, not resolved</h2>
 *
 * <p>These options are serialized into the {@code ACTIVITY_SCHEDULED} event and read back
 * from history when a retry is scheduled. They are deliberately <em>not</em> re-read from
 * config at retry time.</p>
 *
 * <p>A workflow running for a week must keep the policy it was scheduled with. If retry
 * behaviour were resolved from current config, a deploy that lowered {@code maxAttempts}
 * would retroactively exhaust in-flight activities that were mid-retry - failing workflows
 * that would otherwise have succeeded, for reasons invisible in their history.</p>
 *
 * @param taskQueue          routing hint; {@code null} inherits the workflow's queue
 * @param maxAttempts        total attempts including the first. {@code 1} disables retry.
 * @param initialInterval    delay before the first retry
 * @param backoffCoefficient multiplier per attempt. {@code 1.0} gives fixed-interval
 *                           retry; values below 1.0 are rejected.
 * @param maxInterval        ceiling on the computed backoff
 * @param timeout            start-to-close timeout for a single attempt. Distinct from
 *                           the lease: the lease protects against worker death, this
 *                           protects against a worker that is alive but stuck.
 * @param nonRetryableErrors simple class names that terminate immediately rather than
 *                           retrying. Matched against the exception and all its causes.
 */
public record ActivityOptions(
        String taskQueue,
        int maxAttempts,
        Duration initialInterval,
        double backoffCoefficient,
        Duration maxInterval,
        Duration timeout,
        List<String> nonRetryableErrors) {

    /**
     * Defaults tuned for network calls: five attempts over roughly thirty seconds of
     * backoff, then surface the failure to the workflow.
     */
    public static final ActivityOptions DEFAULT = new ActivityOptions(
            null, 5, Duration.ofSeconds(1), 2.0, Duration.ofMinutes(1),
            Duration.ofMinutes(5), List.of());

    public ActivityOptions {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException(
                    "maxAttempts must be >= 1 (1 disables retry), was " + maxAttempts);
        }
        if (backoffCoefficient < 1.0) {
            throw new IllegalArgumentException(
                    "backoffCoefficient must be >= 1.0; a coefficient below 1 accelerates "
                    + "retries under sustained failure, which is the opposite of backoff. "
                    + "Was " + backoffCoefficient);
        }
        Objects.requireNonNull(initialInterval, "initialInterval");
        Objects.requireNonNull(maxInterval, "maxInterval");
        Objects.requireNonNull(timeout, "timeout");
        if (initialInterval.isNegative() || initialInterval.isZero()) {
            throw new IllegalArgumentException("initialInterval must be positive");
        }
        if (maxInterval.compareTo(initialInterval) < 0) {
            throw new IllegalArgumentException(
                    "maxInterval (%s) must be >= initialInterval (%s)"
                            .formatted(maxInterval, initialInterval));
        }
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        nonRetryableErrors = nonRetryableErrors == null
                ? List.of() : List.copyOf(nonRetryableErrors);
    }

    public static ActivityOptions onQueue(String taskQueue) {
        return DEFAULT.withTaskQueue(taskQueue);
    }

    public ActivityOptions withTaskQueue(String v) {
        return new ActivityOptions(v, maxAttempts, initialInterval, backoffCoefficient,
                maxInterval, timeout, nonRetryableErrors);
    }

    public ActivityOptions withMaxAttempts(int v) {
        return new ActivityOptions(taskQueue, v, initialInterval, backoffCoefficient,
                maxInterval, timeout, nonRetryableErrors);
    }

    public ActivityOptions withInitialInterval(Duration v) {
        return new ActivityOptions(taskQueue, maxAttempts, v, backoffCoefficient,
                maxInterval, timeout, nonRetryableErrors);
    }

    public ActivityOptions withBackoffCoefficient(double v) {
        return new ActivityOptions(taskQueue, maxAttempts, initialInterval, v,
                maxInterval, timeout, nonRetryableErrors);
    }

    public ActivityOptions withMaxInterval(Duration v) {
        return new ActivityOptions(taskQueue, maxAttempts, initialInterval,
                backoffCoefficient, v, timeout, nonRetryableErrors);
    }

    public ActivityOptions withTimeout(Duration v) {
        return new ActivityOptions(taskQueue, maxAttempts, initialInterval,
                backoffCoefficient, maxInterval, v, nonRetryableErrors);
    }

    /**
     * Marks exception types that must not be retried.
     *
     * <p>Retrying a deterministic failure is pure waste: five attempts over five minutes
     * to reach the same outcome, while the workflow's {@code catch} block - which could
     * have compensated immediately - waits. Validation errors, malformed input, and
     * business-rule rejections all belong here.</p>
     */
    public ActivityOptions withNonRetryableErrors(String... simpleClassNames) {
        return new ActivityOptions(taskQueue, maxAttempts, initialInterval,
                backoffCoefficient, maxInterval, timeout, List.of(simpleClassNames));
    }

    /** Convenience for the single-attempt case, which reads badly as withMaxAttempts(1). */
    public ActivityOptions withoutRetry() {
        return withMaxAttempts(1);
    }
}
