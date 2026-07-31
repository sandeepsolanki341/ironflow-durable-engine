package io.ironflow.worker.retry;

import io.ironflow.sdk.ActivityOptions;

import java.time.Duration;
import java.util.Random;

/**
 * Computes retry delays. Pure function of (options, attempt, taskId).
 *
 * <p>Isolated from the executor so it is directly unit-testable without a database. Retry
 * timing bugs are silent and slow to surface, so they deserve tests that run in
 * microseconds.</p>
 *
 * <h2>Jitter</h2>
 *
 * <p>Backoff without jitter is worse than it looks. When a downstream dependency fails, it
 * typically fails for <em>every</em> in-flight activity at once. Those activities then all
 * retry at exactly {@code t + 1s}, then all at {@code t + 2s}, and so on - a synchronised
 * thundering herd hitting the recovering dependency with a spike precisely as it tries to
 * come back up.</p>
 *
 * <p>The jitter here is <b>full jitter</b>: a uniform draw over {@code [0, computed]}
 * rather than a narrow band around it. Full jitter is the variant AWS measured as producing
 * the lowest contention and completion time; the narrow-band form leaves enough correlation
 * to keep the herd partly synchronised.</p>
 *
 * <p>The trade-off is that a retry can fire almost immediately. That is acceptable - the
 * <em>expected</em> delay still grows exponentially, which is what matters for
 * backpressure.</p>
 *
 * <h2>Determinism</h2>
 *
 * <p>Seeded from {@code taskId} and {@code attempt}, so the delay for a given retry is
 * reproducible. This matters for debugging: an operator reconstructing an incident can
 * verify a task retried when it should have, rather than wondering whether the scheduler
 * misfired.</p>
 */
public final class RetryPolicy {

    /**
     * Exponent ceiling, applied before {@code Math.pow}.
     *
     * <p>Without it, {@code 2.0^1000} is {@code Infinity}, and the subsequent conversion to
     * a {@link Duration} throws - turning a routine retry into an infrastructure error on
     * precisely the task that has failed most often.</p>
     */
    private static final int MAX_EXPONENT = 32;

    private RetryPolicy() { }

    /**
     * @param attempt the attempt that just failed, 1-based
     * @return {@code true} if another attempt is permitted
     */
    public static boolean shouldRetry(ActivityOptions options, int attempt,
                                      Throwable failure) {
        if (attempt >= options.maxAttempts()) {
            return false;
        }
        return !isNonRetryable(options, failure);
    }

    /**
     * Walks the cause chain looking for a non-retryable type.
     *
     * <p>Checking only the top-level exception would miss the common case: frameworks wrap
     * causes, so a {@code ValidationException} arrives as
     * {@code UndeclaredThrowableException -> InvocationTargetException -> ValidationException}.
     * A user who marks {@code ValidationException} non-retryable expects that to work.</p>
     */
    public static boolean isNonRetryable(ActivityOptions options, Throwable failure) {
        if (options.nonRetryableErrors().isEmpty() || failure == null) {
            return false;
        }
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (options.nonRetryableErrors().contains(t.getClass().getSimpleName())) {
                return true;
            }
            if (t.getCause() == t) {
                break;   // self-referential cause; defensive against malformed exceptions
            }
        }
        return false;
    }

    /**
     * Computes the delay before the next attempt.
     *
     * <p>{@code initialInterval * coefficient^(attempt-1)}, capped at {@code maxInterval},
     * then full-jittered.</p>
     *
     * @param attempt the attempt that just failed, 1-based
     * @param taskId  jitter seed, for reproducibility
     */
    public static Duration nextBackoff(ActivityOptions options, int attempt, long taskId) {
        int exponent = Math.min(Math.max(attempt - 1, 0), MAX_EXPONENT);

        double multiplier = Math.pow(options.backoffCoefficient(), exponent);
        double computedMillis = options.initialInterval().toMillis() * multiplier;

        // Clamp before the long conversion: an unclamped value can exceed Long.MAX_VALUE
        // and wrap negative, scheduling a retry in the past.
        long cappedMillis = (long) Math.min(computedMillis, options.maxInterval().toMillis());

        long jittered = fullJitter(cappedMillis, taskId, attempt);
        return Duration.ofMillis(Math.max(jittered, 1));
    }

    /** Deterministic full jitter over {@code [0, ceiling]}. */
    private static long fullJitter(long ceilingMillis, long taskId, int attempt) {
        if (ceilingMillis <= 0) {
            return 0;
        }
        Random rng = new Random(taskId * 31L + attempt);
        return (long) (rng.nextDouble() * ceilingMillis);
    }
}
