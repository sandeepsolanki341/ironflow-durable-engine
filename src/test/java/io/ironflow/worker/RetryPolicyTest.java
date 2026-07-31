package io.ironflow.worker;

import io.ironflow.sdk.ActivityOptions;
import io.ironflow.worker.retry.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pure unit tests for backoff maths. No container, microsecond runtime.
 *
 * <p>Retry timing bugs are silent and slow to surface in integration tests - a wrong
 * exponent looks like "the test is a bit slow" until production. Testing the arithmetic
 * directly is the only way to catch them cheaply.</p>
 */
class RetryPolicyTest {

    @Test
    void backoffGrowsExponentiallyInExpectation() {
        var options = ActivityOptions.DEFAULT
                .withInitialInterval(Duration.ofSeconds(1))
                .withBackoffCoefficient(2.0)
                .withMaxInterval(Duration.ofHours(1));

        // Full jitter makes any single draw unpredictable, so assert on the mean across many
        // task ids. Asserting a single value would produce a flaky test.
        for (int attempt = 1; attempt <= 6; attempt++) {
            final int a = attempt;
            double expectedCeiling = 1000 * Math.pow(2, attempt - 1);
            double mean = IntStream.range(0, 2_000)
                    .mapToLong(id -> RetryPolicy.nextBackoff(options, a, id).toMillis())
                    .average().orElseThrow();

            assertThat(mean)
                    .as("attempt %d: full jitter means E[delay] ~ ceiling/2", attempt)
                    .isBetween(expectedCeiling * 0.35, expectedCeiling * 0.65);
        }
    }

    @Test
    void backoffIsCappedAtMaxInterval() {
        var options = ActivityOptions.DEFAULT
                .withInitialInterval(Duration.ofSeconds(1))
                .withBackoffCoefficient(2.0)
                .withMaxInterval(Duration.ofSeconds(30));

        for (long id = 0; id < 1_000; id++) {
            assertThat(RetryPolicy.nextBackoff(options, 20, id))
                    .isLessThanOrEqualTo(Duration.ofSeconds(30));
        }
    }

    /**
     * The overflow guard. Without the exponent clamp, pow(2, 999) is Infinity and the
     * Duration conversion throws - turning a routine retry into an infrastructure error on
     * exactly the task that has failed most.
     */
    @Test
    void extremeAttemptCountDoesNotOverflow() {
        var options = ActivityOptions.DEFAULT.withMaxInterval(Duration.ofHours(1));

        assertThatNoException().isThrownBy(() ->
                RetryPolicy.nextBackoff(options, Integer.MAX_VALUE, 42L));
        assertThat(RetryPolicy.nextBackoff(options, 999, 42L))
                .isLessThanOrEqualTo(Duration.ofHours(1))
                .isPositive();
    }

    /** Without jitter, a thousand tasks failing together would retry together. */
    @Test
    void jitterDesynchronisesTheHerd() {
        var options = ActivityOptions.DEFAULT.withInitialInterval(Duration.ofSeconds(10));

        Set<Long> distinctDelays = LongStream.range(0, 1_000)
                .map(id -> RetryPolicy.nextBackoff(options, 3, id).toMillis())
                .boxed().collect(Collectors.toSet());

        assertThat(distinctDelays)
                .as("without jitter every task would share one delay value")
                .hasSizeGreaterThan(900);
    }

    /** Same inputs must produce the same delay, for incident reconstruction. */
    @Test
    void backoffIsReproducible() {
        var options = ActivityOptions.DEFAULT;
        assertThat(RetryPolicy.nextBackoff(options, 3, 12345L))
                .isEqualTo(RetryPolicy.nextBackoff(options, 3, 12345L));
    }

    /** Frameworks wrap exceptions; nonRetryableErrors must still match. */
    @Test
    void nonRetryableIsDetectedThroughTheCauseChain() {
        var options = ActivityOptions.DEFAULT
                .withNonRetryableErrors("IllegalArgumentException");

        Throwable wrapped = new RuntimeException("outer",
                new IllegalStateException("middle",
                        new IllegalArgumentException("bad input")));

        assertThat(RetryPolicy.isNonRetryable(options, wrapped)).isTrue();
        assertThat(RetryPolicy.shouldRetry(options, 1, wrapped)).isFalse();
    }

    @Test
    void selfReferentialCauseDoesNotLoopForever() {
        var self = new RuntimeException("loop");
        self.initCause(self);   // pathological but constructible

        assertThatNoException().isThrownBy(() ->
                RetryPolicy.isNonRetryable(
                        ActivityOptions.DEFAULT.withNonRetryableErrors("Nope"), self));
    }

    @Test
    void retriesStopAtMaxAttempts() {
        var options = ActivityOptions.DEFAULT.withMaxAttempts(3);
        var failure = new RuntimeException("transient");

        assertThat(RetryPolicy.shouldRetry(options, 1, failure)).isTrue();
        assertThat(RetryPolicy.shouldRetry(options, 2, failure)).isTrue();
        assertThat(RetryPolicy.shouldRetry(options, 3, failure)).isFalse();
    }

    @Test
    void singleAttemptDisablesRetry() {
        assertThat(RetryPolicy.shouldRetry(
                ActivityOptions.DEFAULT.withoutRetry(), 1, new RuntimeException()))
                .isFalse();
    }

    @Test
    void coefficientBelowOneIsRejected() {
        assertThatThrownBy(() -> ActivityOptions.DEFAULT.withBackoffCoefficient(0.5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("accelerates retries");
    }

    @Test
    void maxIntervalBelowInitialIsRejected() {
        assertThatThrownBy(() -> ActivityOptions.DEFAULT
                .withInitialInterval(Duration.ofMinutes(5))
                .withMaxInterval(Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
