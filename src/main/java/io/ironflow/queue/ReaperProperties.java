package io.ironflow.queue;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Tuning knobs for {@link LeaseReaper}.
 *
 * <p>The relationship that matters: {@code interval} must be materially shorter than the
 * shortest lease in use, or expired tasks sit idle for up to a full interval before
 * anyone notices. With a 30s lease and a 5s sweep, worst-case recovery latency after a
 * hard crash is 35s.</p>
 */
@ConfigurationProperties(prefix = "ironflow.reaper")
public class ReaperProperties {

    /** Sweep interval in milliseconds. Bound to the {@code @Scheduled} annotation. */
    private long intervalMs = 5_000;

    /** Delay before the first sweep. Raised in tests so sweeps can be driven manually. */
    private long initialDelayMs = 5_000;

    /** Tasks reclaimed per transaction. Bounds lock hold time under mass failure. */
    private int batchSize = 500;

    /** Max batches per sweep, so a deep backlog cannot monopolise the scheduler. */
    private int maxRoundsPerSweep = 20;

    /** First-retry delay; doubles per attempt. */
    private Duration baseBackoff = Duration.ofSeconds(1);

    /** Ceiling on exponential growth. */
    private Duration maxBackoff = Duration.ofMinutes(5);

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = batchSize;
    }

    public int getMaxRoundsPerSweep() {
        return maxRoundsPerSweep;
    }

    public void setMaxRoundsPerSweep(int maxRoundsPerSweep) {
        if (maxRoundsPerSweep < 1) {
            throw new IllegalArgumentException("maxRoundsPerSweep must be >= 1");
        }
        this.maxRoundsPerSweep = maxRoundsPerSweep;
    }

    public Duration getBaseBackoff() {
        return baseBackoff;
    }

    public void setBaseBackoff(Duration baseBackoff) {
        this.baseBackoff = baseBackoff;
    }

    public Duration getMaxBackoff() {
        return maxBackoff;
    }

    public void setMaxBackoff(Duration maxBackoff) {
        this.maxBackoff = maxBackoff;
    }
}
