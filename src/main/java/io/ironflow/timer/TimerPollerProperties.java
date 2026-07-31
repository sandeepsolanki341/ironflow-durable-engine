package io.ironflow.timer;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Timer poller configuration.
 *
 * <p>{@code replicaIndex} and {@code replicaCount} must be injected per pod. In Kubernetes a
 * StatefulSet gives this for free: the ordinal in the pod name is the index, and the replica
 * count is the spec's {@code replicas}. A Deployment does not, which is a good reason to run
 * timer pollers as a StatefulSet.</p>
 *
 * <p><b>Configure replicaCount conservatively.</b> Over-coverage (two replicas owning one
 * shard) is free - SKIP LOCKED makes it merely wasteful. Under-coverage (a shard owned by
 * nobody) is a silent outage: those timers never fire, with no error anywhere.</p>
 */
@ConfigurationProperties(prefix = "ironflow.timer")
public class TimerPollerProperties {

    private boolean enabled = true;

    /** This pod's ordinal, in {@code [0, replicaCount)}. */
    private int replicaIndex = 0;

    /** Total pods polling timers. Setting this too high leaves shards uncovered. */
    private int replicaCount = 1;

    /** Timers fired per transaction. Bounds lock hold time during a catch-up surge. */
    private int batchSize = 200;

    /** Floor on poll interval; also the floor on firing latency. */
    private Duration minPollInterval = Duration.ofMillis(200);

    /** Ceiling on poll interval when idle; bounds idle database load. */
    private Duration maxPollInterval = Duration.ofSeconds(1);

    private Duration errorBackoff = Duration.ofSeconds(5);

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean v) { this.enabled = v; }

    public int getReplicaIndex() { return replicaIndex; }
    public void setReplicaIndex(int v) { this.replicaIndex = v; }

    public int getReplicaCount() { return replicaCount; }
    public void setReplicaCount(int v) {
        if (v < 1) {
            throw new IllegalArgumentException("replicaCount must be >= 1");
        }
        this.replicaCount = v;
    }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int v) {
        if (v < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        this.batchSize = v;
    }

    public Duration getMinPollInterval() { return minPollInterval; }
    public void setMinPollInterval(Duration v) { this.minPollInterval = v; }

    public Duration getMaxPollInterval() { return maxPollInterval; }
    public void setMaxPollInterval(Duration v) { this.maxPollInterval = v; }

    public Duration getErrorBackoff() { return errorBackoff; }
    public void setErrorBackoff(Duration v) { this.errorBackoff = v; }
}
