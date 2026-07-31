package io.ironflow.api.telemetry;

import java.time.Instant;

/**
 * A single point-in-time reading of the engine's live health, for the System Telemetry charts.
 *
 * <p>Each field is one line on the dashboard. Nullable fields degrade gracefully: if a metric
 * is unavailable (e.g. no dispatch-latency timer has recorded a sample yet), its line simply
 * has a gap rather than the endpoint failing. The frontend polls this endpoint and appends each
 * sample to a rolling window, which is why the shape is deliberately flat and cheap to produce.</p>
 *
 * @param timestamp          when this reading was taken
 * @param queueDepth         PENDING tasks waiting to be dispatched (the backlog)
 * @param leasedTasks        tasks currently leased by a worker (in-flight work)
 * @param dispatchLatencyMs  mean task dispatch latency over the recent window, or null
 * @param activeVirtualThreads best-effort count of live virtual threads, or null if unsupported
 */
public record TelemetrySample(
        Instant timestamp,
        long queueDepth,
        long leasedTasks,
        Double dispatchLatencyMs,
        Long activeVirtualThreads) {
}
