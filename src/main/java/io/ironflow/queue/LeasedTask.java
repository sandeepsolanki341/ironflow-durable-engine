package io.ironflow.queue;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * A task claimed from the queue, together with the ownership token required to ack it.
 *
 * @param taskId            queue row id (internal BIGINT key)
 * @param taskUuid          external task handle, added in V2. Used by the orchestrator
 *                          API and by activity code, which may retain it across restarts.
 * @param executionId       owning workflow execution
 * @param scheduledEventSeq history sequence number that scheduled this task
 * @param attempt           1-based attempt counter. {@code > 1} means this task has been
 *                          redelivered, which is the signal to activity code that
 *                          idempotency matters on this invocation. Worth surfacing to
 *                          user code rather than hiding: an activity that can cheaply
 *                          check "did I already do this?" should do so on retries and
 *                          skip the check on first attempt.
 * @param payload           opaque serialized input
 * @param leaseOwner        per-lease ownership token; required by every ack path
 * @param leaseUntil        deadline after which the reaper may reclaim this task
 */
public record LeasedTask(
        long taskId,
        UUID taskUuid,
        UUID executionId,
        long scheduledEventSeq,
        int attempt,
        byte[] payload,
        UUID leaseOwner,
        Instant leaseUntil) {

    /** @return remaining lease time; negative if already expired. */
    public Duration remainingLease() {
        return Duration.between(Instant.now(), leaseUntil);
    }

    /** @return {@code true} if this is a redelivery rather than a first attempt. */
    public boolean isRetry() {
        return attempt > 1;
    }

    /**
     * Records are given array components here for payload efficiency; the generated
     * {@code equals}/{@code hashCode} would compare byte arrays by identity, which is
     * misleading. Identity is the task id, so we override to say so explicitly.
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof LeasedTask other && taskId == other.taskId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(taskId);
    }

    @Override
    public String toString() {
        return "LeasedTask[id=%d, execution=%s, attempt=%d, leaseUntil=%s]"
                .formatted(taskId, executionId, attempt, leaseUntil);
    }
}
