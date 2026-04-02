package io.ironflow.api.dto;

import io.ironflow.persistence.model.ExecutionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * A single row in the executions list.
 *
 * <p>Deliberately lighter than {@link ExecutionDetailResponse}: no history, no result or
 * input payload, no divergence detail. A list view renders hundreds of rows, and shipping
 * each one's full event history would turn a cheap index scan into a payload the size of the
 * entire database. The dashboard fetches the heavy detail only when a row is opened.</p>
 *
 * <p>{@code endTime} is null for the non-terminal states (RUNNING, COMPENSATING, DIVERGENT);
 * the frontend derives "duration so far" from {@code startTime} to now in that case, and a
 * fixed duration from {@code startTime} to {@code endTime} once terminal.</p>
 *
 * @param executionId  stable id, used as the row key and the link target for detail
 * @param workflowType the registered workflow type
 * @param businessKey  caller-supplied idempotency/search key, may be null
 * @param status       current lifecycle state
 * @param startTime    when the execution began; never null
 * @param endTime      when it reached a terminal state, or null if still live
 */
public record ExecutionSummaryResponse(
        UUID executionId,
        String workflowType,
        String businessKey,
        ExecutionStatus status,
        Instant startTime,
        Instant endTime) {
}
