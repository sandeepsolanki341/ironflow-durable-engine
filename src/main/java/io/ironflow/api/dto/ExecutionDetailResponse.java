package io.ironflow.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import io.ironflow.persistence.model.ExecutionStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Full execution state including history.
 *
 * @param currentVersion   optimistic-lock version; exposed so a future conditional-update
 *                         API can use it as an ETag
 * @param divergenceDetail populated only when {@code status} is DIVERGENT. Names both sides
 *                         of the replay mismatch, so an operator can identify the offending
 *                         code change without correlating logs against a deploy timeline.
 * @param history          events in replay order, or empty when the caller opted out
 */
public record ExecutionDetailResponse(
        UUID executionId,
        String workflowType,
        String businessKey,
        ExecutionStatus status,
        long currentVersion,
        Instant startTime,
        Instant endTime,
        JsonNode result,
        String failure,
        String divergenceDetail,
        List<EventView> history) {

    /** @param sequenceNumber position in history; gap-free and strictly increasing */
    public record EventView(
            long sequenceNumber,
            String eventType,
            JsonNode payload,
            Instant createdAt) { }
}
