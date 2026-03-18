package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * One event from an execution's history, as read for replay.
 *
 * @param sequenceNumber position in history; gap-free and strictly increasing
 * @param eventType      discriminator, see {@link EventTypes}
 * @param payload        parsed attributes
 * @param createdAt      when the event was appended. Deliberately NOT what
 *                       {@code ctx.now()} returns - wall-clock append time differs
 *                       between the original run and a replay, so time must come from a
 *                       recorded marker instead.
 */
public record HistoryEvent(
        long sequenceNumber,
        String eventType,
        JsonNode payload,
        Instant createdAt) { }
