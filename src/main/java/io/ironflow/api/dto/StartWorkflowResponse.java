package io.ironflow.api.dto;

import io.ironflow.persistence.model.ExecutionStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * @param alreadyExisted {@code true} if this call matched an existing business key and
 *                       started nothing. Surfaced explicitly so callers can distinguish
 *                       "I started it" from "it was already running" - the HTTP status
 *                       alone (200 vs 201) carries the same information but is easy to
 *                       miss in client code that only reads the body.
 */
public record StartWorkflowResponse(
        UUID executionId,
        String workflowType,
        ExecutionStatus status,
        Instant startTime,
        boolean alreadyExisted) { }
