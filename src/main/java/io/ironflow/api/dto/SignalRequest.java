package io.ironflow.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Request to deliver an external signal.
 *
 * @param signalName the name the workflow awaits via {@code ctx.waitForSignal(name)}
 * @param payload    arbitrary JSON, deserialized by the workflow into its expected type
 * @param signalId   optional idempotency key. Also accepted as the {@code Idempotency-Key}
 *                   header. Strongly recommended - HTTP clients retry, and without this a
 *                   timed-out request that actually succeeded delivers the signal twice.
 */
public record SignalRequest(
        @NotBlank @Size(max = 255) String signalName,
        JsonNode payload,
        @Size(max = 255) String signalId) { }
