package io.ironflow.api.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Request to start a workflow execution.
 *
 * @param workflowType registered workflow type name; must resolve to a known workflow
 * @param input        arbitrary JSON input, opaque to the engine
 * @param businessKey  optional caller-supplied idempotency key. If an execution already
 *                     exists with this key, the start is a no-op returning the existing
 *                     execution rather than creating a duplicate. This is the only
 *                     protection against a client that retries a start after a network
 *                     timeout, which is common enough that omitting the key should be
 *                     considered a caller bug for anything with side effects.
 * @param taskQueue    optional routing hint; defaults to {@code "default"}
 */
public record StartWorkflowRequest(
        @NotBlank @Size(max = 255) String workflowType,
        JsonNode input,
        @Size(max = 255) String businessKey,
        @Size(max = 255) String taskQueue) {

    public static final String DEFAULT_QUEUE = "default";

    public String taskQueueOrDefault() {
        return taskQueue == null || taskQueue.isBlank() ? DEFAULT_QUEUE : taskQueue;
    }
}
