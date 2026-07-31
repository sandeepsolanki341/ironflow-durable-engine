package io.ironflow.api;

import java.util.UUID;

/** Thrown when an execution id does not resolve. Mapped to HTTP 404. */
public class ExecutionNotFoundException extends RuntimeException {

    private final UUID executionId;

    public ExecutionNotFoundException(UUID executionId) {
        super("No execution with id " + executionId);
        this.executionId = executionId;
    }

    public UUID getExecutionId() {
        return executionId;
    }
}
