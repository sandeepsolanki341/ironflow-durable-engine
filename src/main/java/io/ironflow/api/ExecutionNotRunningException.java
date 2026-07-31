package io.ironflow.api;

import java.util.UUID;

/**
 * Thrown when a signal targets an execution that is not RUNNING.
 *
 * <p>Mapped to {@code 409 Conflict}. Signalling a completed workflow is a caller error worth
 * surfacing rather than absorbing - a caller that believes it approved an order should learn
 * that the order already shipped.</p>
 */
public class ExecutionNotRunningException extends RuntimeException {

    public ExecutionNotRunningException(UUID executionId, String status) {
        super("Execution %s is %s, not RUNNING; signals cannot be delivered"
                .formatted(executionId, status));
    }
}
