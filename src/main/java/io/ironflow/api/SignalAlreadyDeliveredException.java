package io.ironflow.api;

import java.util.UUID;

/**
 * Thrown when a signal id has already been delivered to this execution.
 *
 * <p>Not an error from the caller's perspective - their signal IS delivered, which is what
 * they wanted. The controller maps this to {@code 202 Accepted} with
 * {@code deduplicated=true} rather than a failure status.</p>
 */
public class SignalAlreadyDeliveredException extends RuntimeException {

    public SignalAlreadyDeliveredException(UUID executionId, String signalId) {
        super("Signal '%s' was already delivered to execution %s"
                .formatted(signalId, executionId));
    }
}
