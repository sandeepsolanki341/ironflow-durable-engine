package io.ironflow.api;

import java.util.UUID;

/**
 * Thrown when a business key is reused with materially different input.
 *
 * <p>Mapped to {@code 409 Conflict}, deliberately not 200. Returning the existing execution
 * here would tell the caller their request succeeded while silently discarding the input
 * they actually sent.</p>
 *
 * <p>Almost always means the key is less unique than the caller assumed - an order id
 * without a tenant prefix, a date-based key colliding across regions.</p>
 */
public class IdempotencyConflictException extends RuntimeException {

    private final UUID existingExecutionId;

    public IdempotencyConflictException(String businessKey, UUID existingExecutionId,
                                        String existingType, String requestedType) {
        super(("Business key '%s' is already in use by execution %s (type '%s', requested "
                + "'%s') with different input. This is a key collision, not a retry - "
                + "scope the key more narrowly, e.g. by tenant.")
                .formatted(businessKey, existingExecutionId, existingType, requestedType));
        this.existingExecutionId = existingExecutionId;
    }

    public UUID getExistingExecutionId() {
        return existingExecutionId;
    }
}
