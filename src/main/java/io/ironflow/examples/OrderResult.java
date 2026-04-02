package io.ironflow.examples;

/**
 * Result of a successfully fulfilled order returned by {@link OrderFulfillmentWorkflow}.
 *
 * <p>Only produced when the workflow completes its happy path. A compensated failure never
 * returns an {@code OrderResult} - it closes the execution as {@code FAILED_COMPENSATED},
 * carrying the original failure rather than a result - so the presence of this object is
 * itself the signal that the order went through end to end.</p>
 *
 * @param orderId       echoes the input order id
 * @param transactionId the payment transaction that was charged
 * @param reservationId the inventory reservation that was held
 * @param status        terminal business status, {@code "FULFILLED"} on the happy path
 */
public record OrderResult(
        String orderId,
        String transactionId,
        String reservationId,
        String status) {
}
