package io.ironflow.examples;

/**
 * Input to {@link OrderFulfillmentWorkflow}.
 *
 * <p>Deserialized from the workflow's start payload, so it is a plain record with no
 * behaviour. Every field the workflow needs to make its (deterministic) decisions is carried
 * here; the workflow never reaches outside this input and {@code ctx} for data.</p>
 *
 * @param orderId       stable business identifier, used as the idempotency anchor upstream
 * @param sku           the item being purchased
 * @param payment       opaque payment token handed to the {@code chargeCard} activity
 * @param customerEmail destination for the review email
 * @param customerPhone destination for the review SMS
 */
public record OrderInput(
        String orderId,
        String sku,
        String payment,
        String customerEmail,
        String customerPhone) {
}
