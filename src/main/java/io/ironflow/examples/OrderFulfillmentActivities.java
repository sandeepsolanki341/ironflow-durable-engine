package io.ironflow.examples;

import io.ironflow.worker.Activity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Simulated activity implementations for the {@link OrderFulfillmentWorkflow}.
 *
 * <p>In a real production deployment, these methods would contain HTTP clients,
 * SDK calls, and complex business logic to interact with external systems like
 * Stripe, Twilio, and an inventory management service.</p>
 *
 * <p>For local testing and demonstration of the IronFlow engine, these activities
 * are "stubs" that simulate success and failure. Specifically, the
 * {@code sendReviewSms} activity is hardcoded to fail, which triggers the
 * workflow's saga compensation logic to prove that the engine can automatically
 * roll back completed side effects.</p>
 */
@Component
public class OrderFulfillmentActivities {

    private static final Logger log = LoggerFactory.getLogger(OrderFulfillmentActivities.class);

    /**
     * Simulates charging a credit card via a payment gateway.
     *
     * @param paymentToken the opaque payment token from the client
     * @return a simulated charge receipt
     */
    @Activity("chargeCard")
    public OrderFulfillmentWorkflow.Charge chargeCard(String paymentToken) {
        log.info("✅ SUCCESS: Simulating charge for payment token: {}", paymentToken);
        return new OrderFulfillmentWorkflow.Charge("txn-" + System.currentTimeMillis(), 1000);
    }

    /**
     * Simulates reserving inventory in a warehouse system.
     *
     * @param sku the stock keeping unit to reserve
     * @return a simulated reservation receipt
     */
    @Activity("reserveInventory")
    public OrderFulfillmentWorkflow.Reservation reserveInventory(String sku) {
        log.info("✅ SUCCESS: Simulating inventory reservation for SKU: {}", sku);
        return new OrderFulfillmentWorkflow.Reservation("resv-" + System.currentTimeMillis(), sku, 1);
    }

    /**
     * Simulates sending a review email.
     *
     * @param email the destination email address
     */
    @Activity("sendReviewEmail")
    public void sendReviewEmail(String email) {
        log.info("✅ SUCCESS: Simulating review email sent to: {}", email);
    }

    /**
     * Simulates sending a review SMS. This method is intentionally designed to fail
     * to demonstrate the engine's saga compensation capabilities.
     *
     * @param phone the destination phone number
     */
    @Activity("sendReviewSms")
    public void sendReviewSms(String phone) {
        log.error("❌ FAIL: Simulating SMS provider outage. Cannot send to: {}", phone);
        throw new RuntimeException("Simulated failure: SMS provider unreachable");
    }

    /**
     * Compensation activity for {@link #chargeCard}. Simulates refunding a transaction.
     *
     * @param transactionId the transaction to refund
     */
    @Activity("refund")
    public void refund(String transactionId) {
        log.info("↩️ COMPENSATING: Simulating refund for transaction: {}", transactionId);
    }

    /**
     * Compensation activity for {@link #reserveInventory}. Simulates releasing reserved stock.
     *
     * @param reservationId the reservation to release
     */
    @Activity("releaseInventory")
    public void releaseInventory(String reservationId) {
        log.info("↩️ COMPENSATING: Simulating inventory release for reservation: {}", reservationId);
    }
}