package io.ironflow.examples;

import io.ironflow.sdk.ActivityOptions;
import io.ironflow.sdk.Workflow;
import io.ironflow.sdk.WorkflowContext;

import java.time.Duration;

/**
 * The flagship demonstration workflow: an e-commerce order fulfillment saga that exercises
 * every capability IronFlow provides in a single, realistic business process.
 *
 * <h2>Why this workflow is the whole pitch</h2>
 *
 * <p>In roughly forty lines of ordinary-looking sequential Java, this workflow does four
 * things that are individually hard and collectively almost never combined correctly in
 * hand-rolled systems:</p>
 *
 * <ol>
 *   <li><b>Charges a card with a bounded retry policy</b> - transient payment-gateway blips
 *       are absorbed automatically, but a permanently declined card fails fast instead of
 *       retrying forever.</li>
 *   <li><b>Reserves inventory</b>, registering the release of that reservation as a
 *       compensation so a later failure cannot leave stock stranded.</li>
 *   <li><b>Sleeps durably for three days</b> - a real "wait before asking for a review"
 *       delay. No thread is held, no row is polled in a tight loop; the workflow is off the
 *       CPU entirely and resumes on some worker three days later at this exact line.</li>
 *   <li><b>Fans out two notifications in parallel</b> and waits for both, the way a real
 *       system would send email and SMS concurrently rather than one after the other.</li>
 * </ol>
 *
 * <h2>The saga guarantee this demonstrates</h2>
 *
 * <p>The steps are ordered so that money moves first, then inventory, then time passes, then
 * notifications go out. If <em>any</em> later step fails permanently - the canonical demo
 * injects a dead SMS provider in step 4 - the engine automatically unwinds the completed
 * steps <b>in reverse order</b>: it releases the inventory it reserved, then refunds the card
 * it charged. The customer is never left charged-but-unfulfilled, and the warehouse is never
 * left holding stock for an order that will not ship.</p>
 *
 * <p>Crucially, the workflow author does not write any of that rollback logic. Each forward
 * step simply declares its own undo via {@link WorkflowContext#compensateWith}. The reverse
 * ordering, the durability of the rollback across crashes, and the transition to
 * {@code FAILED_COMPENSATED} are the engine's job. That separation - forward business logic
 * reads top-to-bottom, recovery is declarative - is the entire value proposition.</p>
 *
 * <h2>Determinism</h2>
 *
 * <p>Every non-deterministic input (time, parallelism, retries) is mediated by {@code ctx}.
 * There is no {@code Instant.now()}, no thread, no direct I/O. That is what lets the engine
 * replay this method from event history after a crash and arrive at exactly the same place
 * it was before - including, if a failure already occurred, resuming a half-finished
 * rollback rather than restarting it.</p>
 */
public final class OrderFulfillmentWorkflow
        implements Workflow<OrderInput, OrderResult> {

    /**
     * Payment must not retry forever. Five attempts with a 30-second per-attempt timeout
     * rides out transient gateway failures (network blips, brief 503s) while failing fast on
     * a genuine decline. These options are captured into history when the activity is first
     * scheduled and are NOT re-read on retry, so a config change mid-flight cannot
     * retroactively change the retry budget of an in-progress charge.
     */
    private static final ActivityOptions CHARGE_OPTIONS = ActivityOptions.DEFAULT
            .withMaxAttempts(5)
            .withTimeout(Duration.ofSeconds(30));

    /** How long to wait after purchase before soliciting a product review. */
    private static final Duration REVIEW_DELAY = Duration.ofDays(3);

    @Override
    public String type() {
        return "OrderFulfillment";
    }

    @Override
    public Class<OrderInput> inputType() {
        return OrderInput.class;
    }

    @Override
    public OrderResult run(OrderInput order, WorkflowContext ctx) throws Exception {
        ctx.logger().info("Starting fulfillment for order {}", order.orderId());

        // ---- Step 1: charge the card, with a bounded retry policy -------------------------
        // Money moves first. We register the refund compensation IMMEDIATELY after the charge
        // succeeds, so that from this point on any downstream failure will give the customer
        // their money back. Registering before the next risky step - not at the end - is the
        // saga discipline: the undo must exist the instant the side effect does.
        Charge charge = ctx.activity("chargeCard", order.payment(), Charge.class,
                CHARGE_OPTIONS);
        ctx.compensateWith("refund", charge.transactionId());
        ctx.logger().info("Charged card, txn {}", charge.transactionId());

        // ---- Step 2: reserve inventory ----------------------------------------------------
        // Now stock. Same discipline: the release compensation is registered the moment the
        // reservation exists. Because compensations run LAST-IN-FIRST-OUT, this release will
        // execute BEFORE the refund if we have to roll back - inventory is freed, then the
        // money is returned, mirroring the order in which they were committed.
        Reservation reservation = ctx.activity("reserveInventory", order.sku(),
                Reservation.class);
        ctx.compensateWith("releaseInventory", reservation.reservationId());
        ctx.logger().info("Reserved inventory, reservation {}", reservation.reservationId());

        // ---- Step 3: durable three-day wait -----------------------------------------------
        // The workflow leaves the CPU entirely here. A durable timer fires in three days and
        // a worker resumes this method at exactly this line. Kill every machine in the fleet
        // during these three days and nothing is lost; the timer lives in Postgres.
        ctx.sleep(REVIEW_DELAY);

        // ---- Step 4: two review notifications, in parallel --------------------------------
        // Fan out email and SMS together. Both are scheduled in one decision and run
        // concurrently on whatever workers pick them up; awaitAll releases only when BOTH
        // have completed. In the failure demo the SMS provider is permanently down, so
        // sendReviewSms exhausts its retries, awaitAll rethrows that failure, and - because
        // refund and releaseInventory are on the compensation stack - the engine rolls the
        // whole order back instead of leaving a charged, reserved, un-notified order.
        var email = ctx.async("sendReviewEmail", order.customerEmail());
        var sms = ctx.async("sendReviewSms", order.customerPhone());
        ctx.awaitAll(email, sms);
        ctx.logger().info("Review notifications sent for order {}", order.orderId());

        return new OrderResult(order.orderId(), charge.transactionId(),
                reservation.reservationId(), "FULFILLED");
    }

    // ---------------------------------------------------------------------------------
    // Activity result shapes. Small records kept alongside the workflow so the demo is
    // self-contained and readable in one file for a presentation.
    // ---------------------------------------------------------------------------------

    /** Result of the {@code chargeCard} activity. */
    public record Charge(String transactionId, long amountCents) {}

    /** Result of the {@code reserveInventory} activity. */
    public record Reservation(String reservationId, String sku, int quantity) {}
}
