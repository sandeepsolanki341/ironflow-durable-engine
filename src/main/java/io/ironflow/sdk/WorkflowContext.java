package io.ironflow.sdk;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

/**
 * The workflow's only permitted window onto the outside world.
 *
 * <p>Every method here is <em>replay-aware</em>: on first execution it records its outcome
 * to history, and on every subsequent replay it returns the recorded value rather than
 * recomputing. That is what makes a workflow deterministic despite doing nondeterministic
 * things.</p>
 *
 * <p>Not thread-safe, deliberately. A workflow body runs on exactly one virtual thread;
 * spawning threads inside a workflow breaks determinism because the interleaving of SDK
 * calls would differ between runs. If the SDK detects a call from the wrong thread it
 * fails fast rather than corrupting the command sequence.</p>
 */
public interface WorkflowContext {

    /**
     * Schedules an activity and waits for its result.
     *
     * <p><b>On replay</b>, if history already contains the outcome for this call, returns
     * it immediately without re-executing the side effect. This is the core mechanism of
     * durable execution - the reason a workflow resumed after a crash does not re-charge a
     * credit card.</p>
     *
     * <p><b>On first execution</b>, emits a schedule command and blocks the workflow
     * thread. The decision task ends; the activity runs on some worker; a later decision
     * task replays this workflow from the top, and this call then finds its recorded
     * result and returns immediately.</p>
     *
     * @param activityType registered activity type name, used for worker routing
     * @param input        activity input, serialized to JSON
     * @param resultType   expected result type
     * @throws ActivityFailure if the activity failed after exhausting its retries. Catch
     *         it to run compensation; let it propagate to fail the workflow.
     */
    <T> T activity(String activityType, Object input, Class<T> resultType);

    /** As {@link #activity}, with explicit retry and timeout options. */
    <T> T activity(String activityType, Object input, Class<T> resultType,
                   ActivityOptions options);

    /**
     * Deterministic wall-clock time.
     *
     * <p>Records a {@code MARKER_RECORDED} event on first execution and returns the
     * recorded value on every replay, so a workflow that logs a timestamp or computes a
     * deadline sees the same instant no matter how many times it is replayed.</p>
     *
     * <p>Never call {@link Instant#now()} in workflow code. It will appear to work, then
     * produce a workflow that computes a different deadline on every replay.</p>
     */
    Instant now();

    /**
     * Deterministic randomness.
     *
     * <p>Backed by a generator seeded from the execution id, so the same sequence is
     * reproduced on every replay. Unlike {@link #now()} this needs no marker - the seed and
     * the call order fully determine the output, which makes it far cheaper.</p>
     *
     * <p>The returned instance is stable across calls within one workflow; do not cache it
     * across decision tasks.</p>
     */
    Random random();

    /** Deterministic UUID, derived from the seeded generator. */
    UUID randomUUID();

    /**
     * Durable sleep.
     *
     * <p>Costs one row and zero threads. A million workflows sleeping for a month are a
     * million rows that no query touches until their deadline arrives.</p>
     */
    void sleep(Duration duration);

    /**
     * Blocks until a signal with this name has been received.
     *
     * <p>Signals are matched by name from an inbox built during history scanning, NOT
     * against the command cursor. See {@code SignalInbox} for why: a signal has no
     * preceding command and can arrive before the workflow asks for it.</p>
     *
     * <p>Has no timeout of its own. For "wait, or escalate", compose with
     * {@link #sleep} and {@link #pollSignal}.</p>
     */
    <T> T waitForSignal(String signalName, Class<T> payloadType);

    /**
     * Non-blocking poll of the signal inbox.
     *
     * <p>The building block for the common "wait for approval, or time out" pattern:</p>
     *
     * <pre>{@code
     * ctx.sleep(Duration.ofDays(3));
     * var approval = ctx.pollSignal("approval", Approval.class);
     * if (approval.isEmpty()) {
     *     ctx.activity("escalate", orderId, Void.class);
     * }
     * }</pre>
     */
    <T> Optional<T> pollSignal(String signalName, Class<T> payloadType);

    /** @return {@code true} if an unconsumed signal with this name is buffered. */
    boolean hasSignal(String signalName);

    // ---------------------------------------------------------------------------------
    // Parallel branches (Promise.all).
    // ---------------------------------------------------------------------------------

    /**
     * Schedules an activity without waiting, returning a handle to its eventual result.
     *
     * <h2>The fan-out pattern</h2>
     *
     * <p>Call {@code async} several times, then {@code awaitAll} once. Each {@code async}
     * returns immediately, so the workflow reaches {@code awaitAll} holding every handle, and
     * all the activities are scheduled together in one transaction - and therefore run
     * concurrently on whatever workers pick them up.</p>
     *
     * <pre>{@code
     * var inventory = ctx.async("checkInventory", sku);
     * var pricing   = ctx.async("fetchPricing", sku);
     * var shipping  = ctx.async("estimateShipping", address);
     * ctx.awaitAll(inventory, pricing, shipping);   // all three ran in parallel
     * var total = ctx.get(pricing).amount() + ctx.get(shipping).amount();
     * }</pre>
     *
     * <h2>What this does NOT do</h2>
     *
     * <p>It does not start a thread and does not begin the activity. It records the intent to
     * schedule and returns. The activity is not enqueued until the decision task commits -
     * which is correct, because scheduling is a durable act that must be part of the atomic
     * decision, not a side effect that could happen and then be lost to a crash. So
     * {@code async} accomplishes nothing durable until an {@code awaitAll} (or a terminal
     * return) parks the workflow and lets the decision commit.</p>
     *
     * @param activityType registered activity type
     * @param args         activity input. A single argument is passed through unchanged, so
     *                     existing single-input activities need no change; multiple arguments
     *                     become a positional array.
     * @return an inert handle; await it with {@link #awaitAll}, then read via {@link #get}
     */
    <T> WorkflowFuture<T> async(String activityType, Object... args);

    /** Typed variant, when the result class cannot be inferred from context. */
    <T> WorkflowFuture<T> async(String activityType, Class<T> resultType, Object... args);

    /**
     * Suspends until every referenced future has a completion event in history.
     *
     * <h2>The synchronization semantics</h2>
     *
     * <p>On first execution the futures' schedule commands have been accumulated but not yet
     * committed; {@code awaitAll} parks, the decision commits all of them, and the activities
     * fan out. On each subsequent replay it checks history for <em>every</em> future's
     * outcome; if any is missing it parks again - the workflow cannot pass the barrier until
     * the slowest branch lands.</p>
     *
     * <p>This is {@code Promise.all}, not {@code Promise.race}: the barrier releases only when
     * the last branch completes. It mirrors {@code Promise.all}'s rejection behaviour too - if
     * any branch failed, {@code awaitAll} throws that failure as soon as it is known, without
     * waiting for slower branches, because a caller waiting on all results cannot sensibly
     * proceed with a hole where one should be.</p>
     *
     * @throws ActivityFailure if any awaited branch exhausted its retries or failed
     *         non-retryably; the first failure in command order is thrown
     */
    void awaitAll(WorkflowFuture<?>... futures);

    /**
     * Resolves a future to its recorded result.
     *
     * <p>Valid only after an {@link #awaitAll} covering this future has returned. Calling it
     * earlier is a workflow bug - the result is not in history yet - and throws rather than
     * returning a plausible-looking null, which would corrupt downstream business logic far
     * from its cause.</p>
     */
    <T> T get(WorkflowFuture<T> future);

    /**
     * Wraps a nondeterministic local computation so its result is recorded once and
     * replayed thereafter.
     *
     * <p>The escape hatch for things not worth a full activity - reading a config value,
     * generating a correlation id, calling a pure-but-versioned library. Unlike an activity
     * this runs in-process with no retry and no separate task, so use it only for cheap,
     * non-failing work.</p>
     */
    <T> T sideEffect(String name, java.util.function.Supplier<T> supplier,
                     Class<T> resultType);

    /**
     * Registers a compensation action for the step that just succeeded.
     *
     * <h2>The saga contract</h2>
     *
     * <p>Call this immediately after a successful activity whose effect must be undone if a
     * LATER step fails. Registrations form a LIFO stack; if the workflow later throws an
     * unhandled {@link ActivityFailure}, the engine runs the registered compensations in
     * reverse order - the most recent success is undone first - then marks the execution
     * {@code FAILED_COMPENSATED}.</p>
     *
     * <pre>{@code
     * var reservation = ctx.activity("reserveInventory", sku, Reservation.class);
     * ctx.compensateWith("releaseInventory", reservation.id());   // undo #1
     *
     * var charge = ctx.activity("chargeCard", payment, Charge.class);
     * ctx.compensateWith("refundCard", charge.id());              // undo #2
     *
     * // If this throws after exhausting retries, the engine runs refundCard THEN
     * // releaseInventory - reverse order - with no further code from you.
     * ctx.activity("shipOrder", order, Void.class);
     * }</pre>
     *
     * <h2>What this does and does not do</h2>
     *
     * <p>It records the <em>intent</em> to compensate - nothing runs now. The compensation
     * activity executes only if a later step fails; if the workflow completes successfully,
     * no compensation ever runs. Like every other durable act, the registration is written to
     * history, so the compensation stack is reconstructed on replay and survives a crash. A
     * stack held only in memory would be empty after the very crash compensation exists to
     * handle.</p>
     *
     * <p>Compensations are themselves activities: they retry, they can fail, and they run
     * through the same durable task machinery. A compensation that itself exhausts its
     * retries stops the rollback and leaves the execution {@code FAILED_COMPENSATED} with the
     * unfinished compensation recorded - because a saga engine cannot invent a way to undo an
     * undo, and silently continuing would hide a genuinely stuck rollback.</p>
     *
     * @param activityType the compensating activity's registered type
     * @param args         its input; same single-arg-passthrough rule as {@code activity}
     */
    void compensateWith(String activityType, Object... args);

    /** @return the execution id. Stable across replays. */
    UUID executionId();

    /**
     * @return {@code true} while the workflow is re-executing over known history. Useful
     *         only for suppressing duplicate log output - never branch business logic on
     *         this, since doing so guarantees replay diverges from the original run.
     */
    boolean isReplaying();

    /** @return a logger that suppresses output during replay. */
    org.slf4j.Logger logger();
}
