package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.sdk.ActivityFailure;
import io.ironflow.sdk.ActivityOptions;
import io.ironflow.sdk.WorkflowContext;
import io.ironflow.sdk.WorkflowFuture;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The {@link WorkflowContext} implementation that makes replay work.
 *
 * <h2>How pausing works</h2>
 *
 * <p>When an SDK call finds no recorded outcome, it records a command and then blocks the
 * workflow thread forever on {@link #blockForever()}. The decision thread waits on a
 * separate latch that the workflow signals just before parking, so it wakes, collects the
 * commands, and returns. The parked workflow thread is then abandoned - never resumed,
 * never joined.</p>
 *
 * <p>Three alternatives were considered and rejected:</p>
 * <ul>
 *   <li><b>Throw a control-flow exception.</b> Destroys the call stack, so user
 *       {@code try/finally} runs at the wrong time and any {@code catch (Exception)}
 *       swallows the engine's control flow. Fatal.</li>
 *   <li><b>Return a future the workflow awaits.</b> Forces user code to be async all the
 *       way down, losing the "write ordinary imperative Java" property that is the entire
 *       point.</li>
 *   <li><b>Block the workflow thread.</b> Chosen. On a virtual thread, blocking is nearly
 *       free and user code stays plain sequential Java with intact stacks.</li>
 * </ul>
 *
 * <p>Abandoning a thread sounds alarming and is in fact correct. The thread is virtual, so
 * it costs a few hundred bytes rather than a megabyte of stack. It holds no database
 * resources - the decision commit happens on the decision thread after this returns. And
 * it <em>must not</em> be resumed: the next decision task builds a fresh workflow object
 * and replays from the top, which is precisely what makes state reconstruction verifiable
 * rather than dependent on surviving in-memory state.</p>
 *
 * <p>{@link #abandon()} interrupts it during shutdown so nothing leaks across a worker's
 * lifetime.</p>
 *
 * <h2>Why fresh objects every time</h2>
 *
 * <p>A workflow's entire state must be derivable from history. Reusing a workflow instance
 * across decision tasks would let state live in fields history does not describe - and the
 * first time a worker crashed, that state would be gone with no way to reconstruct it.</p>
 */
public final class ReplayContext implements WorkflowContext {

    /**
     * Marker identity for timers.
     *
     * <p>A constant rather than a per-call name, so two consecutive {@code sleep} calls are
     * distinguished only by cursor position. That is sufficient - position is the real
     * determinism check - but note the consequence: swapping two adjacent sleeps of
     * different durations is NOT caught as divergence, where swapping two differently-named
     * activities would be. Accepted, because requiring users to name every sleep would be a
     * poor trade.</p>
     */
    private static final String TIMER_IDENTITY = "__timer";

    /** Marker identity for {@link #now()}. Same positional-only caveat as timers. */
    private static final String NOW_IDENTITY = "__now";

    private final UUID executionId;
    private final EventHistoryCursor cursor;
    private final SignalInbox signals;
    private final CompensationStack compensations;
    private final ObjectMapper mapper;
    private final Logger userLogger;
    private final Thread workflowThread;

    /** Commands accumulated this decision task, flushed atomically on commit. */
    private final List<Command> commands = new ArrayList<>();

    /** Signalled by the workflow thread when it parks or finishes. */
    private final CountDownLatch decisionReady = new CountDownLatch(1);

    /**
     * Deterministic RNG, seeded from the execution id so replay reproduces the sequence.
     *
     * <p>No marker needed: seed plus call order fully determine every value. That makes
     * randomness far cheaper than {@link #now()}, which genuinely cannot be derived and
     * must be recorded.</p>
     */
    private final Random random;

    private long nextProvisionalSeq;

    /**
     * Command sequences scheduled via {@link #async} but not yet released by an
     * {@link #awaitAll}. Lets awaitAll validate that every future it is handed was really
     * scheduled within this execution, catching a fabricated or cross-boundary future as a
     * determinism error rather than a mysterious mismatch several steps later.
     */
    private final Set<Long> scheduledButUnawaited = new HashSet<>();

    /** Command sequences whose outcomes are confirmed present, so {@link #get} may resolve. */
    private final Set<Long> resolved = new HashSet<>();
    private volatile boolean parked;

    ReplayContext(UUID executionId, EventHistoryCursor cursor, SignalInbox signals,
                  CompensationStack compensations, ObjectMapper mapper,
                  Logger userLogger, Thread workflowThread) {
        this.executionId = executionId;
        this.cursor = cursor;
        this.signals = signals;
        this.compensations = compensations;
        this.mapper = mapper;
        this.userLogger = userLogger;
        this.workflowThread = workflowThread;
        this.random = new Random(executionId.getMostSignificantBits()
                ^ executionId.getLeastSignificantBits());
        this.nextProvisionalSeq = cursor.highWaterSeq() + 1;
    }

    // ---------------------------------------------------------------------------------
    // Activities.
    // ---------------------------------------------------------------------------------

    @Override
    public <T> T activity(String activityType, Object input, Class<T> resultType) {
        return activity(activityType, input, resultType, ActivityOptions.DEFAULT);
    }

    /**
     * The heart of the engine.
     *
     * <p>Three cases, in order:</p>
     * <ol>
     *   <li><b>History has the schedule and the outcome</b> - return the recorded result.
     *       The side effect is NOT re-executed. This is the case that makes durable
     *       execution durable.</li>
     *   <li><b>History has the schedule but no outcome</b> - the activity is still in
     *       flight. Park; a later decision task will find the outcome.</li>
     *   <li><b>Cursor exhausted</b> - genuinely new work. Emit a schedule command and
     *       park.</li>
     * </ol>
     */
    @Override
    public <T> T activity(String activityType, Object input, Class<T> resultType,
                          ActivityOptions options) {
        assertOnWorkflowThread();

        Optional<HistoryEvent> scheduled =
                cursor.nextCommand(EventTypes.ACTIVITY_SCHEDULED, activityType);

        if (scheduled.isEmpty()) {
            long seq = nextProvisionalSeq++;
            commands.add(new Command.ScheduleActivity(
                    seq, activityType, toJson(input), options));
            blockForever();
            throw new IllegalStateException("unreachable");
        }

        long scheduledSeq = scheduled.get().sequenceNumber();
        Optional<HistoryEvent> outcome = cursor.outcomeFor(scheduledSeq);

        if (outcome.isEmpty()) {
            blockForever();
            throw new IllegalStateException("unreachable");
        }

        HistoryEvent result = outcome.get();
        if (EventTypes.ACTIVITY_FAILED.equals(result.eventType())) {
            throw new ActivityFailure(activityType, scheduledSeq,
                    result.payload().path("failure").asText("unknown"));
        }
        return fromJson(result.payload().path("result"), resultType);
    }

    // ---------------------------------------------------------------------------------
    // Deterministic time and randomness.
    // ---------------------------------------------------------------------------------

    /**
     * Records a marker on first execution; returns the recorded instant on replay.
     *
     * <p>Unlike an activity, a marker does not park the workflow. The value is known
     * immediately - we simply must remember it, because the wall clock reads differently
     * next time. So we record the command and carry on in the same decision task.</p>
     */
    @Override
    public Instant now() {
        assertOnWorkflowThread();
        return marker(NOW_IDENTITY,
                () -> mapper.getNodeFactory().numberNode(System.currentTimeMillis()),
                node -> Instant.ofEpochMilli(node.asLong()));
    }

    @Override
    public Random random() {
        assertOnWorkflowThread();
        return random;
    }

    @Override
    public UUID randomUUID() {
        assertOnWorkflowThread();
        return new UUID(random.nextLong(), random.nextLong());
    }

    @Override
    public <T> T sideEffect(String name, Supplier<T> supplier, Class<T> resultType) {
        assertOnWorkflowThread();
        return marker(name, () -> toJson(supplier.get()), node -> fromJson(node, resultType));
    }

    /**
     * Shared marker mechanism for {@link #now()} and {@link #sideEffect}.
     *
     * @param name      marker identity, checked against history to catch reordering
     * @param produce   computes the value on first execution only
     * @param interpret converts a recorded JSON value back to its Java type
     */
    private <T> T marker(String name, Supplier<JsonNode> produce,
                         Function<JsonNode, T> interpret) {
        Optional<HistoryEvent> recorded =
                cursor.nextCommand(EventTypes.MARKER_RECORDED, name);

        if (recorded.isPresent()) {
            return interpret.apply(recorded.get().payload().path("value"));
        }

        JsonNode value = produce.get();
        long seq = nextProvisionalSeq++;
        commands.add(new Command.RecordMarker(seq, name, value));
        return interpret.apply(value);
    }

    // ---------------------------------------------------------------------------------
    // Timers.
    // ---------------------------------------------------------------------------------

        /**
     * Durable sleep.
     *
     * <h3>Why fire_at is computed here, not at dispatch</h3>
     *
     * <p>The absolute firing instant is computed from {@link #now()} - itself a replayed
     * marker - and recorded in the {@code TIMER_STARTED} event. It is NOT computed as
     * {@code now() + duration} when the row is written.</p>
     *
     * @param duration non-positive durations return immediately, matching
     * {@code Thread.sleep} semantics and avoiding a pointless round trip
     */
    @Override
    public void sleep(Duration duration) {
        assertOnWorkflowThread();

        if (duration.isNegative() || duration.isZero()) {
            return;
        }

        // We MUST call now() unconditionally. 
        // On first run: it records the __now marker which precedes the timer.
        // On replay: it consumes that __now marker so the cursor stays aligned.
        Instant fireAt = now();

        Optional<HistoryEvent> started =
                cursor.nextCommand(EventTypes.TIMER_STARTED, TIMER_IDENTITY);

        if (started.isEmpty()) {
            long seq = nextProvisionalSeq++;
            commands.add(new Command.StartTimer(seq, duration, fireAt));
            blockForever();
            throw new IllegalStateException("unreachable");
        }

        if (cursor.outcomeFor(started.get().sequenceNumber()).isEmpty()) {
            blockForever();
            throw new IllegalStateException("unreachable");
        }
        // TIMER_FIRED is in history. The sleep is over; continue.
    }

    // ---------------------------------------------------------------------------------
    // Signals.
    // ---------------------------------------------------------------------------------

    /**
     * Blocks until a signal of this name is available.
     *
     * <p>Two cases only - simpler than activities, because there is no "scheduled but
     * pending" state. Either history contains an unconsumed signal or it does not.</p>
     *
     * <p>Note what parking means here: unlike an activity, parking emits <b>no command</b>.
     * There is nothing to schedule; the workflow is waiting on the outside world. The
     * decision task ends with whatever commands preceded this call, and the workflow is
     * woken by {@code SignalService} when a signal actually arrives.</p>
     */
    @Override
    public <T> T waitForSignal(String signalName, Class<T> payloadType) {
        assertOnWorkflowThread();

        Optional<SignalInbox.SignalRecord> received = signals.consume(signalName);
        if (received.isEmpty()) {
            blockForever();
            throw new IllegalStateException("unreachable");
        }
        return fromJson(received.get().payload(), payloadType);
    }

    @Override
    public <T> Optional<T> pollSignal(String signalName, Class<T> payloadType) {
        assertOnWorkflowThread();
        return signals.consume(signalName).map(r -> fromJson(r.payload(), payloadType));
    }

    @Override
    public boolean hasSignal(String signalName) {
        assertOnWorkflowThread();
        return signals.has(signalName);
    }

    // ---------------------------------------------------------------------------------
    // Parallel branches (Promise.all).
    //
    // async and the existing activity() differ in exactly one respect: WHEN they park.
    // activity() schedules and parks in one call; async() schedules and returns, deferring
    // the park to awaitAll. That single difference is what allows several async calls to
    // accumulate their schedule commands before any of them suspends, so the decision commits
    // all of them together and they fan out in parallel.
    // ---------------------------------------------------------------------------------

    @Override
    public <T> WorkflowFuture<T> async(String activityType, Object... args) {
        @SuppressWarnings("unchecked")
        Class<T> inferred = (Class<T>) Object.class;
        return async(activityType, inferred, args);
    }

    @Override
    public <T> WorkflowFuture<T> async(String activityType, Class<T> resultType,
                                       Object... args) {
        assertOnWorkflowThread();

        Optional<HistoryEvent> scheduled =
                cursor.nextCommand(EventTypes.ACTIVITY_SCHEDULED, activityType);

        long seq;
        if (scheduled.isPresent()) {
            // Replay: this async was already recorded. Reuse its recorded sequence number so
            // the returned future points at the same history slot it did last time. We do NOT
            // park and do NOT re-emit the command - that already happened on the original run.
            // This is the entire reason async can return during replay without blocking.
            seq = scheduled.get().sequenceNumber();
        } else {
            // First execution: record the schedule command and move on. Unlike activity(),
            // we do NOT park here. Parking is deferred to awaitAll, which is what lets several
            // async calls accumulate before any of them suspends.
            seq = nextProvisionalSeq++;
            commands.add(new Command.ScheduleActivity(
                    seq, activityType, toJson(argsToInput(args)), ActivityOptions.DEFAULT));
        }

        scheduledButUnawaited.add(seq);
        return new WorkflowFuture<>(activityType, resultType, seq);
    }

    /**
     * The barrier. Parks unless EVERY future's outcome is already in history.
     *
     * <p>The asymmetry with {@link #async} is the whole design: async never parks, awaitAll
     * parks until the slowest branch lands.</p>
     */
    @Override
    public void awaitAll(WorkflowFuture<?>... futures) {
        assertOnWorkflowThread();

        if (futures.length == 0) {
            return;   // vacuously satisfied; awaiting nothing completes immediately
        }

        // Validate every future belongs to this execution's command stream. A future whose
        // seq we never assigned means the workflow fabricated it or carried it across a
        // continue-as-new boundary - both determinism violations, caught here rather than
        // producing a mysterious history mismatch three steps later.
        for (WorkflowFuture<?> f : futures) {
            if (f.commandSeq() >= nextProvisionalSeq
                    || (!scheduledButUnawaited.contains(f.commandSeq())
                        && !resolved.contains(f.commandSeq()))) {
                throw new NonDeterministicError(executionId, cursor.position(),
                        f.commandSeq(),
                        "a future scheduled within this execution",
                        "awaitAll on unknown future " + f);
            }
        }

        // Scan all futures. We check every one rather than short-circuiting on the first
        // missing outcome, because we also want to surface a failure from ANY completed
        // branch even while others are still pending - making awaitAll throw as soon as a
        // failure is known (rather than after the slowest branch finally lands) lets the
        // workflow compensate sooner.
        boolean allPresent = true;
        ActivityFailure firstFailure = null;

        for (WorkflowFuture<?> f : futures) {
            Optional<HistoryEvent> outcome = cursor.outcomeFor(f.commandSeq());
            if (outcome.isEmpty()) {
                allPresent = false;
                continue;
            }
            HistoryEvent ev = outcome.get();
            if (EventTypes.ACTIVITY_FAILED.equals(ev.eventType()) && firstFailure == null) {
                firstFailure = new ActivityFailure(f.activityType(), f.commandSeq(),
                        ev.payload().path("failure").asText("unknown"));
            }
        }

        if (firstFailure != null) {
            // Promise.all-style rejection: throw as soon as any branch's failure is known,
            // without waiting for slower branches. They finish in the background - their side
            // effects are at-least-once regardless - but the workflow does not wait for them
            // to learn what it already knows.
            throw firstFailure;
        }

        if (!allPresent) {
            // At least one branch is still in flight. Park. A later decision task, triggered
            // by whichever completion lands next, will replay to this same awaitAll and
            // re-check. We emit no new commands - the schedules were already committed on the
            // decision task where these futures' async calls first ran.
            blockForever();
            throw new IllegalStateException("unreachable");
        }

        // Every branch completed successfully. Release the barrier: these futures are now
        // resolvable via get().
        for (WorkflowFuture<?> f : futures) {
            scheduledButUnawaited.remove(f.commandSeq());
            resolved.add(f.commandSeq());
        }
    }

    @Override
    public <T> T get(WorkflowFuture<T> future) {
        assertOnWorkflowThread();

        if (!resolved.contains(future.commandSeq())) {
            // The result is not in history yet - get() was called before the awaitAll that
            // covers this future returned. Throwing beats returning null: a null here becomes
            // a wrong business decision downstream, far from its cause.
            throw new IllegalStateException(
                    "get() on " + future + " before its awaitAll returned. Await the future "
                    + "before reading its result.");
        }

        HistoryEvent outcome = cursor.outcomeFor(future.commandSeq()).orElseThrow(
                () -> new CorruptHistoryException(
                        "Future " + future + " marked resolved but has no outcome in history"));

        return fromJson(outcome.payload().path("result"), future.resultType());
    }

    /**
     * Wraps async varargs into the activity input shape.
     *
     * <p>A single argument passes through unchanged, so existing single-input activities are
     * invoked identically whether reached via {@code activity} or {@code async}. Multiple
     * arguments become a positional array; zero arguments become null.</p>
     */
    private Object argsToInput(Object[] args) {
        return switch (args.length) {
            case 0 -> null;
            case 1 -> args[0];
            default -> args;
        };
    }

    // ---------------------------------------------------------------------------------
    // Saga compensation.
    //
    // compensateWith records a COMPENSATION_REGISTERED marker and returns - it does NOT
    // park and it does NOT run anything now. The actual rollback is driven by the
    // COMPENSATING state (see ReplayRunner and CompensationCommitter), which derives the
    // LIFO stack by scanning history for these registrations in reverse. Keeping the stack
    // in history rather than in a field is what lets compensation survive the crash it
    // exists to handle.
    // ---------------------------------------------------------------------------------

    /**
     * @return {@code true} if any compensation is registered and not yet completed - either
     *         derived from history, or registered during THIS decision (a step that
     *         registered its compensation and failed in the same turn). Determines whether a
     *         thrown failure becomes a saga rollback or a plain FAILED.
     */
    boolean hasOutstandingCompensations() {
        if (compensations.hasOutstanding()) {
            return true;
        }
        return commands.stream().anyMatch(c -> c instanceof Command.RecordCompensation);
    }

    @Override
    public void compensateWith(String compensationType, Object... args) {
        assertOnWorkflowThread();

        // Advance the command cursor exactly like a marker, so a compensateWith call keeps
        // its position in the deterministic command stream. On replay the registration is
        // already in history and we simply consume it; on first execution we emit it.
        Optional<HistoryEvent> recorded =
                cursor.nextCommand(EventTypes.COMPENSATION_REGISTERED, compensationType);

        if (recorded.isPresent()) {
            return;   // already registered on the original run; nothing to re-emit
        }

        long seq = nextProvisionalSeq++;
        commands.add(new Command.RecordCompensation(
                seq, compensationType, toJson(argsToInput(args))));
    }

    // ---------------------------------------------------------------------------------
    // Accessors and internals.
    // ---------------------------------------------------------------------------------

    @Override
    public UUID executionId() {
        return executionId;
    }

    @Override
    public boolean isReplaying() {
        return cursor.isReplaying();
    }

    @Override
    public Logger logger() {
        return new ReplayAwareLogger(userLogger, this::isReplaying);
    }

    /**
     * Parks the workflow thread permanently, after signalling the decision thread.
     *
     * <p>Order matters: the latch must be released <em>before</em> parking, or the decision
     * thread waits forever on a workflow that is already asleep.</p>
     */
    private void blockForever() {
        parked = true;
        decisionReady.countDown();
        try {
            // Not LockSupport.park(): a spurious unpark would resume the workflow
            // mid-decision, which would be far worse than an unnecessary wait.
            new CountDownLatch(1).await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WorkflowAbandonedException();
        }
        throw new IllegalStateException("unreachable");
    }

    /**
     * Guards against workflow code spawning its own threads.
     *
     * <p>If a workflow calls SDK methods from two threads, the command sequence depends on
     * scheduling and replay diverges - an especially cruel bug, since it surfaces as an
     * unexplained {@link NonDeterministicError} weeks later on a workflow that has been
     * running fine.</p>
     */
    private void assertOnWorkflowThread() {
        if (Thread.currentThread() != workflowThread) {
            throw new NonDeterministicError(executionId, cursor.position(), -1,
                    "call on workflow thread '" + workflowThread.getName() + "'",
                    "call from thread '" + Thread.currentThread().getName()
                            + "' (workflow code must not spawn threads)");
        }
    }

    CountDownLatch decisionReady() {
        return decisionReady;
    }

    List<Command> commands() {
        return List.copyOf(commands);
    }

    boolean isParked() {
        return parked;
    }

    void abandon() {
        workflowThread.interrupt();
    }

    private JsonNode toJson(Object o) {
        return o == null ? mapper.nullNode() : mapper.valueToTree(o);
    }

    private <T> T fromJson(JsonNode node, Class<T> type) {
        try {
            if (node == null || node.isNull() || node.isMissingNode()) {
                return null;
            }
            return mapper.treeToValue(node, type);
        } catch (Exception e) {
            throw new CorruptHistoryException(
                    "Cannot deserialize recorded result as " + type.getName(), e);
        }
    }
}
