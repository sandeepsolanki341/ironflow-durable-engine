package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import io.ironflow.sdk.ActivityOptions;
import io.ironflow.worker.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes one activity task, applying its retry policy on failure.
 *
 * <h2>Where the transaction boundaries are</h2>
 *
 * <p>The activity body runs <b>outside</b> any transaction, exactly as workflow bodies do.
 * A transaction spanning a five-minute HTTP call would hold a pooled connection for five
 * minutes; a few dozen of those exhaust the pool and the engine deadlocks against itself.</p>
 *
 * <p>What happens after depends on the outcome, and the two paths have deliberately
 * different shapes:</p>
 *
 * <ul>
 *   <li><b>Retry</b> - one UPDATE on {@code wf_tasks}. Nothing else changes; history records
 *       nothing, because a failed attempt that will be retried is not yet an event in the
 *       workflow's story.</li>
 *   <li><b>Exhaustion</b> - an atomic transaction spanning {@code wf_tasks},
 *       {@code wf_events}, and a decision-task enqueue. See
 *       {@link ActivityFailureCommitter} for why those must be inseparable.</li>
 * </ul>
 *
 * <h2>Why timeout needs its own thread</h2>
 *
 * <p>{@code startToCloseTimeout} is enforced by running the activity on a separate virtual
 * thread and bounding the wait. An activity stuck in a socket read with no timeout of its
 * own would otherwise hold its worker slot indefinitely - the lease would expire, the reaper
 * would hand it to another worker, which would get stuck identically, and the fleet would
 * degrade one slot at a time.</p>
 *
 * <p>Note the honest limitation: on timeout we interrupt the thread, but a thread blocked in
 * a non-interruptible native call ignores that. We stop <em>waiting</em>; we cannot always
 * stop the <em>work</em>. Hence the explicit leak warning in
 * {@link #runWithTimeout}.</p>
 */
@Service
public class ActivityTaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(ActivityTaskExecutor.class);

    /** Grace period for an interrupted activity to unwind before we abandon its thread. */
    private static final Duration INTERRUPT_GRACE = Duration.ofSeconds(5);

    private static final int MAX_FAILURE_CHARS = 4_000;

    private final ActivityRegistry registry;
    private final PostgresTaskQueueRepository queue;
    private final ActivityFailureCommitter failureCommitter;
    private final ActivityCompletionCommitter completionCommitter;
    private final CompensationCommitter compensationCommitter;
    private final ObjectMapper mapper;

    public ActivityTaskExecutor(ActivityRegistry registry,
                                PostgresTaskQueueRepository queue,
                                ActivityFailureCommitter failureCommitter,
                                ActivityCompletionCommitter completionCommitter,
                                CompensationCommitter compensationCommitter,
                                ObjectMapper mapper) {
        this.registry = registry;
        this.queue = queue;
        this.failureCommitter = failureCommitter;
        this.completionCommitter = completionCommitter;
        this.compensationCommitter = compensationCommitter;
        this.mapper = mapper;
    }

    /**
     * Runs one activity task end to end.
     *
     * <p>Never throws. An exception escaping into the virtual thread would be swallowed by
     * the executor, leaving the task to expire silently and its failure unlogged - the worst
     * possible outcome, since the task still retries but nobody knows why.</p>
     */
    public void execute(LeasedTask task) {
        ActivityInvocation invocation;
        try {
            invocation = ActivityInvocation.parse(task, mapper);
        } catch (Exception e) {
            // A corrupt payload cannot be fixed by retrying, so fail immediately rather
            // than burning the retry budget on a certainty.
            log.error("Malformed activity payload on task {}; failing permanently",
                    task.taskId(), e);
            failureCommitter.commitFailure(task, "MalformedPayload: " + e.getMessage(),
                    ActivityOptions.DEFAULT);
            return;
        }

        ActivityOptions options = invocation.options();

        try {
            JsonNode result = runWithTimeout(invocation, options.timeout());

            boolean committed;
            if (invocation.isCompensation()) {
                // A compensation's completion advances the saga rollback, not the forward
                // path: it appends COMPENSATION_COMPLETED and either schedules the next
                // compensation or closes FAILED_COMPENSATED. The activity itself ran
                // identically; only the commit differs. The committer derives the queue for
                // any next compensation from the execution itself.
                committed = compensationCommitter.applyCompensationCompletion(
                        task.executionId(), task.taskUuid(), invocation.registrationSeq());
            } else {
                committed = completionCommitter.commitCompletion(task, invocation, result);
            }

            if (!committed) {
                // Lease lost mid-execution: the task was reclaimed and another worker owns
                // it. Discard silently - that worker will produce its own result. Retrying
                // here would race them.
                log.warn("Lost lease on activity task {} before commit; discarding result",
                        task.taskId());
            }
        } catch (Throwable failure) {
            handleFailure(task, invocation, options, failure);
        }
    }

    /**
     * Applies the retry policy to a failed attempt.
     *
     * <p>The branch order matters. Non-retryable types are checked <em>before</em> the
     * attempt count, so a validation error surfaces to the workflow on attempt 1 rather than
     * after five minutes of pointless backoff.</p>
     */
    private void handleFailure(LeasedTask task, ActivityInvocation invocation,
                               ActivityOptions options, Throwable failure) {

        int attempt = task.attempt();
        String detail = describe(failure);

        if (RetryPolicy.isNonRetryable(options, failure)) {
            log.warn("Activity '{}' task {} failed with non-retryable {}; surfacing to "
                            + "workflow immediately (attempt {}/{})",
                    invocation.activityType(), task.taskId(),
                    failure.getClass().getSimpleName(), attempt, options.maxAttempts());
            failureCommitter.commitFailure(task, detail, options);
            return;
        }

        if (RetryPolicy.shouldRetry(options, attempt, failure)) {
            Duration backoff = RetryPolicy.nextBackoff(options, attempt, task.taskId());
            boolean scheduled = queue.fail(task.taskId(), task.leaseOwner(), backoff, detail);

            if (scheduled) {
                log.info("Activity '{}' task {} failed (attempt {}/{}); retrying in {}ms: {}",
                        invocation.activityType(), task.taskId(), attempt,
                        options.maxAttempts(), backoff.toMillis(), detail);
            } else {
                // Lease expired while we were failing. The reaper already reclaimed it and
                // will apply its own backoff; ours is redundant.
                log.warn("Could not schedule retry for task {}: lease already lost",
                        task.taskId());
            }
            return;
        }

        log.error("Activity '{}' task {} exhausted {} attempt(s); surfacing failure to "
                        + "workflow: {}",
                invocation.activityType(), task.taskId(), options.maxAttempts(), detail);
        failureCommitter.commitFailure(task, detail, options);
    }

    /**
     * Runs the activity on a virtual thread, bounded by {@code timeout}.
     *
     * @throws ActivityTimeoutException if the activity did not finish in time
     */
    private JsonNode runWithTimeout(ActivityInvocation invocation, Duration timeout)
            throws Throwable {

        var future = new CompletableFuture<JsonNode>();
        Thread worker = Thread.ofVirtual()
                .name("ironflow-activity-" + invocation.activityType())
                .start(() -> {
                    try {
                        future.complete(registry.invoke(invocation.activityType(),
                                invocation.input(), invocation.activityContext()));
                    } catch (Throwable t) {
                        future.completeExceptionally(t);
                    }
                });

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

        } catch (TimeoutException e) {
            worker.interrupt();
            try {
                // Give it a moment to unwind cleanly so resources get released.
                future.get(INTERRUPT_GRACE.toMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {
                // Did not respond to interrupt. Be explicit about what that means: the
                // thread is abandoned and whatever it is doing continues. This is a real
                // leak and the log line must say so, or it looks like a clean timeout.
                log.warn("Activity '{}' did not respond to interrupt within {}s; thread "
                                + "abandoned and work may still be in flight. Activities "
                                + "performing network I/O must set their own socket timeouts.",
                        invocation.activityType(), INTERRUPT_GRACE.toSeconds());
            }
            throw new ActivityTimeoutException(invocation.activityType(), timeout);

        } catch (ExecutionException e) {
            // Unwrap so the retry policy sees the user's actual exception type, not the
            // ExecutionException wrapper - otherwise nonRetryableErrors never matches.
            throw e.getCause() == null ? e : e.getCause();

        } catch (CancellationException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
    }

    private static String describe(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable c = t; c != null && sb.length() < 3_500; c = c.getCause()) {
            if (!sb.isEmpty()) {
                sb.append(" <- ");
            }
            sb.append(c.getClass().getSimpleName());
            if (c.getMessage() != null) {
                sb.append(": ").append(c.getMessage());
            }
            if (c.getCause() == c) {
                break;
            }
        }
        String s = sb.toString();
        return s.length() > MAX_FAILURE_CHARS ? s.substring(0, MAX_FAILURE_CHARS) : s;
    }
}
