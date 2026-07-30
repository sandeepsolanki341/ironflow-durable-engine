package io.ironflow.sdk;

import java.util.Objects;

/**
 * A handle to an activity whose result will appear in history.
 *
 * <h2>This is not a Java Future, and the difference is the whole point</h2>
 *
 * <p>There is no thread behind this object, no {@code get()} that blocks a pool thread, no
 * callback. It is a <em>token</em> recording "the workflow asked for activity X at command
 * position N". Resolving it means looking up position N's outcome in history - not waiting on
 * a computation.</p>
 *
 * <p>Why this matters: a workflow calling {@code ctx.async()} three times must return from
 * all three <em>immediately and synchronously</em>, so it reaches {@code awaitAll} with three
 * tokens in hand. If {@code async} blocked, the workflow would park on the first call and
 * never schedule the other two, collapsing the parallelism into sequential execution. The
 * token is inert by design.</p>
 *
 * <h2>Identity is positional</h2>
 *
 * <p>A future carries its {@code commandSeq} - the provisional sequence number assigned when
 * its {@code async} call was recorded. That number, not object identity, is how
 * {@code awaitAll} and {@code get} find the future's outcome in history. It is what keeps the
 * scheme deterministic: the third {@code async} call is always the third
 * {@code ACTIVITY_SCHEDULED} event, on every replay.</p>
 *
 * @param <T> the activity's result type
 */
public final class WorkflowFuture<T> {

    private final String activityType;
    private final Class<T> resultType;
    private final long commandSeq;

    /** Package-private: only {@link WorkflowContext} implementations construct these. */
    public WorkflowFuture(String activityType, Class<T> resultType, long commandSeq) {
        this.activityType = Objects.requireNonNull(activityType, "activityType");
        this.resultType = Objects.requireNonNull(resultType, "resultType");
        this.commandSeq = commandSeq;
    }

    public String activityType() {
        return activityType;
    }

    public Class<T> resultType() {
        return resultType;
    }

    public long commandSeq() {
        return commandSeq;
    }

    @Override
    public String toString() {
        return "WorkflowFuture[%s@%d]".formatted(activityType, commandSeq);
    }
}
