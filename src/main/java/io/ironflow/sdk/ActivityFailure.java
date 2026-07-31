package io.ironflow.sdk;

/**
 * Thrown into workflow code when an activity fails after exhausting its retries, or fails
 * with a non-retryable exception type.
 *
 * <p>Catchable by design. A workflow that catches this can run compensation, try a
 * different approach, or continue - and that decision is itself recorded in history, so it
 * replays identically.</p>
 */
public class ActivityFailure extends RuntimeException {

    private final String activityType;
    private final long scheduledEventSeq;

    public ActivityFailure(String activityType, long scheduledEventSeq, String detail) {
        super("Activity '%s' (scheduled at seq %d) failed: %s"
                .formatted(activityType, scheduledEventSeq, detail));
        this.activityType = activityType;
        this.scheduledEventSeq = scheduledEventSeq;
    }

    public String getActivityType() {
        return activityType;
    }

    public long getScheduledEventSeq() {
        return scheduledEventSeq;
    }
}
