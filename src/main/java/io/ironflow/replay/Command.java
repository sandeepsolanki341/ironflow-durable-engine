package io.ironflow.replay;

import com.fasterxml.jackson.databind.JsonNode;
import io.ironflow.sdk.ActivityOptions;

import java.time.Duration;
import java.time.Instant;

/**
 * A command the workflow wants executed, pending atomic commit.
 *
 * <p>Sealed so the committer's switch is exhaustive: adding a command type without
 * handling it in the commit path becomes a compile error rather than a silently dropped
 * command.</p>
 */
public sealed interface Command {

    /**
     * @return provisional sequence id, used only to correlate commands within one decision
     *         task. Real sequence numbers are assigned by the committing transaction's
     *         block reservation.
     */
    long provisionalSeq();

    record ScheduleActivity(long provisionalSeq, String activityType,
                            JsonNode input, ActivityOptions options) implements Command { }

    /**
     * @param duration the requested sleep, retained for history readability
     * @param fireAt   the absolute deadline, derived from replayed time. This is what the
     *                 committer writes to {@code not_before} - never {@code now() +
     *                 duration}, which would drift on decision retry.
     */
    record StartTimer(long provisionalSeq, Duration duration, Instant fireAt)
            implements Command { }

    record RecordMarker(long provisionalSeq, String name, JsonNode value)
            implements Command { }

    /**
     * Registration of a compensation action, written as {@code COMPENSATION_REGISTERED}.
     *
     * <p>Like a marker it does not park the workflow - it records the intent to be able to
     * undo a step later. Unlike a marker it carries the compensating activity's type and
     * input, so the {@code COMPENSATING} state can schedule it without re-running any
     * workflow code.</p>
     *
     * @param compensationType the activity to run if a later step fails
     * @param input            its serialized input
     */
    record RecordCompensation(long provisionalSeq, String compensationType, JsonNode input)
            implements Command { }
}
