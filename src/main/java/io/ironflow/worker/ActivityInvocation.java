package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.queue.LeasedTask;
import io.ironflow.sdk.ActivityOptions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A parsed activity task payload.
 *
 * <p>The {@link ActivityOptions} come from the task payload, which was written from the
 * {@code ACTIVITY_SCHEDULED} event - NOT from current configuration. A workflow running for
 * a week must keep the retry policy it was scheduled with; resolving from config at retry
 * time would let a deploy retroactively exhaust in-flight activities.</p>
 */
public record ActivityInvocation(
        String activityType,
        JsonNode input,
        ActivityOptions options,
        ActivityContext activityContext,
        boolean isCompensation,
        long registrationSeq) {

    /**
     * Parses a leased task's payload.
     *
     * @throws IllegalArgumentException if the payload is absent or malformed
     */
    public static ActivityInvocation parse(LeasedTask task, ObjectMapper mapper)
            throws Exception {
        if (task.payload() == null || task.payload().length == 0) {
            throw new IllegalArgumentException(
                    "Activity task " + task.taskId() + " has no payload");
        }
        JsonNode root = mapper.readTree(
                new String(task.payload(), StandardCharsets.UTF_8));

        String activityType = root.path("activityType").asText();
        if (activityType.isEmpty()) {
            throw new IllegalArgumentException(
                    "Activity task " + task.taskId() + " payload has no activityType");
        }

        ActivityOptions options = parseOptions(root.path("options"));

        // A compensation activity is an ordinary activity from the executor's point of view -
        // it runs, retries, and can time out identically. The only difference is where its
        // completion is committed: the isCompensation flag routes it to the rollback path so
        // it appends COMPENSATION_COMPLETED and advances the saga, rather than the forward
        // ACTIVITY_COMPLETED path.
        boolean isCompensation = root.path("isCompensation").asBoolean(false);
        long registrationSeq = root.path("registrationSeq").asLong(-1);

        return new ActivityInvocation(
                activityType,
                root.path("input"),
                options,
                new ActivityContext(
                        task.executionId(),
                        task.taskUuid(),
                        activityType,
                        task.attempt(),
                        options.maxAttempts()),
                isCompensation,
                registrationSeq);
    }

    private static ActivityOptions parseOptions(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return ActivityOptions.DEFAULT;
        }
        List<String> nonRetryable = new ArrayList<>();
        node.path("nonRetryableErrors").forEach(n -> nonRetryable.add(n.asText()));

        return new ActivityOptions(
                node.path("taskQueue").isNull() ? null : node.path("taskQueue").asText(null),
                node.path("maxAttempts").asInt(ActivityOptions.DEFAULT.maxAttempts()),
                Duration.ofMillis(node.path("initialIntervalMillis")
                        .asLong(ActivityOptions.DEFAULT.initialInterval().toMillis())),
                node.path("backoffCoefficient")
                        .asDouble(ActivityOptions.DEFAULT.backoffCoefficient()),
                Duration.ofMillis(node.path("maxIntervalMillis")
                        .asLong(ActivityOptions.DEFAULT.maxInterval().toMillis())),
                Duration.ofMillis(node.path("timeoutMillis")
                        .asLong(ActivityOptions.DEFAULT.timeout().toMillis())),
                nonRetryable);
    }
}
