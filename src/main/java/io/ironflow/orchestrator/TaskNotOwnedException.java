package io.ironflow.orchestrator;

import java.util.UUID;

/**
 * Thrown when the task being completed was not in {@code LEASED} state.
 *
 * <p>Means the lease expired and the reaper reclaimed the task while the activity was still
 * executing. The side effect really happened, but this node no longer holds the right to
 * record it - another worker owns the task now and will re-execute it.</p>
 *
 * <p>Distinct from {@link StaleExecutionException}: that one says "someone else advanced
 * the execution", this one says "you lost your claim on this specific task". Both roll the
 * transaction back; the operational responses differ, since a rising rate of this exception
 * specifically means leases are too short for the activities being run.</p>
 */
public class TaskNotOwnedException extends RuntimeException {

    private final UUID taskId;
    private final UUID executionId;

    public TaskNotOwnedException(UUID taskId, UUID executionId) {
        super(("Task %s (execution %s) was not LEASED; lease expired and it was reclaimed. "
                + "Discard this result - another worker owns the task.")
                .formatted(taskId, executionId));
        this.taskId = taskId;
        this.executionId = executionId;
    }

    public UUID getTaskId() {
        return taskId;
    }

    public UUID getExecutionId() {
        return executionId;
    }
}
