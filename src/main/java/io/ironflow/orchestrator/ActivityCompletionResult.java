package io.ironflow.orchestrator;

import java.util.UUID;

/**
 * Outcome of a successfully applied activity completion.
 *
 * @param newVersion               the version after the fence bump. Callers driving further
 *                                 transitions must pass this as the next
 *                                 {@code expectedVersion} rather than computing it, which
 *                                 keeps the increment an implementation detail.
 * @param decisionTaskEnqueued     {@code false} when a decision task was already open and
 *                                 the one-open-decision index absorbed the insert. Not a
 *                                 failure - the pending decision will observe these events
 *                                 when it runs. Surfaced because a caller waiting on a
 *                                 fresh task id needs to know none was created.
 */
public record ActivityCompletionResult(
        UUID executionId,
        long newVersion,
        long activityCompletedSeq,
        long workflowTaskScheduledSeq,
        boolean decisionTaskEnqueued) { }
