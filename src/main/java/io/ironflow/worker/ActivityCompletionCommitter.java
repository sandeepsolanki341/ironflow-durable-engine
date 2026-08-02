package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import io.ironflow.orchestrator.ActivityCompletionResult;
import io.ironflow.orchestrator.OrchestratorTransactionManager;
import io.ironflow.orchestrator.StaleExecutionException;
import io.ironflow.orchestrator.TaskNotOwnedException;
import io.ironflow.queue.LeasedTask;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Commits a successful activity result.
 *
 * <p>A thin adapter over {@link OrchestratorTransactionManager}: the fenced transition is
 * the same machinery whether the caller is an activity worker or an external orchestrator,
 * and duplicating it here would mean two places to keep the statement ordering correct.</p>
 *
 * <p>The one piece of work this layer does is reading the current version to use as the
 * fence. An activity worker has no prior view of the execution's version - it only knows
 * about its own task - so it reads immediately before the transition. That read-then-CAS is
 * safe precisely because the CAS itself is atomic: if the version moved between the read
 * and the write, the fence rejects it and the task retries.</p>
 */
@Service
public class ActivityCompletionCommitter {

    private static final Logger log =
            LoggerFactory.getLogger(ActivityCompletionCommitter.class);

    private static final String READ_VERSION_SQL = """
        SELECT current_version FROM wf_executions WHERE id = ? AND status = 'RUNNING'
        """;

    private final DSLContext dsl;
    private final OrchestratorTransactionManager orchestrator;

    public ActivityCompletionCommitter(DSLContext dsl,
                                       OrchestratorTransactionManager orchestrator) {
        this.dsl = dsl;
        this.orchestrator = orchestrator;
    }

    /**
     * Records the activity result and wakes the workflow.
     *
     * @return {@code false} if this worker no longer owns the task or the execution is
     *         closed. Both mean the result must be discarded - not retried, since another
     *         worker owns the task or there is nothing left to advance.
     */
    public boolean commitCompletion(LeasedTask task, ActivityInvocation invocation,
                                    JsonNode result) {
        Record versionRow = dsl.fetchOne(READ_VERSION_SQL, task.executionId());
        if (versionRow == null) {
            log.debug("Execution {} no longer RUNNING; discarding activity result",
                    task.executionId());
            return false;
        }
        long expectedVersion = versionRow.get(0, Long.class);

                try {
            ActivityCompletionResult applied = orchestrator.applyActivityCompletion(
                    task.executionId(), task.taskUuid(), task.scheduledEventSeq(), result, expectedVersion);

            if (log.isDebugEnabled()) {
                log.debug("Activity '{}' completed for execution {} at seq {}",
                        invocation.activityType(), task.executionId(),
                        applied.activityCompletedSeq());
            }
            return true;

        } catch (StaleExecutionException e) {
            // Another writer advanced the execution between our read and the CAS. The task
            // is still leased, so returning false lets the caller log it and the lease
            // expire, after which the reaper redelivers and we try again with a fresh read.
            log.warn("Lost the fence committing activity result for execution {}: {}",
                    task.executionId(), e.getMessage());
            return false;

        } catch (TaskNotOwnedException e) {
            log.warn("Lost lease committing activity result: {}", e.getMessage());
            return false;
        }
    }
}
