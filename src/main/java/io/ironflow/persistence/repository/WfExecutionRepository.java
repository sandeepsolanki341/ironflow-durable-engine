package io.ironflow.persistence.repository;

import io.ironflow.persistence.entity.WfExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Read-side and lifecycle repository for workflow executions.
 *
 * <p>The hot decision-commit path does not go through this interface - it uses jOOQ in
 * {@code DecisionCommitter} so that the version bump, task ack, event append and status
 * transition land in one hand-written transaction with a controlled statement order.
 * This repository serves the API and administrative queries.</p>
 */
public interface WfExecutionRepository extends JpaRepository<WfExecution, UUID> {

    /**
     * @param businessKey caller-supplied idempotency key
     * @return the execution holding this key, if any
     */
    Optional<WfExecution> findByBusinessKey(String businessKey);

    List<WfExecution> findByWorkflowTypeAndStatusOrderByStartTimeDesc(
            String workflowType, String status);

    /**
     * Reserves a contiguous block of history sequence numbers.
     *
     * <p>This is the serialization point for concurrent decision commits on one
     * execution. The UPDATE takes a row-level exclusive lock held until commit, so a
     * second committer blocks here rather than interleaving sequence numbers. Call this
     * <em>first</em> in any transaction that also touches {@code wf_tasks}, to establish
     * a consistent lock ordering and avoid deadlocks against the task-ack UPDATE.</p>
     *
     * <p>Read-modify-write in application code would be wrong here: two committers could
     * both read {@code next_sequence = 5} and both write 8, producing colliding event
     * ids that violate {@code uq_wf_events_seq}. The increment must happen inside the
     * database in one statement.</p>
     *
     * @param executionId execution to allocate against
     * @param count       number of sequence numbers required; must be >= 1
     * @return the first sequence number of the reserved block
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
        UPDATE wf_executions
           SET next_sequence = next_sequence + :count
         WHERE id = :executionId
        RETURNING next_sequence - :count
        """, nativeQuery = true)
    long reserveSequenceBlock(@Param("executionId") UUID executionId,
                              @Param("count") int count);
}
