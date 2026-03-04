package io.ironflow.persistence.repository;

import io.ironflow.persistence.entity.WfEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * Append-and-read repository for history.
 *
 * <p>No update or delete methods are exposed, and none should be added. The database
 * trigger would reject them anyway, but the absence of the method is the clearer
 * signal to the next reader that history is immutable by design.</p>
 */
public interface WfEventRepository extends JpaRepository<WfEvent, Long> {

    /**
     * Full history for replay, in the exact order the workflow originally observed.
     * This ordering is the replay contract.
     */
    List<WfEvent> findByExecutionIdOrderBySequenceNumberAsc(UUID executionId);

    /**
     * Incremental read from a cached checkpoint - avoids re-reading long histories on
     * every decision once the worker keeps a warm cache.
     */
    List<WfEvent> findByExecutionIdAndSequenceNumberGreaterThanOrderBySequenceNumberAsc(
            UUID executionId, long afterSequence);

    long countByExecutionId(UUID executionId);
}
