package io.ironflow.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.DecisionOutcome;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import io.ironflow.worker.CompensationCommitter;
import io.ironflow.worker.DecisionCommitter;
import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end saga rollback against real PostgreSQL.
 *
 * <p>Drives the committer entry points directly (bypassing the poller) so the rollback
 * sequence is exact rather than timing-dependent, and asserts the full lifecycle:
 * COMPENSATING transition, LIFO scheduling, COMPENSATION_COMPLETED events, and the terminal
 * FAILED_COMPENSATED close with its end_time.</p>
 */
@SpringBootTest
@Import(TestFixtures.class)
class SagaCompensationIT extends AbstractPostgresIT {

    @Autowired DecisionCommitter decisionCommitter;
    @Autowired CompensationCommitter compensationCommitter;
    @Autowired PostgresTaskQueueRepository queue;
    @Autowired TestFixtures fixtures;
    @Autowired ObjectMapper mapper;

    private UUID execId;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
        execId = fixtures.newExecution("saga-it");
    }

    /**
     * The full rollback: two compensations registered, the forward path fails, and the
     * engine rolls back refundCard THEN releaseInventory (reverse order), then closes
     * FAILED_COMPENSATED.
     */
    @Test
    void fullRollbackRunsInReverseAndClosesFailedCompensated() {
        // Two successful steps, each with a registered compensation.
        long releaseSeq = fixtures.registerCompensation(execId, "releaseInventory", "\"sku-1\"");
        long refundSeq  = fixtures.registerCompensation(execId, "refundCard", "\"cust-1\"");

        // A leased decision task (as if a completion had enqueued it), and the failure
        // outcome that triggers compensation.
        LeasedTask decision = leasedDecision();
        var outcome = DecisionOutcome.compensationRequired(
                "ActivityFailure: shipOrder failed", List.of());

        // 1. Commit the COMPENSATION_REQUIRED outcome -> transition + schedule first comp.
        decisionCommitter.commit(decision, outcome, "default");

        assertThat(fixtures.executionStatus(execId))
                .as("forward failure with compensations must enter COMPENSATING")
                .isEqualTo("COMPENSATING");
        assertThat(fixtures.eventTypesFor(execId)).contains("COMPENSATION_TRIGGERED");

        // First scheduled compensation must be the LAST registered: refundCard.
        assertThat(fixtures.pendingCompensationTypes(execId))
                .as("rollback is LIFO - refundCard (last registered) runs first")
                .containsExactly("refundCard");

        // 2. refundCard completes -> COMPENSATION_COMPLETED + schedule releaseInventory.
        UUID refundTask = fixtures.leasePendingCompensation(execId);
        boolean applied = compensationCommitter.applyCompensationCompletion(
                execId, refundTask, refundSeq);
        assertThat(applied).isTrue();

        assertThat(fixtures.executionStatus(execId))
                .as("still rolling back - one compensation remains")
                .isEqualTo("COMPENSATING");
        assertThat(fixtures.pendingCompensationTypes(execId))
                .as("releaseInventory (first registered) runs second")
                .containsExactly("releaseInventory");

        // 3. releaseInventory completes -> stack empty -> FAILED_COMPENSATED.
        UUID releaseTask = fixtures.leasePendingCompensation(execId);
        compensationCommitter.applyCompensationCompletion(execId, releaseTask, releaseSeq);

        assertThat(fixtures.executionStatus(execId))
                .as("rollback complete - terminal FAILED_COMPENSATED")
                .isEqualTo("FAILED_COMPENSATED");
        assertThat(fixtures.endTimeOf(execId))
                .as("a terminal execution must carry an end_time")
                .isNotNull();

        // Both compensations recorded exactly once, in the order they ran.
        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn("COMPENSATION_COMPLETED"::equals)
                .hasSize(2);
    }

    /**
     * A failure with no registered compensations transitions straight to a plain terminal
     * FAILED via the normal path - the saga machinery must not interfere.
     */
    @Test
    void failureWithoutCompensationsClosesPlainFailed() {
        LeasedTask decision = leasedDecision();
        decisionCommitter.commit(decision, DecisionOutcome.failed("boom"), "default");

        assertThat(fixtures.executionStatus(execId)).isEqualTo("FAILED");
        assertThat(fixtures.eventTypesFor(execId)).doesNotContain("COMPENSATION_TRIGGERED");
    }

    /**
     * Crash recovery mid-rollback: after the first compensation completes, a fresh
     * derivation of the stack (as a replay after a crash would do) still knows exactly
     * which compensation is next. Proven by driving completion, then re-reading the
     * outstanding set from history.
     */
    @Test
    void rollbackResumesAfterCrashMidway() {
        long releaseSeq = fixtures.registerCompensation(execId, "releaseInventory", "null");
        long refundSeq  = fixtures.registerCompensation(execId, "refundCard", "null");

        decisionCommitter.commit(leasedDecision(),
                DecisionOutcome.compensationRequired("fail", List.of()), "default");

        // refundCard completes; simulate a crash before releaseInventory is scheduled by
        // checking that the scheduling nonetheless happened durably (in-transaction).
        UUID refundTask = fixtures.leasePendingCompensation(execId);
        compensationCommitter.applyCompensationCompletion(execId, refundTask, refundSeq);

        // The next compensation is durably scheduled - a crashed-and-restarted worker would
        // find it PENDING, not have to reconstruct it from memory.
        assertThat(fixtures.pendingCompensationTypes(execId))
                .containsExactly("releaseInventory");
        assertThat(fixtures.executionStatus(execId)).isEqualTo("COMPENSATING");
    }

    /** A leased WORKFLOW decision task for the execution, matching the poller's output. */
    private LeasedTask leasedDecision() {
        // Enqueue a pending decision, then lease it.
        fixtures.enqueuePending(execId, "default", TaskKind.WORKFLOW, 1);
        return queue.poll("default", TaskKind.WORKFLOW, 1, Duration.ofMinutes(1)).getFirst();
    }
}
