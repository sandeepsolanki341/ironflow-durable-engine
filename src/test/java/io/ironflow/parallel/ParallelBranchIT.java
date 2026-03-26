package io.ironflow.parallel;

import io.ironflow.orchestrator.ActivityCompletionResult;
import io.ironflow.orchestrator.OrchestratorTransactionManager;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import io.ironflow.worker.WaitCommitter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The fan-in lost-wakeup race, reproduced end to end against real PostgreSQL.
 *
 * <p>These tests hand-drive the exact statement interleaving that could strand a workflow
 * whose parallel branches all complete, verifying that {@link WaitCommitter} closes the
 * window. They deliberately bypass the poller so the interleaving is exact rather than
 * timing-dependent - a race test that relies on timing to hit the race is a test that passes
 * for the wrong reason.</p>
 */
@SpringBootTest
@Import(TestFixtures.class)
class ParallelBranchIT extends AbstractPostgresIT {

    @Autowired OrchestratorTransactionManager orchestrator;
    @Autowired WaitCommitter waitCommitter;
    @Autowired PostgresTaskQueueRepository queue;
    @Autowired TestFixtures fixtures;
    @Autowired ObjectMapper mapper;

    private UUID execId;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
        execId = fixtures.newExecution("fan-in-test");
    }

    /**
     * The stranding scenario, step by step:
     *
     * <ol>
     *   <li>Three branches are scheduled (as if a fan-out decision committed them).</li>
     *   <li>Branch A completes, enqueuing decision D.</li>
     *   <li>D is leased and replays; the workflow is still WAITING on B and C.</li>
     *   <li>Branch B completes WHILE D is leased - its decision enqueue is absorbed by
     *       ON CONFLICT.</li>
     *   <li>D acks. WITHOUT the fix, no open decision exists and B's completion is stranded.
     *       WITH WaitCommitter, D's ack observes that history advanced and re-enqueues.</li>
     * </ol>
     */
    @Test
    void completionDuringLeasedDecisionDoesNotStrandTheWorkflow() {
        // 1. Three parallel activity branches, leased-ready. scheduled_event_seq 2,3,4.
        UUID branchA = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 2);
        UUID branchB = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 3);
        UUID branchC = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 4);

        // 2. Branch A completes -> appends ACTIVITY_COMPLETED, enqueues decision D.
        ActivityCompletionResult a = orchestrator.applyActivityCompletion(
                execId, branchA, mapper.valueToTree(10), fixtures.versionOf(execId));
        assertThat(a.decisionTaskEnqueued()).isTrue();

        // 3. Lease D and capture the history high-water the "replay" would have seen.
        LeasedTask decision = queue.poll("default", TaskKind.WORKFLOW, 1,
                Duration.ofMinutes(1)).getFirst();
        long observedNextSeq = fixtures.nextSequenceOf(execId);   // what the replay read

        // 4. Branch B completes WHILE D is leased. Its decision enqueue hits ON CONFLICT
        //    (D is still LEASED) and is absorbed - this is the dangerous step.
        long versionBeforeB = fixtures.versionOf(execId);
        ActivityCompletionResult b = orchestrator.applyActivityCompletion(
                execId, branchB, mapper.valueToTree(20), versionBeforeB);
        assertThat(b.decisionTaskEnqueued())
                .as("B's decision enqueue must be absorbed while D is still leased")
                .isFalse();

        // 5. D acks via WaitCommitter, having observed history at step 3's high-water.
        //    Because B advanced next_sequence past observedNextSeq, this MUST re-enqueue.
        waitCommitter.commitWait(decision, "default", observedNextSeq);

        // The workflow must NOT be stranded: a fresh open decision must exist to observe B.
        assertThat(fixtures.countOpenDecisions(execId))
                .as("history advanced during the leased decision, so a new decision must "
                        + "exist - otherwise B's completion is stranded forever")
                .isEqualTo(1);
    }

    /**
     * The complementary case: if history did NOT advance during the leased decision, the ack
     * must stand with NO re-enqueue - otherwise a workflow genuinely waiting on a signal
     * would hot-loop.
     */
    @Test
    void genuineWaitAcksExactlyOnceWithNoReenqueue() {
        // One branch scheduled, one completed, decision enqueued. Then the workflow waits on
        // a second branch that has NOT completed - a genuine wait, no history movement.
        UUID branchA = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 2);
        fixtures.enqueueLeasedActivityAtSeq(execId, "default", 3);   // branchB, never completes

        orchestrator.applyActivityCompletion(
                execId, branchA, mapper.valueToTree(10), fixtures.versionOf(execId));

        LeasedTask decision = queue.poll("default", TaskKind.WORKFLOW, 1,
                Duration.ofMinutes(1)).getFirst();
        long observedNextSeq = fixtures.nextSequenceOf(execId);

        // No completion happens between the observation and the ack.
        waitCommitter.commitWait(decision, "default", observedNextSeq);

        assertThat(fixtures.countOpenDecisions(execId))
                .as("a genuine wait must not re-enqueue - that would hot-loop")
                .isZero();
    }

    /**
     * All three completing near-simultaneously: exactly one decision survives to observe the
     * full set, and the workflow is never stranded regardless of interleaving.
     */
    @Test
    void allBranchesCompletingConcurrentlyLeaveExactlyOneLiveDecision() {
        UUID branchA = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 2);
        UUID branchB = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 3);
        UUID branchC = fixtures.enqueueLeasedActivityAtSeq(execId, "default", 4);

        // A completes and enqueues D.
        orchestrator.applyActivityCompletion(
                execId, branchA, mapper.valueToTree(1), fixtures.versionOf(execId));
        LeasedTask decision = queue.poll("default", TaskKind.WORKFLOW, 1,
                Duration.ofMinutes(1)).getFirst();
        long observed = fixtures.nextSequenceOf(execId);

        // B and C both complete while D is leased; both enqueues absorbed by ON CONFLICT.
        orchestrator.applyActivityCompletion(
                execId, branchB, mapper.valueToTree(2), fixtures.versionOf(execId));
        orchestrator.applyActivityCompletion(
                execId, branchC, mapper.valueToTree(3), fixtures.versionOf(execId));

        // D acks; history advanced, so it re-enqueues exactly one decision.
        waitCommitter.commitWait(decision, "default", observed);

        assertThat(fixtures.countOpenDecisions(execId)).isEqualTo(1);
        // And all three completions are durably in history for that decision to observe.
        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn("ACTIVITY_COMPLETED"::equals)
                .hasSize(3);
    }
}
