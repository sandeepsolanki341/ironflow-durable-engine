package io.ironflow.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.EventTypes;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Split-brain protection: the optimistic fence must admit exactly one writer.
 */
@SpringBootTest
@Import(TestFixtures.class)
class OrchestratorFenceIT extends AbstractPostgresIT {

    @Autowired OrchestratorTransactionManager orchestrator;
    @Autowired TestFixtures fixtures;
    @Autowired ObjectMapper mapper;

    private UUID execId;
    private UUID taskId;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
        execId = fixtures.newExecution("fence-test");
        taskId = fixtures.enqueueLeasedActivity(execId, "default");
    }

    @Test
    void winningTransitionAppliesAllSixOperations() {
        long version = fixtures.versionOf(execId);

        var result = orchestrator.applyActivityCompletion(
                execId, taskId, mapper.valueToTree("ok"), version);

        assertThat(result.newVersion()).isEqualTo(version + 1);
        assertThat(fixtures.versionOf(execId)).isEqualTo(version + 1);
        assertThat(fixtures.statusOfTask(taskId)).isEqualTo("COMPLETED");
        assertThat(fixtures.eventTypesFor(execId))
                .containsSubsequence(EventTypes.ACTIVITY_COMPLETED,
                        EventTypes.WORKFLOW_TASK_SCHEDULED);
        assertThat(result.decisionTaskEnqueued()).isTrue();
    }

    /** The core split-brain assertion. */
    @Test
    void staleVersionIsRejectedEntirely() {
        long version = fixtures.versionOf(execId);
        orchestrator.applyActivityCompletion(execId, taskId, mapper.valueToTree("first"),
                version);

        int eventsAfterFirst = fixtures.countEvents();
        UUID secondTask = fixtures.enqueueLeasedActivity(execId, "default");

        assertThatThrownBy(() -> orchestrator.applyActivityCompletion(
                execId, secondTask, mapper.valueToTree("second"), version))
                .isInstanceOfSatisfying(StaleExecutionException.class, e -> {
                    assertThat(e.isRetryable()).isTrue();
                    assertThat(e.getActualVersion()).isEqualTo(version + 1);
                });

        assertThat(fixtures.countEvents())
                .as("the loser must leave no trace at all")
                .isEqualTo(eventsAfterFirst);
        assertThat(fixtures.statusOfTask(secondTask))
                .as("the loser's task must remain leased, not acked")
                .isEqualTo("LEASED");
    }

    /**
     * Under real concurrency, exactly one writer wins per version.
     *
     * <p>Barrier-synchronised so all threads attempt the CAS in the same instant, which is
     * the only way to exercise the fence rather than a sequence of uncontended writes.</p>
     */
    @Test
    void concurrentWritersProduceExactlyOneWinner() throws Exception {
        int writers = 32;
        long version = fixtures.versionOf(execId);
        var tasks = IntStream.range(0, writers)
                .mapToObj(i -> fixtures.enqueueLeasedActivity(execId, "default"))
                .toList();

        var barrier = new CyclicBarrier(writers);
        var winners = new AtomicInteger();
        var losers = new ConcurrentLinkedQueue<StaleExecutionException>();

        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = IntStream.range(0, writers).mapToObj(i -> pool.submit(() -> {
                barrier.await(30, TimeUnit.SECONDS);
                try {
                    orchestrator.applyActivityCompletion(
                            execId, tasks.get(i), mapper.valueToTree(i), version);
                    winners.incrementAndGet();
                } catch (StaleExecutionException e) {
                    losers.add(e);
                }
                return null;
            })).toList();
            for (var f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        }

        assertThat(winners.get())
                .as("exactly one writer may win a given version")
                .isEqualTo(1);
        assertThat(losers).hasSize(writers - 1);
        assertThat(fixtures.versionOf(execId)).isEqualTo(version + 1);
        assertThat(fixtures.countEvents())
                .as("only the winner's two events may exist")
                .isEqualTo(2);
    }

    /**
     * A closed execution must be distinguishable from contention, so callers stop retrying
     * work that can never succeed.
     */
    @Test
    void closedExecutionIsPermanentNotRetryable() {
        long version = fixtures.versionOf(execId);
        fixtures.closeExecution(execId, "CANCELLED");

        assertThatThrownBy(() -> orchestrator.applyActivityCompletion(
                execId, taskId, mapper.valueToTree("late"), version))
                .isInstanceOfSatisfying(StaleExecutionException.class, e -> {
                    assertThat(e.isPermanent()).isTrue();
                    assertThat(e.isRetryable()).isFalse();
                    assertThat(e.getActualStatus()).isEqualTo("CANCELLED");
                });
    }

    /** An unleased task must not be ackable, even with a correct version. */
    @Test
    void unleasedTaskIsRejected() {
        UUID pending = fixtures.enqueuePendingActivity(execId, "default");
        long version = fixtures.versionOf(execId);

        assertThatThrownBy(() -> orchestrator.applyActivityCompletion(
                execId, pending, mapper.valueToTree("x"), version))
                .isInstanceOf(TaskNotOwnedException.class);

        assertThat(fixtures.versionOf(execId))
                .as("the version bump must roll back with the failed ack")
                .isEqualTo(version);
    }

    /** A task id belonging to another execution must not be ackable. */
    @Test
    void crossExecutionTaskIdIsRejected() {
        UUID otherExec = fixtures.newExecution("other");
        UUID otherTask = fixtures.enqueueLeasedActivity(otherExec, "default");
        long version = fixtures.versionOf(execId);

        assertThatThrownBy(() -> orchestrator.applyActivityCompletion(
                execId, otherTask, mapper.valueToTree("x"), version))
                .isInstanceOf(TaskNotOwnedException.class);

        assertThat(fixtures.statusOfTask(otherTask))
                .as("the other execution's task must be untouched")
                .isEqualTo("LEASED");
    }

    /**
     * Parallel fan-in: the second completion must not fail just because a decision task is
     * already open.
     */
    @Test
    void parallelCompletionsAbsorbDuplicateDecisionEnqueue() {
        UUID taskA = taskId;
        UUID taskB = fixtures.enqueueLeasedActivity(execId, "default");

        var first = orchestrator.applyActivityCompletion(
                execId, taskA, mapper.valueToTree("a"), fixtures.versionOf(execId));
        assertThat(first.decisionTaskEnqueued()).isTrue();

        var second = orchestrator.applyActivityCompletion(
                execId, taskB, mapper.valueToTree("b"), fixtures.versionOf(execId));

        assertThat(second.decisionTaskEnqueued())
                .as("the one-open-decision index must absorb the second enqueue")
                .isFalse();
        assertThat(fixtures.eventTypesFor(execId))
                .as("but both completions must still be recorded")
                .filteredOn(EventTypes.ACTIVITY_COMPLETED::equals)
                .hasSize(2);
    }

    /** Sequence numbers must be gap-free and correctly ordered. */
    @Test
    void sequenceNumbersAreContiguousAndOrdered() {
        var result = orchestrator.applyActivityCompletion(
                execId, taskId, mapper.valueToTree("ok"), fixtures.versionOf(execId));

        assertThat(result.workflowTaskScheduledSeq())
                .isEqualTo(result.activityCompletedSeq() + 1);

        List<Long> seqs = fixtures.eventSequencesFor(execId);
        for (int i = 1; i < seqs.size(); i++) {
            assertThat(seqs.get(i)).isEqualTo(seqs.get(i - 1) + 1);
        }
    }

    /** The decision task must inherit the activity's queue, not fall back to default. */
    @Test
    void decisionTaskInheritsSourceQueue() {
        UUID dedicated = fixtures.newExecution("routing-test");
        UUID task = fixtures.enqueueLeasedActivity(dedicated, "gpu-workers");

        orchestrator.applyActivityCompletion(
                dedicated, task, mapper.valueToTree("ok"), fixtures.versionOf(dedicated));

        assertThat(fixtures.queueOfPendingDecision(dedicated))
                .as("executions must stay pinned to their worker pool")
                .isEqualTo("gpu-workers");
    }
}
