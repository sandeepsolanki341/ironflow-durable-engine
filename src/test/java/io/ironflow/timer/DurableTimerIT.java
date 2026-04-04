package io.ironflow.timer;

import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.EventTypes;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Durable timer firing and sharding.
 */
@SpringBootTest
@Import(TestFixtures.class)
class DurableTimerIT extends AbstractPostgresIT {

    @Autowired TimerFiringRepository timers;
    @Autowired TestFixtures fixtures;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
    }

    /** Firing must mark the timer, append the event, and enqueue a decision - together. */
    @Test
    void dueTimerFiresAtomically() {
        UUID execId = fixtures.newExecution("timer-fire-test");
        fixtures.insertPendingTimer(execId, Instant.now().minusSeconds(1));
        short shard = ShardAssignment.shardFor(execId);
        long versionBefore = fixtures.versionOf(execId);

        int fired = timers.fireDueTimers(shard, 100);

        assertThat(fired).isEqualTo(1);
        assertThat(fixtures.versionOf(execId)).isEqualTo(versionBefore + 1);
        assertThat(fixtures.eventTypesFor(execId))
                .containsSubsequence(EventTypes.TIMER_FIRED,
                        EventTypes.WORKFLOW_TASK_SCHEDULED);
        assertThat(fixtures.countTasksByKind("WORKFLOW")).isEqualTo(1);
        assertThat(fixtures.countTasksByStatus("PENDING"))
                .as("the fired timer must have left PENDING")
                .isEqualTo(1);   // only the new decision task remains
    }

    /** A timer that is not yet due must not fire. */
    @Test
    void futureTimerDoesNotFire() {
        UUID execId = fixtures.newExecution("future-timer-test");
        fixtures.insertPendingTimer(execId, Instant.now().plus(Duration.ofDays(30)));

        assertThat(timers.fireDueTimers(ShardAssignment.shardFor(execId), 100))
                .isZero();
        assertThat(fixtures.eventTypesFor(execId)).doesNotContain(EventTypes.TIMER_FIRED);
    }

    /** A poller must only see its own shard. */
    @Test
    void timersAreIsolatedByShard() {
        UUID execId = fixtures.newExecution("shard-isolation-test");
        fixtures.insertPendingTimer(execId, Instant.now().minusSeconds(1));
        short owning = ShardAssignment.shardFor(execId);

        for (int shard = 0; shard < ShardAssignment.SHARD_COUNT; shard++) {
            if (shard != owning) {
                assertThat(timers.fireDueTimers(shard, 100))
                        .as("shard %d must not see a timer owned by shard %d", shard, owning)
                        .isZero();
            }
        }
        assertThat(timers.fireDueTimers(owning, 100)).isEqualTo(1);
    }

    /**
     * A timer outliving its execution must drop cleanly, not block its batch.
     *
     * <p>Throwing on a cancelled execution would abort the whole batch and roll back timers
     * that fired fine, so one cancelled workflow would block every other timer in its
     * shard.</p>
     */
    @Test
    void timerForClosedExecutionDropsWithoutBlockingOthers() {
        // Both on the same shard, so one batch covers both.
        UUID cancelled = null;
        UUID healthy = null;
        short targetShard = 0;
        while (cancelled == null || healthy == null) {
            UUID candidate = UUID.randomUUID();
            if (ShardAssignment.shardFor(candidate) != targetShard) {
                continue;
            }
            if (cancelled == null) {
                cancelled = fixtures.newExecutionWithId(candidate, "cancelled-wf");
            } else {
                healthy = fixtures.newExecutionWithId(candidate, "healthy-wf");
            }
        }

        fixtures.insertPendingTimer(cancelled, Instant.now().minusSeconds(1));
        fixtures.insertPendingTimer(healthy, Instant.now().minusSeconds(1));
        fixtures.closeExecution(cancelled, "CANCELLED");

        int fired = timers.fireDueTimers(targetShard, 100);

        assertThat(fired)
                .as("only the healthy execution's timer counts as fired")
                .isEqualTo(1);
        assertThat(fixtures.eventTypesFor(healthy)).contains(EventTypes.TIMER_FIRED);
        assertThat(fixtures.eventTypesFor(cancelled)).doesNotContain(EventTypes.TIMER_FIRED);
    }

    /** Batch size must bound how much one transaction claims. */
    @Test
    void batchSizeBoundsClaimedTimers() {
        short targetShard = 0;
        List<UUID> execIds = new ArrayList<>();
        while (execIds.size() < 10) {
            UUID candidate = UUID.randomUUID();
            if (ShardAssignment.shardFor(candidate) == targetShard) {
                execIds.add(fixtures.newExecutionWithId(candidate, "batch-test"));
            }
        }
        execIds.forEach(id ->
                fixtures.insertPendingTimer(id, Instant.now().minusSeconds(1)));

        assertThat(timers.fireDueTimers(targetShard, 3)).isEqualTo(3);
        assertThat(timers.fireDueTimers(targetShard, 3)).isEqualTo(3);
        assertThat(timers.fireDueTimers(targetShard, 100)).isEqualTo(4);
        assertThat(timers.fireDueTimers(targetShard, 100)).isZero();
    }

    /** Firing twice must be impossible - the claim removes the row from PENDING. */
    @Test
    void timerFiresExactlyOnce() {
        UUID execId = fixtures.newExecution("once-test");
        fixtures.insertPendingTimer(execId, Instant.now().minusSeconds(1));
        short shard = ShardAssignment.shardFor(execId);

        assertThat(timers.fireDueTimers(shard, 100)).isEqualTo(1);
        assertThat(timers.fireDueTimers(shard, 100)).isZero();

        assertThat(fixtures.eventTypesFor(execId))
                .filteredOn(EventTypes.TIMER_FIRED::equals)
                .as("SKIP LOCKED plus the status transition must prevent double-firing")
                .hasSize(1);
    }

    /**
     * The scale claim, tested against the query plan: many sleepers must cost nothing.
     */
    @Test
    void manySleepersDoNotDegradeTheIndex() {
        for (int i = 0; i < 5_000; i++) {
            UUID execId = fixtures.newExecutionWithId(UUID.randomUUID(), "sleeper");
            fixtures.insertPendingTimer(execId, Instant.now().plus(Duration.ofDays(30)));
        }
        fixtures.analyze();

        String plan = fixtures.explainTimerPoll(0);
        assertThat(plan)
                .as("the partial index must be used, not a sequential scan")
                .contains("idx_wf_tasks_timer_shard");
        assertThat(plan).doesNotContain("Seq Scan");
    }

    /** Concurrent pollers on the same shard must not double-fire. */
    @Test
    void overlappingPollersDoNotDoubleFire() throws Exception {
        short targetShard = 0;
        List<UUID> execIds = new ArrayList<>();
        while (execIds.size() < 40) {
            UUID candidate = UUID.randomUUID();
            if (ShardAssignment.shardFor(candidate) == targetShard) {
                execIds.add(fixtures.newExecutionWithId(candidate, "overlap-test"));
            }
        }
        execIds.forEach(id ->
                fixtures.insertPendingTimer(id, Instant.now().minusSeconds(1)));

        var totalFired = new java.util.concurrent.atomic.AtomicInteger();
        try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = java.util.stream.IntStream.range(0, 8)
                    .mapToObj(i -> pool.submit(() -> {
                        int fired;
                        do {
                            fired = timers.fireDueTimers(targetShard, 5);
                            totalFired.addAndGet(fired);
                        } while (fired > 0);
                        return null;
                    })).toList();
            for (var f : futures) {
                f.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
        }

        assertThat(totalFired.get())
                .as("SKIP LOCKED must make overlap wasteful, not incorrect")
                .isEqualTo(40);
        for (UUID id : execIds) {
            assertThat(fixtures.eventTypesFor(id))
                    .filteredOn(EventTypes.TIMER_FIRED::equals).hasSize(1);
        }
    }

    /** Java and SQL shard functions must agree, or timers silently go unpolled. */
    @Test
    void javaShardMatchesStoredShard() {
        for (int i = 0; i < 200; i++) {
            UUID id = UUID.randomUUID();
            fixtures.newExecutionWithId(id, "shard-agreement-test");
            fixtures.insertPendingTimer(id, Instant.now().plusSeconds(3600));

            assertThat(fixtures.shardOfTimer(id))
                    .as("the shard written must match what the poller queries")
                    .isEqualTo(ShardAssignment.shardFor(id));
        }
    }
}
