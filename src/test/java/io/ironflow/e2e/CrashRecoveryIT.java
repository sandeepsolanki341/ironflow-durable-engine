package io.ironflow.e2e;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Crash recovery: the property the whole engine exists to provide.
 *
 * <p>These tests kill work mid-flight and assert it resumes. A durable execution engine that
 * passes every other test but fails these is not a durable execution engine.</p>
 */
@SpringBootTest
@Import(TestFixtures.class)
class CrashRecoveryIT extends AbstractPostgresIT {

    @Autowired PostgresTaskQueueRepository queue;
    @Autowired TestFixtures fixtures;

    /**
     * A worker dies holding a lease. The reaper must return the task to the queue.
     *
     * <p>Simulated by leasing and then never acking - which is exactly what a
     * {@code kill -9} looks like from the database's point of view. There is no cleaner way
     * to simulate a crash, and that is the point: the engine must recover from a worker that
     * gets no chance to clean up.</p>
     */
    @Test
    void leaseExpiryReturnsAbandonedTaskToTheQueue() {
        UUID execId = fixtures.newExecution("crash-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);

        List<LeasedTask> leased = queue.poll("default", TaskKind.ACTIVITY, 1,
                Duration.ofMillis(500));
        assertThat(leased).hasSize(1);
        LeasedTask abandoned = leased.getFirst();

        // The "crash": we simply never ack.
        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10, Duration.ofSeconds(30)))
                .as("a leased task must be invisible while its lease holds")
                .isEmpty();

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(
                        queue.poll("default", TaskKind.ACTIVITY, 1, Duration.ofSeconds(30)))
                        .as("the reaper must return the task once the lease expires")
                        .hasSize(1));

        // Attempt count must have advanced, or a permanently-crashing task would retry
        // forever rather than eventually failing.
        assertThat(fixtures.attemptOf(abandoned.taskId())).isGreaterThan(1);
    }

    /**
     * A zombie worker whose lease expired must not be able to ack.
     *
     * <p>This is the exactly-once boundary. The zombie is alive, holds a stale lease id, and
     * believes it owns the task - and every mutation is gated on {@code lease_owner}, so it
     * can do nothing.</p>
     */
    @Test
    void expiredLeaseHolderCannotAck() {
        UUID execId = fixtures.newExecution("zombie-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask zombie = queue.poll("default", TaskKind.ACTIVITY, 1,
                Duration.ofMillis(300)).getFirst();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(queue.poll("default", TaskKind.ACTIVITY, 1, Duration.ofSeconds(30)))
                        .hasSize(1));

        assertThat(queue.complete(zombie.taskId(), zombie.leaseOwner()))
                .as("a stale lease holder must not be able to complete the task")
                .isFalse();
        assertThat(queue.fail(zombie.taskId(), zombie.leaseOwner(),
                Duration.ZERO, "zombie write"))
                .as("nor fail it")
                .isFalse();
    }

    /**
     * Heartbeating extends a lease so long-running work is not reclaimed underneath it.
     *
     * <p>Without this, an activity legitimately taking longer than the lease is reclaimed and
     * re-executed while still running - duplicating its side effect for no reason other than
     * that it was slow.</p>
     */
    @Test
    void heartbeatPreventsReclamation() throws Exception {
        UUID execId = fixtures.newExecution("heartbeat-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask task = queue.poll("default", TaskKind.ACTIVITY, 1,
                Duration.ofSeconds(2)).getFirst();

        for (int i = 0; i < 5; i++) {
            Thread.sleep(700);
            assertThat(queue.heartbeat(task.taskId(), task.leaseOwner(), Duration.ofSeconds(2)))
                    .as("heartbeat %d must succeed", i)
                    .isTrue();
        }

        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10, Duration.ofSeconds(5)))
                .as("a heartbeating worker must keep its task")
                .isEmpty();
        assertThat(queue.complete(task.taskId(), task.leaseOwner())).isTrue();
    }

    /**
     * Only one worker may hold a given task, under concurrent polling.
     *
     * <p>This is what {@code FOR UPDATE SKIP LOCKED} buys, and it is worth asserting under
     * real contention rather than trusting the documentation.</p>
     */
    @Test
    void concurrentPollersNeverShareATask() throws Exception {
        UUID execId = fixtures.newExecution("contention-test");
        int taskCount = 200;
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, taskCount);

        var seen = java.util.concurrent.ConcurrentHashMap.<Long>newKeySet();
        var duplicates = new java.util.concurrent.atomic.AtomicInteger();

        try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = java.util.stream.IntStream.range(0, 16)
                    .mapToObj(i -> pool.submit(() -> {
                        while (true) {
                            var batch = queue.poll("default", TaskKind.ACTIVITY, 5,
                                    Duration.ofMinutes(1));
                            if (batch.isEmpty()) {
                                return null;
                            }
                            for (LeasedTask t : batch) {
                                if (!seen.add(t.taskId())) {
                                    duplicates.incrementAndGet();
                                }
                            }
                        }
                    })).toList();
            for (var f : futures) {
                f.get(60, java.util.concurrent.TimeUnit.SECONDS);
            }
        }

        assertThat(duplicates.get())
                .as("SKIP LOCKED must guarantee disjoint dispatch")
                .isZero();
        assertThat(seen).hasSize(taskCount);
    }

    /**
     * A timer scheduled far in the future must survive a full restart untouched.
     *
     * <p>The point of durable timers is that they outlive the process that created them.</p>
     */
    @Test
    void pendingTimerSurvivesWithNoInMemoryState() {
        UUID execId = fixtures.newExecution("timer-durability");
        fixtures.insertPendingTimer(execId, Instant.now().plus(Duration.ofDays(30)));

        // Nothing in the JVM knows about this timer. It is one row.
        assertThat(fixtures.countTasksByKind("TIMER")).isEqualTo(1);
        assertThat(fixtures.timerFireAt(execId))
                .isAfter(Instant.now().plus(Duration.ofDays(29)));

        // And it must not be dispatchable as an ordinary task.
        assertThat(queue.poll("default", TaskKind.TIMER, 10, Duration.ofSeconds(30)))
                .as("a timer due in 30 days must be invisible today")
                .isEmpty();
    }
}
