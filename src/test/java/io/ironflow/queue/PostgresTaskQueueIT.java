package io.ironflow.queue;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the correctness properties the queue's guarantees rest on.
 *
 * <p>The headline test is {@link #concurrentPollersNeverReceiveDuplicateTasks}. A naive
 * version of it - spawn threads, poll until empty, assert no duplicates - passes
 * trivially even against a broken implementation, because if the threads never actually
 * overlap in time there was no contention to survive. The version here forces genuine
 * simultaneity with a {@link CyclicBarrier} and then asserts that overlap really
 * occurred, so a future refactor that accidentally serialises polling fails loudly
 * instead of passing silently.</p>
 */
@SpringBootTest
@Import(TestFixtures.class)
class PostgresTaskQueueIT extends AbstractPostgresIT {

    @Autowired
    private PostgresTaskQueueRepository queue;
    @Autowired
    private LeaseReaper reaper;
    @Autowired
    private TestFixtures fixtures;

    @BeforeEach
    void reset() {
        fixtures.truncateAll();
    }

    // ---------------------------------------------------------------------------------
    // The headline property.
    // ---------------------------------------------------------------------------------

    /**
     * Under maximum contention, every task is leased exactly once.
     *
     * <p>A duplicate lease here would mean duplicate side effects in production - two
     * workers charging the same credit card. This is the single most important assertion
     * in the codebase.</p>
     *
     * <p>Repeated because SKIP LOCKED races are timing-dependent: one green run is weak
     * evidence, and a broken implementation can easily pass once.</p>
     */
    @RepeatedTest(3)
    void concurrentPollersNeverReceiveDuplicateTasks() throws Exception {
        final int taskCount = 2_000;
        final int pollerCount = 64;
        // Small batches relative to poller count maximise concurrent claim attempts
        // against the same index range.
        final int batchSize = 5;

        UUID executionId = fixtures.newExecution("contention-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, taskCount);

        var leasedIds = ConcurrentHashMap.<Long>newKeySet();
        var duplicates = new ConcurrentLinkedQueue<Long>();
        var leaseTokens = new ConcurrentLinkedQueue<UUID>();

        var barrier = new CyclicBarrier(pollerCount);
        var inFlight = new AtomicInteger();
        var maxConcurrent = new AtomicInteger();

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, pollerCount)
                    .mapToObj(i -> pool.submit(() -> {
                        barrier.await(30, TimeUnit.SECONDS);   // release together

                        int emptyPolls = 0;
                        while (emptyPolls < 3) {
                            int now = inFlight.incrementAndGet();
                            maxConcurrent.accumulateAndGet(now, Math::max);
                            List<LeasedTask> batch;
                            try {
                                batch = queue.poll("default", TaskKind.ACTIVITY, batchSize);
                            } finally {
                                inFlight.decrementAndGet();
                            }

                            if (batch.isEmpty()) {
                                emptyPolls++;
                                Thread.sleep(5);
                                continue;
                            }
                            emptyPolls = 0;
                            for (LeasedTask t : batch) {
                                // Set semantics give duplicate detection for free:
                                // add() returns false iff this id was already claimed.
                                if (!leasedIds.add(t.taskId())) {
                                    duplicates.add(t.taskId());
                                }
                                leaseTokens.add(t.leaseOwner());
                            }
                        }
                        return null;
                    }))
                    .toList();

            for (Future<?> f : futures) {
                f.get(180, TimeUnit.SECONDS);
            }
        }

        assertThat(duplicates)
                .as("SKIP LOCKED must never hand the same row to two pollers")
                .isEmpty();
        assertThat(leasedIds)
                .as("every enqueued task must be claimed exactly once")
                .hasSize(taskCount);
        assertThat(maxConcurrent.get())
                .as("test is only meaningful if polls actually overlapped in time")
                .isGreaterThan(1);
        assertThat(fixtures.countByStatus("LEASED")).isEqualTo(taskCount);
        assertThat(fixtures.countByStatus("PENDING")).isZero();
        assertThat(new HashSet<>(leaseTokens))
                .as("each poll() call must issue a distinct ownership token")
                .hasSizeGreaterThan(1);
    }

    /**
     * SKIP LOCKED must not merely avoid duplicates - it must avoid <em>blocking</em>. If
     * a poller ever waited on another poller's row lock, throughput would collapse to
     * single-consumer and the whole broker-free design would be pointless.
     */
    @Test
    void pollersDoNotBlockEachOther() throws Exception {
        UUID executionId = fixtures.newExecution("throughput-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1_000);

        long start = System.nanoTime();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, 20)
                    .mapToObj(i -> pool.submit(() -> {
                        while (!queue.poll("default", TaskKind.ACTIVITY, 10).isEmpty()) {
                            // drain
                        }
                    }))
                    .toList();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(fixtures.countByStatus("LEASED")).isEqualTo(1_000);
        assertThat(elapsed)
                .as("slowness here indicates a lock convoy rather than SKIP LOCKED")
                .isLessThan(Duration.ofSeconds(30));
    }

    // ---------------------------------------------------------------------------------
    // Visibility.
    // ---------------------------------------------------------------------------------

    /** A leased task is invisible to every other poller until its lease lapses. */
    @Test
    void leasedTaskIsInvisibleToOtherPollers() {
        UUID executionId = fixtures.newExecution("visibility-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10)).hasSize(1);
        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10))
                .as("already-leased task must not be re-dispatched")
                .isEmpty();
    }

    /** Future-dated tasks (timers, backoff) must not be dispatched early. */
    @Test
    void tasksAreInvisibleUntilNotBefore() {
        UUID executionId = fixtures.newExecution("timer-test");
        fixtures.enqueuePendingAt(executionId, "default", TaskKind.TIMER,
                Instant.now().plusSeconds(3));

        assertThat(queue.poll("default", TaskKind.TIMER, 10)).isEmpty();

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertThat(queue.poll("default", TaskKind.TIMER, 10)).hasSize(1));
    }

    /** Queues and kinds are isolated dispatch domains. */
    @Test
    void pollIsScopedToQueueAndKind() {
        UUID executionId = fixtures.newExecution("routing-test");
        fixtures.enqueuePending(executionId, "queue-a", TaskKind.ACTIVITY, 3);
        fixtures.enqueuePending(executionId, "queue-b", TaskKind.ACTIVITY, 3);

        assertThat(queue.poll("queue-a", TaskKind.ACTIVITY, 10)).hasSize(3);
        assertThat(queue.poll("queue-a", TaskKind.WORKFLOW, 10)).isEmpty();
        assertThat(queue.poll("queue-b", TaskKind.ACTIVITY, 10)).hasSize(3);
    }

    // ---------------------------------------------------------------------------------
    // Ownership.
    // ---------------------------------------------------------------------------------

    /**
     * The zombie-worker scenario, and the reason {@code lease_owner} is a per-lease token
     * rather than a stable worker identity.
     */
    @Test
    void staleLeaseOwnerCannotCompleteReassignedTask() {
        UUID executionId = fixtures.newExecution("zombie-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask original = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(50)).getFirst();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Instant.now().isAfter(original.leaseUntil()));
        reaper.reclaimBatch(10);

        // Reclaimed with backoff, so wait for it to become visible again.
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200))
                .until(() -> !queue.poll("default", TaskKind.ACTIVITY, 1,
                        Duration.ofMinutes(1)).isEmpty()
                        || fixtures.countByStatus("LEASED") == 1);

        assertThat(queue.complete(original.taskId(), original.leaseOwner()))
                .as("zombie worker must not be able to ack a task it no longer owns")
                .isFalse();
    }

    /** Heartbeat must keep a long-running task out of the reaper's reach. */
    @Test
    void heartbeatExtendsLeaseAndPreventsReclaim() {
        UUID executionId = fixtures.newExecution("heartbeat-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask task = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(500)).getFirst();
        assertThat(queue.heartbeat(task.taskId(), task.leaseOwner(), Duration.ofSeconds(60)))
                .isTrue();

        await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    assertThat(reaper.reclaimBatch(10)).isZero();
                    assertThat(fixtures.countByStatus("LEASED")).isEqualTo(1);
                });
    }

    /** Heartbeat on a lost lease must report failure so the worker can abort. */
    @Test
    void heartbeatFailsAfterLeaseReclaimed() {
        UUID executionId = fixtures.newExecution("heartbeat-lost-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask task = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(50)).getFirst();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Instant.now().isAfter(task.leaseUntil()));
        reaper.reclaimBatch(10);

        assertThat(queue.heartbeat(task.taskId(), task.leaseOwner(), Duration.ofSeconds(30)))
                .as("worker must learn its lease is gone and abort in-flight work")
                .isFalse();
    }

    /** Completing under a valid lease must succeed and close the task. */
    @Test
    void completeUnderValidLeaseSucceeds() {
        UUID executionId = fixtures.newExecution("complete-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask task = queue.poll("default", TaskKind.ACTIVITY, 1).getFirst();
        assertThat(queue.complete(task.taskId(), task.leaseOwner())).isTrue();
        assertThat(fixtures.statusOf(task.taskId())).isEqualTo("COMPLETED");
        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10)).isEmpty();
    }

    // ---------------------------------------------------------------------------------
    // Reaper.
    // ---------------------------------------------------------------------------------

    /** The crash-recovery path: expired lease returns to PENDING with attempt bumped. */
    @Test
    void reaperReclaimsExpiredLeaseAndIncrementsAttempt() {
        UUID executionId = fixtures.newExecution("reaper-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        LeasedTask task = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(100)).getFirst();
        assertThat(task.attempt()).isEqualTo(1);

        await().atMost(Duration.ofSeconds(5))
                .until(() -> Instant.now().isAfter(task.leaseUntil()));

        assertThat(reaper.reclaimBatch(100)).isEqualTo(1);
        assertThat(fixtures.attemptOf(task.taskId())).isEqualTo(2);
        assertThat(fixtures.statusOf(task.taskId())).isEqualTo("PENDING");
        assertThat(fixtures.leaseOwnerOf(task.taskId())).isNull();
    }

    /**
     * Backoff must actually delay redelivery, not just bump a counter.
     *
     * <p>This is what turns a poison task - one that OOM-kills every worker that touches
     * it - from a fleet-wide crash loop into a slowly-failing task that gets out of the
     * way.</p>
     */
    @Test
    void reclaimAppliesExponentialBackoff() {
        UUID executionId = fixtures.newExecution("backoff-test");
        fixtures.enqueuePendingWithAttempt(executionId, "default", TaskKind.ACTIVITY, 4);

        LeasedTask task = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(50)).getFirst();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Instant.now().isAfter(task.leaseUntil()));
        reaper.reclaimBatch(10);

        // attempt was 4 -> base(1s) * 2^3 = 8s, with +/-12.5% jitter -> [7s, 9s].
        Duration delay = Duration.between(Instant.now(), fixtures.notBeforeOf(task.taskId()));
        assertThat(delay).isBetween(Duration.ofSeconds(6), Duration.ofSeconds(10));
        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10))
                .as("backed-off task must not be immediately visible")
                .isEmpty();
    }

    /** Exhausted retries terminate rather than looping forever. */
    @Test
    void reclaimFailsTaskWhenAttemptsExhausted() {
        UUID executionId = fixtures.newExecution("exhaustion-test");
        // max_attempts is 5 in the fixture.
        fixtures.enqueuePendingWithAttempt(executionId, "default", TaskKind.ACTIVITY, 5);

        LeasedTask task = queue
                .poll("default", TaskKind.ACTIVITY, 1, Duration.ofMillis(50)).getFirst();
        await().atMost(Duration.ofSeconds(5))
                .until(() -> Instant.now().isAfter(task.leaseUntil()));
        reaper.reclaimBatch(10);

        assertThat(fixtures.statusOf(task.taskId())).isEqualTo("FAILED");
        assertThat(queue.poll("default", TaskKind.ACTIVITY, 10)).isEmpty();
    }

    /** Live leases must survive a sweep untouched. */
    @Test
    void reaperIgnoresLiveLeases() {
        UUID executionId = fixtures.newExecution("live-lease-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 5);
        queue.poll("default", TaskKind.ACTIVITY, 5, Duration.ofMinutes(5));

        assertThat(reaper.reclaimBatch(100)).isZero();
        assertThat(fixtures.countByStatus("LEASED")).isEqualTo(5);
    }

    /**
     * Multiple app instances reap concurrently in production. Concurrent reapers must
     * partition the expired set via SKIP LOCKED rather than double-reclaiming, which
     * would double-increment {@code attempt} and burn retries a task had not used.
     */
    @Test
    void concurrentReapersDoNotDoubleReclaim() throws Exception {
        UUID executionId = fixtures.newExecution("concurrent-reap-test");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 500);
        queue.poll("default", TaskKind.ACTIVITY, 500, Duration.ofMillis(100));

        await().atMost(Duration.ofSeconds(10)).until(fixtures::allLeasesExpired);

        var totalReclaimed = new AtomicInteger();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, 8)
                    .mapToObj(i -> pool.submit(() -> {
                        int n;
                        while ((n = reaper.reclaimBatch(50)) > 0) {
                            totalReclaimed.addAndGet(n);
                        }
                    }))
                    .toList();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        }

        assertThat(totalReclaimed.get())
                .as("each expired task reclaimed exactly once across all reapers")
                .isEqualTo(500);
        assertThat(fixtures.distinctAttempts())
                .as("no task may be double-incremented")
                .containsExactly(2);
    }

    // ---------------------------------------------------------------------------------
    // Planner.
    // ---------------------------------------------------------------------------------

    /**
     * Guards the partial index. The dispatch query must remain an index scan even when
     * the table is dominated by COMPLETED rows - this is the property that lets a single
     * table serve as a queue indefinitely without a separate archive.
     *
     * <p>Asserting on a query plan in a test is unusual, but this characteristic degrades
     * <em>silently</em>: nothing fails, no error is logged, throughput just quietly
     * collapses months later in production once the dead-row count crosses a threshold.
     * A test is the only place this gets caught early.</p>
     */
    @Test
    void dispatchQueryUsesPartialIndex() {
        UUID executionId = fixtures.newExecution("plan-test");
        fixtures.enqueueCompleted(executionId, "default", TaskKind.ACTIVITY, 50_000);
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 100);
        fixtures.analyze();

        String plan = fixtures.explainDispatch("default", TaskKind.ACTIVITY, 10);

        assertThat(plan)
                .as("dispatch must use the partial index: %s", plan)
                .contains("idx_wf_tasks_poll");
        assertThat(plan)
                .as("a sequential scan means the partial predicate stopped matching")
                .doesNotContain("Seq Scan on wf_tasks");
    }
}
