package io.ironflow.worker;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.LeasedTask;
import io.ironflow.queue.PostgresTaskQueueRepository;
import io.ironflow.queue.notify.QueueSignal;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Polls for tasks and executes them on virtual threads.
 *
 * <p>Runs one dispatch loop per {@link TaskKind} it serves - decisions and activities have
 * different execution profiles, and a shared loop would let a backlog of one starve the
 * other.</p>
 *
 * <h2>Threading model</h2>
 *
 * <p>One platform-thread-backed dispatch loop per kind, and one virtual thread per in-flight
 * task. The dispatch loop is a tight database poll that benefits from a real carrier thread;
 * task execution is dominated by blocking I/O, which is exactly what virtual threads exist
 * for. Ten thousand concurrent tasks cost a few hundred KB of heap rather than ten thousand
 * OS threads.</p>
 *
 * <h2>Backpressure</h2>
 *
 * <p>A semaphore bounds in-flight tasks, and the loop acquires permits <em>before</em>
 * polling. This ordering is the whole point: leasing tasks you have no capacity to run is
 * actively harmful, because the lease clock starts at dispatch, not at execution. A worker
 * that claims 500 tasks and runs 10 at a time will watch the other 490 expire and be
 * reclaimed by the reaper - duplicating work it is still holding.</p>
 *
 * <h2>Notification-driven wakeup</h2>
 *
 * <p>Instead of polling on a timer, the loop blocks on {@link QueueSignal} with a long
 * safety-net timeout. An idle worker issues roughly one query per kind per safety-net
 * interval - with a 30s default, a 30x reduction over the previous 1s adaptive poll - and a
 * task enqueued anywhere wakes the right loop in single-digit milliseconds.</p>
 *
 * <p><b>The generation is captured before the lease attempt.</b> Capture it after and a
 * notification arriving during the lease is missed, costing a full safety-net interval -
 * far worse than the polling it replaced. See {@link QueueSignal} for the full argument.</p>
 */
@Service
public class WorkerPoller {

    private static final Logger log = LoggerFactory.getLogger(WorkerPoller.class);

    private static final long POLL_ERROR_BACKOFF_MS = 1_000;
    private static final long CAPACITY_WAIT_MS = 200;
    private static final long SHUTDOWN_GRACE_SECONDS = 30;

    private final PostgresTaskQueueRepository queue;
    private final DecisionTaskExecutor decisionExecutor;
    private final ActivityTaskExecutor activityExecutor;
    private final QueueSignal signal;

    private final String taskQueue;
    private final int maxConcurrency;
    private final Duration lease;
    private final Duration safetyNetInterval;
    private final boolean enabled;

    private final AtomicBoolean running = new AtomicBoolean();
    private final Semaphore capacity;
    private final ExecutorService taskThreads = Executors.newVirtualThreadPerTaskExecutor();
    private volatile Thread decisionLoop;
    private volatile Thread activityLoop;

    public WorkerPoller(PostgresTaskQueueRepository queue,
                        DecisionTaskExecutor decisionExecutor,
                        ActivityTaskExecutor activityExecutor,
                        QueueSignal signal,
                        @Value("${ironflow.worker.task-queue:default}") String taskQueue,
                        @Value("${ironflow.worker.max-concurrency:64}") int maxConcurrency,
                        @Value("${ironflow.worker.lease-seconds:30}") long leaseSeconds,
                        @Value("${ironflow.worker.safety-net-interval:30s}")
                        Duration safetyNetInterval,
                        @Value("${ironflow.worker.enabled:true}") boolean enabled) {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("max-concurrency must be >= 1");
        }
        this.queue = queue;
        this.decisionExecutor = decisionExecutor;
        this.activityExecutor = activityExecutor;
        this.signal = signal;
        this.taskQueue = taskQueue;
        this.maxConcurrency = maxConcurrency;
        this.lease = Duration.ofSeconds(leaseSeconds);
        this.safetyNetInterval = safetyNetInterval;
        this.enabled = enabled;
        this.capacity = new Semaphore(maxConcurrency);
    }

    /**
     * Starts the dispatch loops once the application context is fully ready.
     *
     * <p>Deliberately not {@code @PostConstruct}: starting during bean construction can
     * lease tasks before the rest of the context exists, producing confusing failures where
     * a task executes against half-initialised beans.</p>
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!enabled) {
            log.info("WorkerPoller disabled by configuration");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }

        decisionLoop = Thread.ofPlatform()
                .name("ironflow-dispatch-decision-" + taskQueue)
                .daemon(false)
                .start(() -> loop(TaskKind.WORKFLOW, decisionExecutor::execute));

        activityLoop = Thread.ofPlatform()
                .name("ironflow-dispatch-activity-" + taskQueue)
                .daemon(false)
                .start(() -> loop(TaskKind.ACTIVITY, activityExecutor::execute));

        log.info("WorkerPoller started on queue '{}' (concurrency={}, lease={}s, "
                        + "safetyNet={}s)",
                taskQueue, maxConcurrency, lease.toSeconds(), safetyNetInterval.toSeconds());
    }

    private void loop(TaskKind kind, Consumer<LeasedTask> handler) {
        String queueKey = QueueSignal.key(taskQueue, kind);

        while (running.get()) {
            try {
                int permits = acquireCapacity();
                if (permits == 0) {
                    continue;
                }

                // BEFORE the lease. See the class Javadoc.
                long generation = signal.currentGeneration(queueKey);

                List<LeasedTask> batch;
                try {
                    batch = queue.poll(taskQueue, kind, permits, lease);
                } catch (Exception e) {
                    capacity.release(permits);
                    log.error("Poll failed on queue '{}' kind {}; backing off",
                            taskQueue, kind, e);
                    Thread.sleep(POLL_ERROR_BACKOFF_MS);
                    continue;
                }

                capacity.release(permits - batch.size());

                if (batch.isEmpty()) {
                    // The safety-net timeout is what makes a lost notification harmless: it
                    // costs added latency, never a stranded task. This is the property that
                    // permits using a best-effort channel at all.
                    signal.await(queueKey, generation, safetyNetInterval);
                    continue;
                }

                for (LeasedTask task : batch) {
                    taskThreads.submit(() -> {
                        try {
                            handler.accept(task);
                        } finally {
                            // Must be in a finally: a leaked permit permanently reduces
                            // this worker's capacity, and enough leaks stall it entirely
                            // with no error to show for it.
                            capacity.release();
                        }
                    });
                }

                // Drained a full batch: more may be waiting. Loop immediately rather than
                // waiting for another notification, which may never come - the enqueues
                // that produced this backlog already fired their notifications.

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in {} dispatch loop", kind, e);
            }
        }
        log.info("Dispatch loop for queue '{}' kind {} exited", taskQueue, kind);
    }

    /**
     * Claims all currently free capacity, or waits briefly for a single permit.
     *
     * @return number of permits held, or {@code 0} if none became available
     */
    private int acquireCapacity() throws InterruptedException {
        int permits = capacity.drainPermits();
        if (permits > 0) {
            return permits;
        }
        return capacity.tryAcquire(CAPACITY_WAIT_MS, TimeUnit.MILLISECONDS) ? 1 : 0;
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        for (Thread t : new Thread[] { decisionLoop, activityLoop }) {
            if (t != null) {
                t.interrupt();
            }
        }
        taskThreads.shutdown();
        if (!taskThreads.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
            log.warn("In-flight tasks did not finish in {}s; their leases will expire and "
                    + "they will be retried", SHUTDOWN_GRACE_SECONDS);
            taskThreads.shutdownNow();
        }
    }

    /** @return currently executing task count; used by tests and health checks. */
    public int inFlight() {
        return maxConcurrency - capacity.availablePermits();
    }

    public boolean isRunning() {
        return running.get();
    }
}
