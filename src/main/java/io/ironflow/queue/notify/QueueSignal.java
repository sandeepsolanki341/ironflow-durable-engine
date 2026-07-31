package io.ironflow.queue.notify;

import io.ironflow.persistence.model.TaskKind;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wakeup coordination between the notification listener and the pollers.
 *
 * <h2>The lost-wakeup problem</h2>
 *
 * <p>The obvious implementation - a {@code CountDownLatch} per queue that the poller awaits
 * and the listener counts down - has a race that costs a full safety-net interval of latency
 * every time it fires:</p>
 *
 * <pre>
 *   Poller:   lease() -&gt; empty
 *   Listener:                    notification arrives, wake() called
 *   Poller:   await()  -&gt; sleeps, having missed the wakeup entirely
 * </pre>
 *
 * <p>The notification landed in the window between the poller's failed lease and its
 * decision to sleep. The task is enqueued and visible, and the poller is asleep.</p>
 *
 * <p>The fix is a monotonic generation counter rather than an edge-triggered latch. The
 * poller records the generation <em>before</em> it leases; if the generation has advanced by
 * the time it goes to sleep, a wakeup was missed and it re-polls immediately instead.
 * Level-triggered rather than edge-triggered.</p>
 *
 * <h2>Why not a BlockingQueue or Condition</h2>
 *
 * <p>Both are edge-triggered and have the same race unless wrapped in exactly this counter.
 * Doing it explicitly makes the invariant visible rather than hiding it inside a lock
 * protocol a future reader has to reconstruct.</p>
 */
@Component
public class QueueSignal {

    /**
     * Per-queue generation counters. A queue key is {@code "queueName:KIND"}, matching the
     * notification payload so routing needs no parsing beyond the key itself.
     */
    private final Map<String, Waiter> waiters = new ConcurrentHashMap<>();

    /**
     * Broadcast generation, incremented by {@link #wakeAll}. Checked alongside the per-queue
     * counter so a reconnect wakes pollers on queues that have never had a notification.
     */
    private final AtomicLong globalGeneration = new AtomicLong();

    public static String key(String taskQueue, TaskKind kind) {
        return taskQueue + ":" + kind.name();
    }

    /**
     * Reads the current generation for a queue.
     *
     * <p>Must be called <b>before</b> the lease attempt. That ordering is the entire
     * lost-wakeup fix: capturing it after would reintroduce the race it exists to close.</p>
     */
    public long currentGeneration(String queueKey) {
        return waiterFor(queueKey).generation.get() + globalGeneration.get();
    }

    /**
     * Blocks until the generation advances past {@code seenGeneration}, or the timeout
     * elapses.
     *
     * @param seenGeneration the value from {@link #currentGeneration} taken before the
     *                       poller's failed lease attempt
     * @param timeout        the safety-net interval; the caller re-polls on expiry
     *                       regardless, which is what makes lost notifications harmless
     * @return {@code true} if woken by a signal rather than the timeout
     */
    public boolean await(String queueKey, long seenGeneration, Duration timeout)
            throws InterruptedException {

        Waiter waiter = waiterFor(queueKey);
        long deadline = System.nanoTime() + timeout.toNanos();

        synchronized (waiter) {
            while (waiter.generation.get() + globalGeneration.get() <= seenGeneration) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                // Guarded by the same monitor the notifier uses, so a signal cannot slip
                // between the predicate check and the wait.
                waiter.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
            return true;
        }
    }

    /** Wakes pollers on one queue. Called from the listener thread. */
    public void wake(String queueKey) {
        Waiter waiter = waiterFor(queueKey);
        synchronized (waiter) {
            waiter.generation.incrementAndGet();
            waiter.notifyAll();
        }
    }

    /**
     * Wakes every poller.
     *
     * <p>Used on listener reconnect, where notifications published during the outage are
     * unrecoverable and the only safe assumption is that work is waiting.</p>
     */
    public void wakeAll() {
        globalGeneration.incrementAndGet();
        for (Waiter waiter : waiters.values()) {
            synchronized (waiter) {
                waiter.notifyAll();
            }
        }
    }

    private Waiter waiterFor(String queueKey) {
        return waiters.computeIfAbsent(queueKey, k -> new Waiter());
    }

    private static final class Waiter {
        final AtomicLong generation = new AtomicLong();
    }
}
