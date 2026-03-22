package io.ironflow.timer;

import io.ironflow.queue.ShardAssignment;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Fires due timers, scanning a subset of shards on virtual threads.
 *
 * <h2>The sharding argument</h2>
 *
 * <p>The naive design has every replica poll every timer. With {@code SKIP LOCKED} this is
 * correct - no timer fires twice - but correctness was never the concern. Every replica
 * walks the same hot leading edge of the same B-tree, and at millions of pending timers the
 * buffer contention on those pages dominates.</p>
 *
 * <p>Sharding gives each replica a disjoint slice of the index. Replica A scans shards 0-3,
 * replica B scans 4-7; they touch different pages and never contend.</p>
 *
 * <h2>Shard assignment without coordination</h2>
 *
 * <p>Assignment is computed locally from {@code (replicaIndex, replicaCount)} - no election,
 * no coordination service, no lease on a shard. Two consequences, both deliberate:</p>
 *
 * <ul>
 *   <li>If configuration is wrong and two replicas claim the same shard, nothing breaks.
 *       {@code SKIP LOCKED} makes overlap merely wasteful. The system degrades to the naive
 *       design rather than firing timers twice.</li>
 *   <li>If a shard is claimed by nobody, its timers never fire. That failure <em>is</em>
 *       silent, so {@link #assertShardCoverage} logs loudly at startup.</li>
 * </ul>
 *
 * <p>The asymmetry is worth internalising: <b>over-coverage is free, under-coverage is a
 * silent outage.</b> Configure conservatively.</p>
 *
 * <h2>One virtual thread per shard</h2>
 *
 * <p>Each owned shard gets its own poll loop. A shard with a large due backlog therefore
 * cannot starve a shard with none - which a single loop iterating shards sequentially
 * would do.</p>
 */
@Service
public class ShardedTimerPoller {

    private static final Logger log = LoggerFactory.getLogger(ShardedTimerPoller.class);

    private final TimerFiringRepository timers;
    private final TimerPollerProperties props;

    private final AtomicBoolean running = new AtomicBoolean();
    private final ExecutorService shardThreads = Executors.newVirtualThreadPerTaskExecutor();
    private volatile List<Integer> ownedShards = List.of();

    public ShardedTimerPoller(TimerFiringRepository timers, TimerPollerProperties props) {
        this.timers = timers;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!props.isEnabled()) {
            log.info("Timer poller disabled by configuration");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }

        ownedShards = computeOwnedShards(props.getReplicaIndex(), props.getReplicaCount());
        assertShardCoverage();

        log.info("Timer poller starting: replica {}/{} owns shards {}",
                props.getReplicaIndex(), props.getReplicaCount(), ownedShards);

        for (int shard : ownedShards) {
            shardThreads.submit(() -> pollShard(shard));
        }
    }

    /**
     * Assigns shards round-robin by replica index.
     *
     * <p>Round-robin rather than contiguous ranges: if load is uneven across the shard space
     * - which it will be, since hash distribution is only approximately uniform -
     * round-robin spreads the hot shards across replicas rather than concentrating them.</p>
     */
    static List<Integer> computeOwnedShards(int replicaIndex, int replicaCount) {
        if (replicaCount < 1) {
            throw new IllegalArgumentException("replicaCount must be >= 1");
        }
        if (replicaIndex < 0 || replicaIndex >= replicaCount) {
            throw new IllegalArgumentException(
                    "replicaIndex %d out of range for replicaCount %d"
                            .formatted(replicaIndex, replicaCount));
        }
        return IntStream.range(0, ShardAssignment.SHARD_COUNT)
                .filter(shard -> shard % replicaCount == replicaIndex)
                .boxed()
                .toList();
    }

    /**
     * Warns when the configured fleet cannot cover every shard.
     *
     * <p>{@code replicaCount > SHARD_COUNT} means some replicas own nothing - wasteful but
     * harmless. The dangerous case is a replica that dies while its shards are unclaimed;
     * that is an operational concern outside this class, but worth flagging here because it
     * is the one failure mode with no error signal.</p>
     */
    private void assertShardCoverage() {
        if (props.getReplicaCount() > ShardAssignment.SHARD_COUNT) {
            log.warn("replicaCount ({}) exceeds shard count ({}); {} replica(s) will own no "
                            + "shards and idle. Reduce replicaCount or raise SHARD_COUNT.",
                    props.getReplicaCount(), ShardAssignment.SHARD_COUNT,
                    props.getReplicaCount() - ShardAssignment.SHARD_COUNT);
        }
        if (ownedShards.isEmpty()) {
            log.warn("This replica owns no timer shards and will not fire any timers.");
        }
    }

    /**
     * Poll loop for one shard.
     *
     * <p>Adaptive backoff: tight when timers are firing, relaxed when idle. The floor bounds
     * firing latency; the ceiling bounds idle database load. With the defaults, an idle
     * shard costs one query per second and a due timer fires within a second of its
     * deadline.</p>
     *
     * <p>The latency floor is inherent to polling. {@code LISTEN/NOTIFY} removes it for
     * immediately-visible tasks but not for timers, whose deadlines are in the future and
     * therefore have nothing to notify on. Sub-second timer precision would need a different
     * mechanism; for sleeps measured in hours or days - the actual use case - a one-second
     * floor is irrelevant.</p>
     */
    private void pollShard(int shard) {
        Thread.currentThread().setName("ironflow-timer-shard-" + shard);
        long idleMillis = props.getMinPollInterval().toMillis();

        while (running.get()) {
            try {
                int fired = timers.fireDueTimers(shard, props.getBatchSize());

                if (fired > 0) {
                    idleMillis = props.getMinPollInterval().toMillis();
                    if (fired == props.getBatchSize()) {
                        // Full batch: more may be due. Poll again immediately.
                        continue;
                    }
                } else {
                    idleMillis = Math.min(idleMillis * 2,
                            props.getMaxPollInterval().toMillis());
                }
                Thread.sleep(idleMillis);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // A database blip must not kill the shard's loop permanently: the shard
                // would go dark and its timers would never fire, with only this one log
                // line as evidence.
                log.error("Timer poll failed on shard {}; backing off", shard, e);
                try {
                    Thread.sleep(props.getErrorBackoff().toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.info("Timer poll loop for shard {} exited", shard);
    }

    @PreDestroy
    public void stop() throws InterruptedException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        shardThreads.shutdownNow();
        if (!shardThreads.awaitTermination(10, TimeUnit.SECONDS)) {
            log.warn("Timer shard threads did not stop within 10s");
        }
    }

    public List<Integer> ownedShards() {
        return ownedShards;
    }
}
