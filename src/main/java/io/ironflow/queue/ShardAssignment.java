package io.ironflow.queue;

import java.util.UUID;

/**
 * Maps an execution to its poller shard.
 *
 * <p>The shard is computed in Java and written explicitly on insert. The SQL expression in
 * {@code V4__timer_sharding.sql} is used <em>only</em> for the one-time backfill of
 * pre-existing rows.</p>
 *
 * <p>That split is deliberate. If new rows relied on a SQL default while the poller queried
 * by a Java-computed shard, any mismatch between the two functions would be silent and
 * nasty: a subset of timers would be polled by nobody, and those workflows would sleep
 * forever with no error anywhere. Computing it in one place removes the possibility.</p>
 */
public final class ShardAssignment {

    /**
     * Number of poller shards.
     *
     * <p>Schema-level constant: changing it means recomputing every existing row's shard,
     * since the modulo changes. 16 handles 1-16 poller replicas cleanly and is a power of
     * two. If you expect to exceed 16 replicas, raise it now rather than later.</p>
     */
    public static final int SHARD_COUNT = 16;

    private ShardAssignment() { }

    /**
     * @return a stable shard in {@code [0, SHARD_COUNT)} for this execution
     */
    public static short shardFor(UUID executionId) {
        // Fold both halves so shard selection depends on the whole UUID. Using only the
        // low bits would cluster badly with time-ordered UUID schemes.
        long mixed = executionId.getMostSignificantBits()
                ^ executionId.getLeastSignificantBits();
        return (short) Math.floorMod(mixed, SHARD_COUNT);
    }
}
