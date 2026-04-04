package io.ironflow.timer;

import io.ironflow.queue.ShardAssignment;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for shard assignment. */
class ShardAssignmentTest {

    /**
     * Every shard must be owned by exactly one replica, for every fleet size.
     *
     * <p>A gap here is a silent outage: the unowned shard's timers never fire, with no error
     * anywhere. An overlap is merely wasteful. Both are worth asserting.</p>
     */
    @Test
    void everyShardIsCoveredExactlyOnceForAllFleetSizes() {
        for (int replicaCount = 1; replicaCount <= ShardAssignment.SHARD_COUNT;
             replicaCount++) {
            var covered = new TreeSet<Integer>();
            for (int idx = 0; idx < replicaCount; idx++) {
                var owned = ShardedTimerPoller.computeOwnedShards(idx, replicaCount);
                assertThat(covered)
                        .as("replicaCount=%d idx=%d must not overlap", replicaCount, idx)
                        .doesNotContainAnyElementsOf(owned);
                covered.addAll(owned);
            }
            assertThat(covered)
                    .as("replicaCount=%d must cover all %d shards",
                            replicaCount, ShardAssignment.SHARD_COUNT)
                    .hasSize(ShardAssignment.SHARD_COUNT);
        }
    }

    /** A shard must be stable for an execution's whole lifetime. */
    @Test
    void shardIsStablePerExecution() {
        UUID id = UUID.randomUUID();
        short first = ShardAssignment.shardFor(id);
        for (int i = 0; i < 100; i++) {
            assertThat(ShardAssignment.shardFor(id)).isEqualTo(first);
        }
    }

    @Test
    void shardIsAlwaysInRange() {
        for (int i = 0; i < 10_000; i++) {
            short shard = ShardAssignment.shardFor(UUID.randomUUID());
            assertThat(shard).isBetween((short) 0,
                    (short) (ShardAssignment.SHARD_COUNT - 1));
        }
    }

    /** Distribution must be roughly even, or one poller carries the fleet. */
    @Test
    void shardDistributionIsApproximatelyUniform() {
        Map<Short, Integer> counts = new HashMap<>();
        int samples = 160_000;
        for (int i = 0; i < samples; i++) {
            counts.merge(ShardAssignment.shardFor(UUID.randomUUID()), 1, Integer::sum);
        }
        int expected = samples / ShardAssignment.SHARD_COUNT;
        assertThat(counts).hasSize(ShardAssignment.SHARD_COUNT);
        counts.values().forEach(c -> assertThat(c)
                .as("expected ~%d per shard", expected)
                .isBetween((int) (expected * 0.9), (int) (expected * 1.1)));
    }

    @Test
    void invalidReplicaIndexIsRejected() {
        assertThatThrownBy(() -> ShardedTimerPoller.computeOwnedShards(5, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShardedTimerPoller.computeOwnedShards(-1, 3))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ShardedTimerPoller.computeOwnedShards(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** More replicas than shards: some own nothing. Wasteful but not an error. */
    @Test
    void excessReplicasOwnNothing() {
        var owned = ShardedTimerPoller.computeOwnedShards(
                ShardAssignment.SHARD_COUNT + 3, ShardAssignment.SHARD_COUNT + 5);
        assertThat(owned).isEmpty();
    }
}
