-- =====================================================================================
-- V4 - Timer sharding.
--
-- Sharding exists for exactly one reason: to let N poller replicas scan for due timers
-- without contending on the same index pages.
--
-- Without shards every replica issues the identical query - PENDING timers where
-- not_before <= now(), ordered by not_before - and they all converge on the same hot
-- leading edge of the index. SKIP LOCKED keeps that *correct*, but correctness was never
-- the problem: every replica still walks the same B-tree pages, and buffer contention on
-- those pages is what bites at millions of sleeping workflows. With shards, replica A
-- walks shards 0-3 and replica B walks 4-7, touching disjoint index ranges.
--
-- 16 shards is deliberate. It must exceed the expected replica count (so each replica
-- owns at least one) while staying small enough that a single replica can cover all of
-- them when scaled down to one. 16 handles 1-16 replicas cleanly and is a power of two.
-- =====================================================================================

ALTER TABLE wf_tasks
    ADD COLUMN shard SMALLINT;

-- Derived from execution_id, not random: every task for one execution lands on the same
-- shard. Timers for a single execution fire in order, and same-shard placement means one
-- poller handles them sequentially rather than two pollers racing to enqueue decision
-- tasks for the same workflow. The one-open-decision index would catch that race, but
-- not racing at all is cheaper than losing.
--
-- NOTE: this expression is used ONLY for the one-time backfill. New rows get their shard
-- computed in Java by ShardAssignment.shardFor(). See the class Javadoc for why.
UPDATE wf_tasks
   SET shard = abs(hashtext(execution_id::text)) % 16
 WHERE shard IS NULL;

ALTER TABLE wf_tasks
    ALTER COLUMN shard SET NOT NULL,
    ADD CONSTRAINT ck_wf_task_shard CHECK (shard >= 0 AND shard < 16);

-- The timer dispatch index. Here "fire_at" is not_before and "fired = false" is
-- status = 'PENDING' - a fired timer leaves PENDING and therefore leaves this index
-- entirely.
--
-- That last property is what makes this scale to millions of sleepers: index size tracks
-- the number of PENDING timers, not the number ever created. Ten million fired timers
-- cost nothing here.
CREATE INDEX idx_wf_tasks_timer_shard
    ON wf_tasks (shard, not_before, id)
    WHERE status = 'PENDING' AND kind = 'TIMER';

COMMENT ON COLUMN wf_tasks.shard IS
    'Poller shard in [0,16). Computed by ShardAssignment.shardFor(execution_id) in Java. '
    'Stable per execution for its whole lifetime.';
