-- =============================================================================================
-- IronFlow chaos-test invariant verification.
--
-- Run against the PostgreSQL instance AFTER the load generator has finished and all workflows
-- have settled. Proves the three correctness properties the whole suite exists to demonstrate:
--
--   1. ZERO LOST STATE TRANSITIONS
--        Per execution, sequence_number is gap-free from 1..max, and the execution's
--        next_sequence high-water equals max+1. A gap means a sequence was reserved but its
--        event never committed - a transition the engine intended and lost. Under kill -9 mid
--        commit this is exactly what a broken engine would produce.
--
--   2. ZERO DUPLICATED ACTIVITY SIDE EFFECTS
--        Each scheduled activity task (wf_tasks.task_uuid) yields AT MOST ONE ACTIVITY_COMPLETED.
--        A duplicate means the same activity's side effect was committed twice - the classic
--        at-least-once-becomes-at-least-twice failure when a worker dies after the side effect
--        but before the ack, and recovery double-runs it. The version-fence CAS is designed to
--        make this impossible; this query VERIFIES the fence actually held under chaos.
--
--   3. 100% COMPLETION OR CLEAN SAGA ROLLBACK
--        After settling, no execution remains in a live state (RUNNING, COMPENSATING, DIVERGENT).
--        Every workflow either COMPLETED, or FAILED_COMPENSATED (its saga cleanly rolled back),
--        or reached another terminal state. A stuck live execution is a workflow the chaos
--        stranded - the failure mode durable execution is supposed to make impossible.
--
-- Output contract: the final SELECT returns exactly one row with a boolean per invariant plus
-- offending counts, and a single overall `chaos_passing` boolean. The CI job reads this to build
-- the badge JSON. Any invariant false => the run fails => the badge goes red.
-- =============================================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------------------------
-- Invariant 1a: sequence gaps. For each execution, the count of events must equal the max
-- sequence number (gap-free from 1). If count < max, at least one sequence is missing.
-- ---------------------------------------------------------------------------------------------
WITH seq_stats AS (
    SELECT
        e.id AS execution_id,
        e.next_sequence,
        count(ev.sequence_number) AS event_count,
        coalesce(max(ev.sequence_number), 0) AS max_seq,
        coalesce(min(ev.sequence_number), 1) AS min_seq
    FROM wf_executions e
    LEFT JOIN wf_events ev ON ev.execution_id = e.id
    GROUP BY e.id, e.next_sequence
),
lost_transitions AS (
    SELECT execution_id, next_sequence, event_count, max_seq, min_seq
    FROM seq_stats
    WHERE
        -- A gap: fewer events than the max sequence implies (history is meant to be 1..max
        -- contiguous), or history does not start at 1.
        (event_count <> max_seq AND max_seq > 0)
        OR (min_seq <> 1 AND event_count > 0)
        -- High-water must sit exactly one past the last committed event. A next_sequence ahead
        -- of max+1 means sequences were reserved for events that never landed.
        OR (next_sequence <> max_seq + 1 AND event_count > 0)
),

-- ---------------------------------------------------------------------------------------------
-- Invariant 2: duplicated activity completions. Correlate each ACTIVITY_COMPLETED to the task
-- it completed via the taskId embedded in the payload, and assert no task_uuid appears twice.
-- ---------------------------------------------------------------------------------------------
completion_task_ids AS (
    SELECT
        execution_id,
        (payload ->> 'taskId') AS task_id,
        count(*) AS completion_count
    FROM wf_events
    WHERE event_type = 'ACTIVITY_COMPLETED'
      AND payload ? 'taskId'
    GROUP BY execution_id, (payload ->> 'taskId')
),
duplicated_side_effects AS (
    SELECT execution_id, task_id, completion_count
    FROM completion_task_ids
    WHERE completion_count > 1
),

-- Invariant 2b: duplicated compensation completions. A saga must not run the same compensation
-- twice - that would double-undo (e.g. refund a card twice). Keyed by registrationSeq.
duplicated_compensations AS (
    SELECT
        execution_id,
        (payload ->> 'registrationSeq') AS registration_seq,
        count(*) AS n
    FROM wf_events
    WHERE event_type = 'COMPENSATION_COMPLETED'
      AND payload ? 'registrationSeq'
    GROUP BY execution_id, (payload ->> 'registrationSeq')
    HAVING count(*) > 1
),

-- ---------------------------------------------------------------------------------------------
-- Invariant 3: no execution stranded in a live state after settling.
-- ---------------------------------------------------------------------------------------------
stranded_executions AS (
    SELECT id, status
    FROM wf_executions
    WHERE status IN ('RUNNING', 'COMPENSATING', 'DIVERGENT')
),

-- ---------------------------------------------------------------------------------------------
-- Invariant 3b: a FAILED_COMPENSATED execution must have actually run its compensations - every
-- COMPENSATION_REGISTERED before the failure should have a matching COMPENSATION_COMPLETED.
-- A "clean rollback" that skipped a compensation is not clean.
-- ---------------------------------------------------------------------------------------------
incomplete_rollbacks AS (
    SELECT
        e.id AS execution_id,
        count(*) FILTER (WHERE ev.event_type = 'COMPENSATION_REGISTERED') AS registered,
        count(*) FILTER (WHERE ev.event_type = 'COMPENSATION_COMPLETED') AS completed
    FROM wf_executions e
    JOIN wf_events ev ON ev.execution_id = e.id
    WHERE e.status = 'FAILED_COMPENSATED'
    GROUP BY e.id
    HAVING count(*) FILTER (WHERE ev.event_type = 'COMPENSATION_REGISTERED')
         <> count(*) FILTER (WHERE ev.event_type = 'COMPENSATION_COMPLETED')
)

-- ---------------------------------------------------------------------------------------------
-- Verdict row. One row, consumed by the CI job.
-- ---------------------------------------------------------------------------------------------
SELECT
    (SELECT count(*) FROM wf_executions)                    AS total_executions,
    (SELECT count(*) FROM lost_transitions)                 AS lost_transition_execs,
    (SELECT count(*) FROM duplicated_side_effects)          AS duplicated_side_effects,
    (SELECT count(*) FROM duplicated_compensations)         AS duplicated_compensations,
    (SELECT count(*) FROM stranded_executions)              AS stranded_executions,
    (SELECT count(*) FROM incomplete_rollbacks)             AS incomplete_rollbacks,
    (SELECT count(*) FROM wf_executions WHERE status = 'COMPLETED')           AS completed,
    (SELECT count(*) FROM wf_executions WHERE status = 'FAILED_COMPENSATED')  AS rolled_back,
    -- The single source of truth for the badge. AND of every invariant.
    (
        (SELECT count(*) FROM lost_transitions) = 0
        AND (SELECT count(*) FROM duplicated_side_effects) = 0
        AND (SELECT count(*) FROM duplicated_compensations) = 0
        AND (SELECT count(*) FROM stranded_executions) = 0
        AND (SELECT count(*) FROM incomplete_rollbacks) = 0
    )                                                        AS chaos_passing
;
