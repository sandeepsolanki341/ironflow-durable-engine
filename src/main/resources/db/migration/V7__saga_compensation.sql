-- =====================================================================================
-- V7 - Saga compensation states.
--
-- Two new statuses, and the asymmetry between them is the whole point:
--
--   COMPENSATING       - NON-terminal. The forward workflow failed, but rollback is now
--                        in progress. The execution is still doing work (running
--                        compensation activities), so it must stay live: no end_time, and
--                        its tasks keep flowing through the dispatch machinery.
--
--   FAILED_COMPENSATED - terminal. Rollback finished. Distinct from plain FAILED so an
--                        operator can tell "failed and cleaned up after itself" from
--                        "failed and left side effects behind". That distinction is the
--                        entire operational value of a saga: FAILED means someone has to
--                        go check what got half-done; FAILED_COMPENSATED means the system
--                        already undid it.
--
-- COMPENSATING is modelled exactly like RUNNING and DIVERGENT: a live state that carries
-- no end_time. Treating it as terminal would be a subtle disaster - the execution would
-- be marked done the instant the forward path failed, and the compensation activities
-- would have no live execution to attach their events to.
-- =====================================================================================

ALTER TABLE wf_executions
    DROP CONSTRAINT ck_wf_exec_status;

ALTER TABLE wf_executions
    ADD CONSTRAINT ck_wf_exec_status CHECK (
        status IN ('RUNNING','COMPLETED','FAILED','CANCELLED','TIMED_OUT','DIVERGENT',
                   'COMPENSATING','FAILED_COMPENSATED'));

-- COMPENSATING joins RUNNING and DIVERGENT as a live state with no end_time.
-- FAILED_COMPENSATED is terminal and therefore DOES carry one.
ALTER TABLE wf_executions
    DROP CONSTRAINT ck_wf_exec_end_time;

ALTER TABLE wf_executions
    ADD CONSTRAINT ck_wf_exec_end_time CHECK (
        (status IN ('RUNNING','DIVERGENT','COMPENSATING')) = (end_time IS NULL));

COMMENT ON CONSTRAINT ck_wf_exec_status ON wf_executions IS
    'COMPENSATING is non-terminal (rollback in progress); FAILED_COMPENSATED is terminal '
    '(rollback done). The pair lets operators distinguish a clean saga rollback from a '
    'raw failure that may have left side effects behind.';
