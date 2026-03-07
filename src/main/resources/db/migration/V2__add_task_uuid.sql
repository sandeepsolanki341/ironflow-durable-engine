-- =====================================================================================
-- V2 - Stable UUID handles for tasks.
--
-- The orchestrator API exchanges task identifiers with callers that may retain them
-- across restarts and hand them back later. A BIGINT identity is fine internally but
-- leaks sequence cardinality (an observer learns total task throughput from two samples)
-- and makes id reuse across environments easy to do by accident.
--
-- Both identifiers are kept: id remains the physical key and the target of every index,
-- while task_uuid is the external handle. Replacing id outright would bloat every index
-- in the hottest table in the system for no operational benefit.
-- =====================================================================================

ALTER TABLE wf_tasks
    ADD COLUMN task_uuid UUID NOT NULL DEFAULT gen_random_uuid();

CREATE UNIQUE INDEX uq_wf_tasks_uuid ON wf_tasks (task_uuid);

COMMENT ON COLUMN wf_tasks.task_uuid IS
    'External task handle. Used by the orchestrator API; id remains the internal key.';
