-- =====================================================================================
-- V3 - Replay divergence quarantine.
--
-- DIVERGENT is deliberately NON-TERMINAL.
--
-- Divergence means the deployed code stopped matching recorded history - a workflow was
-- edited while instances of it were mid-flight. The execution itself is undamaged: its
-- history is valid and its state fully reconstructible. What is broken is the binary that
-- tried to replay it.
--
-- Marking it terminal would make a bad deploy permanently destroy every in-flight
-- instance of the affected workflow type, recoverable only by hand-editing history. So
-- instead we quarantine: halt progress, stop burning retries, surface it loudly, and
-- allow resumption once the code is rolled back or a compatible patch ships.
-- =====================================================================================

ALTER TABLE wf_executions
    DROP CONSTRAINT ck_wf_exec_status;

ALTER TABLE wf_executions
    ADD CONSTRAINT ck_wf_exec_status CHECK (
        status IN ('RUNNING','COMPLETED','FAILED','CANCELLED','TIMED_OUT','DIVERGENT'));

-- DIVERGENT is non-terminal, so it must NOT carry an end_time. This reuses the existing
-- invariant: end_time is set exactly when the execution can no longer progress.
ALTER TABLE wf_executions
    DROP CONSTRAINT ck_wf_exec_end_time;

ALTER TABLE wf_executions
    ADD CONSTRAINT ck_wf_exec_end_time CHECK (
        (status IN ('RUNNING','DIVERGENT')) = (end_time IS NULL));

-- Forensics. Populated on quarantine, cleared on resume. Without the expected/actual pair
-- an operator has to reconstruct the mismatch from logs that may have rotated.
ALTER TABLE wf_executions
    ADD COLUMN divergence_detail      TEXT,
    ADD COLUMN divergence_detected_at TIMESTAMPTZ,
    ADD COLUMN divergence_count       INT NOT NULL DEFAULT 0;

-- Operator dashboard: "what is quarantined right now, and since when".
CREATE INDEX idx_wf_exec_divergent
    ON wf_executions (divergence_detected_at DESC)
    WHERE status = 'DIVERGENT';
