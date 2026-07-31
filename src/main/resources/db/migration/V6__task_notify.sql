-- =====================================================================================
-- V6 - LISTEN/NOTIFY task dispatch.
--
-- Emits a notification whenever a task becomes dispatchable, so idle pollers can block on
-- a socket rather than polling the database.
--
-- WHY A TRIGGER RATHER THAN pg_notify() IN THE INSERT STATEMENTS
--
-- Every enqueue path would otherwise have to remember to call it: WorkflowService.start,
-- DecisionCommitter, ActivityFailureCommitter, TimerFiringRepository, SignalService, and
-- the reaper's reclaim. Six places today, more later. One missed call is a task that sits
-- until the safety-net poll finds it - a latency bug that appears on only one code path
-- and is nearly impossible to spot in review.
--
-- A trigger cannot be forgotten.
--
-- TRANSACTIONAL DELIVERY
--
-- PostgreSQL queues notifications until COMMIT. A listener therefore can never be woken
-- for a row that is not yet visible - the classic race in broker-based designs, where the
-- wakeup outruns the write. This is precisely why a database-native pub/sub is safe where
-- an external one is not.
-- =====================================================================================

CREATE OR REPLACE FUNCTION wf_tasks_notify() RETURNS TRIGGER AS $$
BEGIN
    -- Only notify for tasks dispatchable NOW. A timer scheduled for next Tuesday must not
    -- wake a poller today - it would wake, find nothing due, and sleep again, which is
    -- pure waste at the scale of millions of pending timers.
    IF NEW.status = 'PENDING' AND NEW.not_before <= now() THEN
        -- Payload is a ROUTING HINT ONLY. The listener re-queries unconditionally and
        -- never trusts this content for correctness. Kept small because the payload limit
        -- is 8000 bytes and the notification queue is a fixed 8GB shared cluster-wide.
        PERFORM pg_notify('task_queue_channel', NEW.task_queue || ':' || NEW.kind);
    END IF;
    RETURN NULL;   -- AFTER trigger; return value is ignored
END;
$$ LANGUAGE plpgsql;

-- AFTER INSERT: notifications queue until COMMIT regardless, but firing AFTER keeps the
-- trigger out of the insert's critical path.
CREATE TRIGGER trg_wf_tasks_notify_insert
    AFTER INSERT ON wf_tasks
    FOR EACH ROW EXECUTE FUNCTION wf_tasks_notify();

-- Retries and reaper reclaims transition an existing row back to PENDING. Without this
-- trigger those tasks would wait for the safety-net poll, so every retry would carry an
-- extra interval of latency.
--
-- The WHEN clause matters: wf_tasks rows are updated on every lease, heartbeat and ack.
-- Firing on all of them would notify on transitions INTO leased/completed states, waking
-- pollers to find nothing - noise proportional to throughput, which is the opposite of
-- what this feature is for.
CREATE TRIGGER trg_wf_tasks_notify_update
    AFTER UPDATE ON wf_tasks
    FOR EACH ROW
    WHEN (NEW.status = 'PENDING' AND OLD.status IS DISTINCT FROM 'PENDING')
    EXECUTE FUNCTION wf_tasks_notify();

COMMENT ON FUNCTION wf_tasks_notify() IS
    'Wakes idle pollers on task enqueue. Notifications are delivered on COMMIT only, so a '
    'listener can never be woken for a row it cannot yet see. Delivery is best-effort: '
    'the poller safety-net interval is what guarantees no task is stranded.';
