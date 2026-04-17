-- =====================================================================================
-- V8 - LISTEN/NOTIFY event-stream fan-out for the observability dashboard (SSE).
--
-- Emits a notification whenever a new row is appended to wf_events, so the API tier can push
-- that event to any dashboard client currently streaming that execution over SSE - without
-- polling wf_events on a timer per open connection.
--
-- WHY A TRIGGER (same reasoning as V6, restated because it is the crux)
--
-- Events are appended from six different committers: DecisionCommitter,
-- ActivityCompletionCommitter (via OrchestratorTransactionManager), ActivityFailureCommitter,
-- CompensationCommitter, WorkflowService.start, and the timer path. Hooking pg_notify into
-- each Java call site would mean six places to keep in sync, and one missed call is an event
-- that never reaches the live DAG - a bug that shows up on exactly one workflow shape and
-- survives review. A trigger fires from one place regardless of who inserts.
--
-- TRANSACTIONAL DELIVERY IS WHAT MAKES THIS SAFE
--
-- PostgreSQL holds notifications until COMMIT. The SSE layer therefore can never be told
-- about an event row that is not yet visible to a reader - so when the client reacts by
-- re-fetching (or by applying the pushed row), the row is guaranteed to be there. This is the
-- same property that makes the queue wakeup safe in V6, and it is the reason a database-native
-- stream beats bolting an external broker onto the append path.
--
-- PAYLOAD IS A ROUTING HINT, NOT DATA
--
-- The notification carries only the execution id (and the new sequence number, as a cheap
-- freshness cue). It deliberately does NOT carry the event payload: NOTIFY payloads are capped
-- at 8000 bytes and the cluster-wide notification queue is a fixed 8GB, so shipping event
-- bodies through it would be a latent overflow under load. The API tier re-reads the actual
-- event(s) from wf_events - the source of truth - keyed by the execution id. Best-effort
-- delivery is fine: a dropped notification just means the client's next reconnect or the
-- detail hook's poll picks the events up. Nothing is stranded.
-- =====================================================================================

CREATE OR REPLACE FUNCTION wf_events_notify() RETURNS TRIGGER AS $$
BEGIN
    -- execution_id routes the notification to the right set of SSE emitters; sequence_number
    -- lets the listener skip a re-read if it has already seen up to this point.
    PERFORM pg_notify('wf_events_channel',
                      NEW.execution_id::text || ':' || NEW.sequence_number::text);
    RETURN NULL;   -- AFTER trigger; return value ignored
END;
$$ LANGUAGE plpgsql;

-- AFTER INSERT only. wf_events is append-only (enforced by trg_wf_events_immutable), so there
-- is no UPDATE/DELETE path to consider - inserts are the entire lifecycle of a row.
CREATE TRIGGER trg_wf_events_notify_insert
    AFTER INSERT ON wf_events
    FOR EACH ROW EXECUTE FUNCTION wf_events_notify();

COMMENT ON FUNCTION wf_events_notify() IS
    'Notifies wf_events_channel on history append so the SSE layer can push to live '
    'dashboard clients. Payload is execution_id:sequence_number - a routing hint only; the '
    'API re-reads the event body from wf_events. Delivery is best-effort and fires on COMMIT.';
