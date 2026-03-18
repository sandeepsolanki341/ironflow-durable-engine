package io.ironflow.replay;

/**
 * Thrown into a parked workflow thread when it is interrupted during shutdown.
 *
 * <p>Not a failure. Parked workflow threads are abandoned by design - the next decision
 * task builds a fresh workflow object and replays from the top - so interrupting them at
 * shutdown is routine cleanup, and {@code ReplayRunner} swallows this.</p>
 */
public class WorkflowAbandonedException extends RuntimeException {

    public WorkflowAbandonedException() {
        super("Workflow thread abandoned during shutdown");
    }
}
