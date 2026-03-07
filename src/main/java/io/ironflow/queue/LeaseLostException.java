package io.ironflow.queue;

/**
 * Thrown when a worker attempts to commit work for a task it no longer owns.
 *
 * <p>Not an error condition in the usual sense - it is the system working correctly.
 * The lease expired, the reaper reclaimed the task, and another worker now owns it. The
 * correct response is to discard the in-flight result silently: the other worker will
 * produce its own. Retrying would be actively wrong, since it would race the current
 * owner.</p>
 *
 * <p>A rising rate of these is the primary signal that leases are configured too short
 * for the work being executed, or that heartbeating is missing where it is needed.</p>
 */
public class LeaseLostException extends RuntimeException {

    public LeaseLostException(String message) {
        super(message);
    }
}
