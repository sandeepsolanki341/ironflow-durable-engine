package io.ironflow.replay;

import java.time.Duration;
import java.util.UUID;

/**
 * Thrown when a workflow body runs too long without reaching an SDK call.
 *
 * <p>Guards against an infinite loop in workflow code. Without it, such a workflow pins a
 * carrier thread and never releases its lease; the reaper then hands it to another worker
 * to hang identically, and the fleet degrades one slot at a time.</p>
 *
 * <p>Not a divergence: quarantining under that banner would send operators hunting for a
 * history mismatch that does not exist.</p>
 */
public class DecisionTimeoutException extends RuntimeException {

    public DecisionTimeoutException(UUID executionId, Duration timeout) {
        super("Workflow %s did not reach an SDK call within %s; suspected infinite loop"
                .formatted(executionId, timeout));
    }
}
