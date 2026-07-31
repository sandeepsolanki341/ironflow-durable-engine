package io.ironflow.api.dto;

import io.ironflow.api.SignalService;

import java.util.UUID;

/**
 * Result of a signal delivery.
 *
 * @param deduplicated {@code true} if this delivery was suppressed as a retry of a signal id
 *                     already delivered. Not a failure - from the caller's perspective the
 *                     signal IS delivered, which is what they wanted.
 * @param woken        {@code false} when a decision task was already pending, so no new one
 *                     was created. Also not a failure: the pending decision will observe
 *                     this signal when it runs.
 * @param buffered     {@code true} when the target execution did not exist yet and the
 *                     signal was held for delivery at start time.
 */
public record SignalResponse(
        UUID executionId,
        String signalName,
        Long sequenceNumber,
        boolean deduplicated,
        boolean woken,
        boolean buffered) {

    public static SignalResponse delivered(SignalService.SignalResult result) {
        return new SignalResponse(result.executionId(), result.signalName(),
                result.sequenceNumber(), false, result.woken(), false);
    }

    public static SignalResponse deduplicated(UUID executionId, String signalName) {
        return new SignalResponse(executionId, signalName, null, true, false, false);
    }

    public static SignalResponse buffered(String signalName) {
        return new SignalResponse(null, signalName, null, false, false, true);
    }
}
