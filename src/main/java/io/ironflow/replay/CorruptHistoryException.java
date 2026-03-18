package io.ironflow.replay;

/**
 * Thrown when history itself is malformed - a duplicate outcome for one command, an
 * outcome with no {@code scheduledEventSeq}, a signal with no name.
 *
 * <p>Distinct from {@link NonDeterministicError}, and the distinction matters
 * operationally. Divergence means the <em>code</em> changed and the fix is a rollback.
 * Corruption means the <em>data</em> is wrong, which implies an engine bug - the
 * orchestrator double-applied a completion, or something wrote history outside the normal
 * path. A rollback will not help.</p>
 */
public class CorruptHistoryException extends RuntimeException {

    public CorruptHistoryException(String message) {
        super(message);
    }

    public CorruptHistoryException(String message, Throwable cause) {
        super(message, cause);
    }
}
