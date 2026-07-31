package io.ironflow.persistence.model;

/**
 * Lifecycle of a workflow instance.
 *
 * <p>Three states are <b>non-terminal</b> - the execution can still make progress and
 * therefore carries no {@code end_time}:</p>
 * <ul>
 *   <li>{@link #RUNNING} - normal forward progress.</li>
 *   <li>{@link #DIVERGENT} - quarantined after a replay mismatch, awaiting a code rollback.</li>
 *   <li>{@link #COMPENSATING} - the forward path failed and rollback is in progress.</li>
 * </ul>
 *
 * <p>The rest are terminal. {@link #FAILED_COMPENSATED} is deliberately distinct from
 * {@link #FAILED}: it means "failed, and the saga rolled its side effects back", where plain
 * {@code FAILED} means "failed, and something may be half-done". That distinction is the whole
 * operational point of compensation.</p>
 */
public enum ExecutionStatus {
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMED_OUT,
    DIVERGENT,
    COMPENSATING,
    FAILED_COMPENSATED;

    /**
     * @return {@code true} if no further progress is possible. Terminal executions must carry
     *         an {@code end_time}, enforced by {@code ck_wf_exec_end_time}. The three live
     *         states - RUNNING, DIVERGENT, COMPENSATING - return {@code false}.
     */
    public boolean isTerminal() {
        return switch (this) {
            case RUNNING, DIVERGENT, COMPENSATING -> false;
            default -> true;
        };
    }

    /** @return {@code true} while the saga is actively rolling back completed steps. */
    public boolean isCompensating() {
        return this == COMPENSATING;
    }
}
