package io.ironflow.replay;

/** Event type constants shared by the replay engine, orchestrator, and workers. */
public final class EventTypes {

    public static final String WORKFLOW_STARTED = "WORKFLOW_STARTED";
    public static final String WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED";
    public static final String WORKFLOW_FAILED = "WORKFLOW_FAILED";
    public static final String WORKFLOW_TASK_SCHEDULED = "WORKFLOW_TASK_SCHEDULED";
    public static final String ACTIVITY_SCHEDULED = "ACTIVITY_SCHEDULED";
    public static final String ACTIVITY_COMPLETED = "ACTIVITY_COMPLETED";
    public static final String ACTIVITY_FAILED = "ACTIVITY_FAILED";
    public static final String TIMER_STARTED = "TIMER_STARTED";
    public static final String TIMER_FIRED = "TIMER_FIRED";
    public static final String MARKER_RECORDED = "MARKER_RECORDED";
    public static final String SIGNAL_RECEIVED = "SIGNAL_RECEIVED";

    // ---- Saga compensation (Phase 5) ------------------------------------------------

    /**
     * Recorded by {@code ctx.compensateWith(...)} after a successful step, capturing the
     * intent to undo it. The LIFO compensation stack is DERIVED by scanning history for
     * these in reverse order - it is never held in memory, so it survives the crash that
     * triggers the rollback.
     */
    public static final String COMPENSATION_REGISTERED = "COMPENSATION_REGISTERED";

    /** Marks entry into the COMPENSATING state, once per execution. */
    public static final String COMPENSATION_TRIGGERED = "COMPENSATION_TRIGGERED";

    /**
     * One compensation activity finished. Carries the registration seq it discharges, so
     * replay knows which entries on the derived stack are already done and which remain.
     */
    public static final String COMPENSATION_COMPLETED = "COMPENSATION_COMPLETED";

    private EventTypes() { }
}
