package io.ironflow.persistence.model;

/**
 * Discriminator for the polymorphic task queue.
 *
 * <p>All three kinds share one table and one dispatch path. This is deliberate: a
 * timer is not a separate subsystem, it is a task row whose {@code not_before} is in
 * the future, and retry backoff is the same mechanism. Collapsing them removes an
 * entire scheduler component from the design.</p>
 */
public enum TaskKind {
    /** A decision task: replays workflow code to produce the next commands. */
    WORKFLOW,
    /** An activity task: executes user side-effecting code. At-least-once. */
    ACTIVITY,
    /** A durable timer: a task whose only meaningful payload is its {@code not_before}. */
    TIMER
}
