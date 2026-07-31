package io.ironflow.worker;

import java.util.Set;

/**
 * Thrown when a scheduled activity type has no registered implementation.
 *
 * <p>Usually a deploy ordering problem: a workflow scheduled an activity that only exists
 * in a newer build, and an older worker picked up the task. Retrying is correct here - the
 * task will eventually land on a worker that has the implementation.</p>
 */
public class UnknownActivityTypeException extends RuntimeException {

    public UnknownActivityTypeException(String requested, Set<String> known) {
        super("Unknown activity type '%s'. Registered types: %s".formatted(requested, known));
    }
}
