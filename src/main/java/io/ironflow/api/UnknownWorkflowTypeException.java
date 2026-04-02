package io.ironflow.api;

import java.util.Set;

/**
 * Thrown when a start request names a workflow type no worker has registered.
 *
 * <p>Checked eagerly at the API boundary so a typo fails fast with a 400 rather than
 * creating an execution that sits RUNNING forever because nothing can advance it. The
 * known types are included in the message: this error is nearly always a typo, and
 * showing the alternatives turns a support ticket into a self-service fix.</p>
 */
public class UnknownWorkflowTypeException extends RuntimeException {

    public UnknownWorkflowTypeException(String requested, Set<String> known) {
        super("Unknown workflow type '%s'. Registered types: %s".formatted(requested, known));
    }
}
