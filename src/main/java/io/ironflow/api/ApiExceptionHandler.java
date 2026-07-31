package io.ironflow.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.stream.Collectors;

/**
 * Maps domain failures to RFC 9457 problem details.
 *
 * <p>Centralised so no controller leaks a stack trace or a raw
 * {@code DataIntegrityViolationException} message. The latter matters more than it looks:
 * those messages embed index and constraint names, which disclose schema structure to
 * unauthenticated callers.</p>
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    private static final String BASE = "https://ironflow.io/problems/";

    @ExceptionHandler(ExecutionNotFoundException.class)
    public ProblemDetail notFound(ExecutionNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Execution not found",
                "execution-not-found", e.getMessage());
    }

    @ExceptionHandler(UnknownWorkflowTypeException.class)
    public ProblemDetail unknownType(UnknownWorkflowTypeException e) {
        return problem(HttpStatus.BAD_REQUEST, "Unknown workflow type",
                "unknown-workflow-type", e.getMessage());
    }

    /**
     * A reused business key with different input.
     *
     * <p>409 rather than 200: returning the existing execution would tell the caller their
     * request succeeded while silently discarding the input they actually sent.</p>
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail idempotencyConflict(IdempotencyConflictException e) {
        ProblemDetail pd = problem(HttpStatus.CONFLICT, "Business key collision",
                "idempotency-conflict", e.getMessage());
        pd.setProperty("existingExecutionId", e.getExistingExecutionId().toString());
        return pd;
    }

    @ExceptionHandler(ExecutionNotRunningException.class)
    public ProblemDetail notRunning(ExecutionNotRunningException e) {
        return problem(HttpStatus.CONFLICT, "Execution is not running",
                "execution-not-running", e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail illegalState(IllegalStateException e) {
        // Covers resume() on a non-DIVERGENT execution and similar precondition failures.
        return problem(HttpStatus.CONFLICT, "Invalid state transition",
                "invalid-state", e.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail invalid(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + ": " + f.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "Invalid request",
                "invalid-request", detail);
    }

    /**
     * Catch-all. Logs the full cause server-side and returns a deliberately opaque body: the
     * caller can do nothing with a stack trace except learn about our internals.
     */
    @ExceptionHandler(Exception.class)
    public ProblemDetail internal(Exception e) {
        log.error("Unhandled exception serving request", e);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "internal-error", "An internal error occurred");
    }

    private static ProblemDetail problem(HttpStatus status, String title,
                                         String type, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setType(URI.create(BASE + type));
        pd.setTitle(title);
        return pd;
    }
}
