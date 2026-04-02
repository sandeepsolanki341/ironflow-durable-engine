package io.ironflow.api;

import io.ironflow.api.dto.ExecutionDetailResponse;
import io.ironflow.api.dto.SignalRequest;
import io.ironflow.api.dto.SignalResponse;
import io.ironflow.api.dto.StartWorkflowRequest;
import io.ironflow.api.dto.StartWorkflowResponse;
import io.ironflow.replay.DivergenceQuarantine;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * Public HTTP surface for starting, inspecting, and signalling workflow executions.
 *
 * <p>Deliberately thin: it validates, delegates, and maps to HTTP. All transactional work
 * lives in the services.</p>
 */
@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private final WorkflowService service;
    private final SignalService signalService;
    private final DivergenceQuarantine quarantine;

    public WorkflowController(WorkflowService service, SignalService signalService,
                              DivergenceQuarantine quarantine) {
        this.service = service;
        this.signalService = signalService;
        this.quarantine = quarantine;
    }

    /**
     * Starts a workflow execution.
     *
     * @return {@code 201 Created} for a new execution; {@code 200 OK} when an existing
     *         business key matched, since nothing was created. Both carry a
     *         {@code Location} header pointing at the execution.
     */
    @PostMapping("/start")
    public ResponseEntity<StartWorkflowResponse> start(
            @Valid @RequestBody StartWorkflowRequest request,
            UriComponentsBuilder uriBuilder) {

        StartWorkflowResponse response = service.start(request);

        URI location = uriBuilder.path("/api/v1/workflows/{id}")
                .buildAndExpand(response.executionId())
                .toUri();

        return response.alreadyExisted()
                ? ResponseEntity.ok().location(location).body(response)
                : ResponseEntity.created(location).body(response);
    }

    /**
     * Returns an execution with its full event history in replay order.
     *
     * @param includeHistory set {@code false} for a status-only poll. Callers polling for
     *                       completion should always pass {@code false} - history grows
     *                       without bound, and a client polling once a second while
     *                       fetching full history will eventually transfer megabytes per
     *                       request for no benefit.
     */
    @GetMapping("/{id}")
    public ExecutionDetailResponse get(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "true") boolean includeHistory) {
        return service.findById(id, includeHistory);
    }

    /**
     * Delivers an external signal to a running workflow.
     *
     * @return {@code 202 Accepted}: the signal is durably recorded, but the workflow
     *         processes it asynchronously. {@code 200} would imply it had been handled.
     */
    @PostMapping("/{id}/signal")
    public ResponseEntity<SignalResponse> signal(
            @PathVariable UUID id,
            @Valid @RequestBody SignalRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false)
            String idempotencyKey) {

        String signalId = request.signalId() != null ? request.signalId() : idempotencyKey;

        try {
            var result = signalService.signal(id, request.signalName(),
                    request.payload(), signalId);
            return ResponseEntity.accepted().body(SignalResponse.delivered(result));

        } catch (SignalAlreadyDeliveredException e) {
            // A retry of a delivery that already succeeded. 202 with deduplicated=true, not
            // an error - from the caller's perspective the signal is delivered.
            return ResponseEntity.accepted()
                    .body(SignalResponse.deduplicated(id, request.signalName()));
        }
    }

    /**
     * Delivers a signal addressed by business key, buffering it if the execution does not
     * exist yet.
     *
     * <p>A separate endpoint rather than an overload, because the semantics differ: this one
     * can succeed for a workflow that has not been created. That handles the real ordering
     * hazard where "create order" and "cancel order" race.</p>
     */
    @PostMapping("/by-key/{businessKey}/signal")
    public ResponseEntity<SignalResponse> signalByKey(
            @PathVariable String businessKey,
            @Valid @RequestBody SignalRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false)
            String idempotencyKey) {

        String signalId = request.signalId() != null ? request.signalId() : idempotencyKey;
        signalService.bufferForFutureExecution(
                businessKey, request.signalName(), request.payload(), signalId);
        return ResponseEntity.accepted().body(SignalResponse.buffered(request.signalName()));
    }

    /** Operator view: executions quarantined after a replay divergence. */
    @GetMapping("/divergent")
    public List<DivergenceQuarantine.DivergentExecution> divergent(
            @RequestParam(defaultValue = "50") int limit) {
        return quarantine.listDivergent(limit);
    }

    /**
     * Returns a quarantined execution to RUNNING.
     *
     * <p>Call after the offending code is rolled back or patched. If the code still
     * diverges, the execution quarantines again and {@code divergence_count} increments -
     * which is the signal the fix did not work, rather than an infinite retry.</p>
     */
    @PostMapping("/{id}/resume")
    public ResponseEntity<Void> resume(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "default") String taskQueue) {
        quarantine.resume(id, taskQueue);
        return ResponseEntity.accepted().build();
    }
}
