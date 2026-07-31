package io.ironflow.api;

import io.ironflow.api.dto.ExecutionDetailResponse;
import io.ironflow.api.dto.ExecutionSummaryResponse;
import io.ironflow.api.dto.PageResponse;
import io.ironflow.api.stream.EventStreamListener;
import io.ironflow.api.stream.ExecutionStreamRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Read-side HTTP surface for the observability dashboard.
 *
 * <p>Separate from {@link WorkflowController} on purpose. That controller is the lifecycle
 * surface - start, signal, resume - keyed by the "workflow" verb. This one is the resource
 * surface for the "execution" noun the dashboard browses: list and detail. Keeping them
 * apart means the dashboard's read endpoints can evolve (extra filters, projections) without
 * touching the operational write API, and the URL path reads as what it returns.</p>
 *
 * <p>Detail is served here at {@code /api/v1/executions/{id}} by delegating to the same
 * service method the workflows controller uses, so both paths return byte-identical bodies -
 * the dashboard does not depend on which one it hits.</p>
 */
@RestController
@RequestMapping("/api/v1/executions")
public class ExecutionController {

    private final WorkflowService service;
    private final ExecutionStreamRegistry streamRegistry;
    private final EventStreamListener eventStreamListener;

    // SSE connections are long-lived; a finite timeout lets the container reclaim a stalled
    // one rather than pinning a thread forever. The client's EventSource reconnects
    // automatically, so a timeout is invisible to the user - it just refreshes the stream.
    private static final long SSE_TIMEOUT_MS = Duration.ofMinutes(30).toMillis();

    public ExecutionController(WorkflowService service,
                              ExecutionStreamRegistry streamRegistry,
                              EventStreamListener eventStreamListener) {
        this.service = service;
        this.streamRegistry = streamRegistry;
        this.eventStreamListener = eventStreamListener;
    }

    /**
     * Lists executions, newest first, with optional status and business-key filters.
     *
     * @param status      exact status filter (e.g. {@code RUNNING}); omit for all
     * @param businessKey case-insensitive business-key prefix; omit for all
     * @param page        zero-based page index; defaults to the first page
     * @param size        page size; defaults to 25, capped server-side at 200
     */
    @GetMapping
    public PageResponse<ExecutionSummaryResponse> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String businessKey,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "25") int size) {
        return service.list(status, businessKey, page, size);
    }

    /**
     * Full execution detail including replay history.
     *
     * @param includeHistory set false to fetch metadata only, skipping the event list
     */
    @GetMapping("/{id}")
    public ExecutionDetailResponse get(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "true") boolean includeHistory) {
        return service.findById(id, includeHistory);
    }

    /**
     * Server-Sent Events stream of an execution's history, live.
     *
     * <p>On connect, the client receives the entire current history as a burst of
     * {@code event} messages (its initial snapshot), then one further {@code event} per row as
     * it is appended - driven by the {@code wf_events_channel} notification, so latency is a
     * database round trip, not a poll interval. A periodic heartbeat is unnecessary here
     * because {@link SseEmitter} plus the browser's EventSource handle reconnection; the finite
     * server timeout simply triggers a transparent client reconnect.</p>
     *
     * <p>The snapshot-then-stream handshake closes the gap where an event could be appended
     * between the initial read and the LISTEN registration: the watermark is primed to the
     * snapshot's high-water sequence BEFORE the emitter is registered, so any event that lands
     * in that window is delivered by the first notification rather than lost.</p>
     */
    @GetMapping("/{id}/stream")
    public SseEmitter stream(@PathVariable UUID id) throws IOException {
        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT_MS);

        // Initial snapshot: everything so far. Sent before registration so the client has a
        // complete picture the instant the stream opens.
        List<ExecutionDetailResponse.EventView> snapshot = service.historyAfter(id, 0L);
        long highWater = 0L;
        for (ExecutionDetailResponse.EventView ev : snapshot) {
            emitter.send(SseEmitter.event().name("event").data(ev));
            highWater = Math.max(highWater, ev.sequenceNumber());
        }

        // Prime the watermark to the snapshot's high-water BEFORE registering, so no event in
        // the connect window is double-sent or dropped.
        eventStreamListener.primeWatermark(id, highWater);
        streamRegistry.register(id, emitter);

        // A final "ready" marker lets the client distinguish "snapshot complete, now live" from
        // "still receiving initial history" - useful for a subtle live indicator in the UI.
        emitter.send(SseEmitter.event().name("ready").data(highWater));
        return emitter;
    }
}
