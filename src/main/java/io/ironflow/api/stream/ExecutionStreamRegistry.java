package io.ironflow.api.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Tracks the open {@link SseEmitter}s for each execution, so a single database notification can
 * be fanned out to every dashboard tab currently watching that execution.
 *
 * <h2>Why a registry rather than one emitter per execution</h2>
 *
 * <p>Several people can watch the same execution at once - two operators, a presenter and a
 * screen, the same user in two tabs. Each needs its own {@link SseEmitter} (an emitter is a
 * single HTTP response and cannot be shared), but they all care about the same event stream.
 * The registry maps {@code executionId -> list of emitters} so the listener does one re-read of
 * history per notification and writes it to all interested connections, instead of every
 * connection independently polling the database.</p>
 *
 * <h2>Lifecycle and cleanup</h2>
 *
 * <p>An SSE connection can end three ways: the client navigates away (completion), the request
 * times out, or a write fails because the socket is already gone. All three must remove the
 * emitter from the registry, or the map leaks emitters for connections that no longer exist and
 * every notification wastes work writing to dead sockets. Each emitter is registered with
 * onCompletion/onTimeout/onError callbacks that evict it. Eviction is idempotent and safe to
 * call from any of those paths.</p>
 *
 * <p>The per-execution lists are {@link CopyOnWriteArrayList}: writes (register/evict) are rare
 * relative to reads (every notification iterates the list to broadcast), and the copy-on-write
 * cost is paid exactly when connections open or close, not on the hot broadcast path.</p>
 */
@Component
public class ExecutionStreamRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutionStreamRegistry.class);

    private final Map<UUID, CopyOnWriteArrayList<SseEmitter>> emitters =
            new ConcurrentHashMap<>();

    /**
     * Registers an emitter for an execution and wires its teardown callbacks.
     *
     * @return the same emitter, for fluent use in the controller
     */
    public SseEmitter register(UUID executionId, SseEmitter emitter) {
        emitters.computeIfAbsent(executionId, k -> new CopyOnWriteArrayList<>()).add(emitter);

        // All three teardown paths evict. Without this the map grows without bound as tabs
        // close, and every future notification writes to sockets that are already gone.
        emitter.onCompletion(() -> evict(executionId, emitter));
        emitter.onTimeout(() -> evict(executionId, emitter));
        emitter.onError(e -> evict(executionId, emitter));

        log.debug("Registered SSE emitter for execution {} ({} now open)",
                executionId, count(executionId));
        return emitter;
    }

    private void evict(UUID executionId, SseEmitter emitter) {
        CopyOnWriteArrayList<SseEmitter> list = emitters.get(executionId);
        if (list != null) {
            list.remove(emitter);
            // Drop the bucket entirely once empty, so an execution that is no longer watched
            // leaves no residue in the map.
            if (list.isEmpty()) {
                emitters.remove(executionId, list);
            }
        }
    }

    /** Emitters currently watching an execution; empty list if none. */
    public List<SseEmitter> emittersFor(UUID executionId) {
        return emitters.getOrDefault(executionId, new CopyOnWriteArrayList<>());
    }

    /** True if at least one client is watching - lets the listener skip a re-read if not. */
    public boolean hasWatchers(UUID executionId) {
        CopyOnWriteArrayList<SseEmitter> list = emitters.get(executionId);
        return list != null && !list.isEmpty();
    }

    public int count(UUID executionId) {
        CopyOnWriteArrayList<SseEmitter> list = emitters.get(executionId);
        return list == null ? 0 : list.size();
    }

    /**
     * Writes a named SSE event to every emitter watching this execution, evicting any that
     * fail (the client is gone). Returns the number of successful deliveries.
     */
    public int broadcast(UUID executionId, String eventName, Object payload) {
        List<SseEmitter> list = emittersFor(executionId);
        int delivered = 0;
        for (SseEmitter emitter : list) {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(payload));
                delivered++;
            } catch (IOException | IllegalStateException e) {
                // Socket already closed, or emitter already completed. Evict and move on;
                // one dead connection must not block delivery to the healthy ones.
                evict(executionId, emitter);
            }
        }
        return delivered;
    }
}
