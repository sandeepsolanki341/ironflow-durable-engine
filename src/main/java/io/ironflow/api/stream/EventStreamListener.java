package io.ironflow.api.stream;

import io.ironflow.api.WorkflowService;
import io.ironflow.api.dto.ExecutionDetailResponse;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bridges PostgreSQL's {@code wf_events_channel} notifications to open SSE connections.
 *
 * <h2>What it does</h2>
 *
 * <p>Holds one dedicated connection with a {@code LISTEN wf_events_channel} registration, in a
 * loop that mirrors the queue's {@link io.ironflow.queue.notify.PostgresNotificationListener}:
 * block for notifications, reconnect with exponential backoff on failure. Each notification
 * carries {@code executionId:sequenceNumber}. For an execution that actually has watchers, it
 * reads the new events (those past what that stream last sent) and broadcasts them to every
 * emitter via {@link ExecutionStreamRegistry}.</p>
 *
 * <h2>Why re-read rather than trust the payload</h2>
 *
 * <p>The notification payload is a routing hint, not the event. NOTIFY payloads are capped and
 * the notification queue is a bounded shared resource, so the event body travels through
 * wf_events - the source of truth - not through the channel. The listener re-reads, which also
 * means a batch of ten appends that arrive as ten notifications still results in correct output:
 * each re-read fetches whatever is new since the last high-water mark for that execution, so
 * duplicate or coalesced notifications converge rather than double-send.</p>
 *
 * <h2>Per-execution high-water mark</h2>
 *
 * <p>{@code lastSentSeq} tracks the highest sequence number already pushed for each watched
 * execution. This makes delivery idempotent under the best-effort channel: a redelivered or
 * out-of-order notification for an already-sent sequence reads zero new rows and sends nothing.
 * The map is pruned when an execution loses all watchers, so it does not grow unbounded.</p>
 */
@Component
public class EventStreamListener {

    private static final Logger log = LoggerFactory.getLogger(EventStreamListener.class);

    public static final String CHANNEL = "wf_events_channel";

    private static final Duration POLL_BLOCK = Duration.ofSeconds(5);
    private static final Duration RECONNECT_MIN = Duration.ofMillis(250);
    private static final Duration RECONNECT_MAX = Duration.ofSeconds(30);

    private final DataSource dataSource;
    private final ExecutionStreamRegistry registry;
    private final WorkflowService service;
    private final boolean enabled;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicLong notificationsReceived = new AtomicLong();
    private volatile Thread listenerThread;

    /** Highest sequence number already streamed per execution, for idempotent re-reads. */
    private final ConcurrentHashMap<UUID, Long> lastSentSeq = new ConcurrentHashMap<>();

    public EventStreamListener(
            DataSource dataSource,
            ExecutionStreamRegistry registry,
            WorkflowService service,
            @Value("${ironflow.sse.enabled:true}") boolean enabled) {
        this.dataSource = dataSource;
        this.registry = registry;
        this.service = service;
        this.enabled = enabled;
    }

    @PostConstruct
    public void start() {
        if (!enabled) {
            log.info("SSE event stream disabled; dashboard will fall back to polling");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        listenerThread = new Thread(this::listenLoop, "wf-events-sse-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
        log.info("Event stream listener started on channel '{}'", CHANNEL);
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
    }

    private void listenLoop() {
        long backoffMillis = RECONNECT_MIN.toMillis();

        while (running.get()) {
            try (Connection conn = dataSource.getConnection()) {
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                if (!conn.getAutoCommit()) {
                    conn.commit();   // LISTEN must be committed or no notification ever arrives
                }
                log.info("Listening on '{}'", CHANNEL);
                backoffMillis = RECONNECT_MIN.toMillis();

                consumeUntilFailure(conn.unwrap(PGConnection.class));

            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                log.warn("Event stream listener connection lost ({}); reconnecting in {}ms",
                        e.getMessage(), backoffMillis);
                sleepQuietly(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, RECONNECT_MAX.toMillis());
            }
        }
        log.info("Event stream listener stopped");
    }

    private void consumeUntilFailure(PGConnection pg) throws Exception {
        while (running.get()) {
            PGNotification[] notifications = pg.getNotifications((int) POLL_BLOCK.toMillis());
            if (notifications == null) {
                continue;   // block timed out; loop to re-check running flag
            }
            for (PGNotification n : notifications) {
                notificationsReceived.incrementAndGet();
                handleNotification(n.getParameter());
            }
        }
    }

    /**
     * Parses an {@code executionId:sequenceNumber} payload and pushes any new events to the
     * emitters watching that execution.
     */
    private void handleNotification(String payload) {
        UUID executionId;
        try {
            int colon = payload.indexOf(':');
            String idPart = colon >= 0 ? payload.substring(0, colon) : payload;
            executionId = UUID.fromString(idPart);
        } catch (RuntimeException e) {
            log.debug("Ignoring malformed event notification payload: {}", payload);
            return;
        }

        // No one is watching this execution - do not touch the database. This is what keeps the
        // stream cheap: notifications for unwatched executions cost one map lookup and stop.
        if (!registry.hasWatchers(executionId)) {
            return;
        }

        long since = lastSentSeq.getOrDefault(executionId, 0L);
        List<ExecutionDetailResponse.EventView> fresh = service.historyAfter(executionId, since);
        if (fresh.isEmpty()) {
            return;   // already sent everything up to here; a coalesced/duplicate notification
        }

        long maxSeq = since;
        for (ExecutionDetailResponse.EventView ev : fresh) {
            registry.broadcast(executionId, "event", ev);
            if (ev.sequenceNumber() > maxSeq) {
                maxSeq = ev.sequenceNumber();
            }
        }
        lastSentSeq.put(executionId, maxSeq);

        // If the execution just reached a terminal event, tell clients so they can close the
        // stream and stop reconnecting. The client also detects this via status, but an
        // explicit signal avoids a reconnect storm at end-of-life.
        if (!registry.hasWatchers(executionId)) {
            lastSentSeq.remove(executionId);
        }
    }

    /** Seeds the high-water mark when a client connects, so it only gets truly-new events. */
    public void primeWatermark(UUID executionId, long alreadySeenSeq) {
        lastSentSeq.merge(executionId, alreadySeenSeq, Math::max);
    }

    /** Drops the high-water mark once an execution has no watchers left. */
    public void forget(UUID executionId) {
        if (!registry.hasWatchers(executionId)) {
            lastSentSeq.remove(executionId);
        }
    }

    public long notificationsReceived() {
        return notificationsReceived.get();
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
