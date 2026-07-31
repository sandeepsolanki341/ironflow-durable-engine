package io.ironflow.queue.notify;

import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Listens on {@code task_queue_channel} and wakes idle pollers.
 *
 * <h2>Why a dedicated connection outside the pool</h2>
 *
 * <p>A {@code LISTEN} registration lives on a specific backend connection and dies with it.
 * A pooled connection is returned, reused for unrelated queries, and eventually evicted - at
 * which point the registration silently disappears and notifications stop arriving with no
 * error anywhere.</p>
 *
 * <p>Worse, this connection is occupied indefinitely by design, so borrowing it from the
 * pool permanently removes one connection from the pool's capacity while Hikari's leak
 * detector reports it as abandoned.</p>
 *
 * <h2>Why polling for notifications rather than a callback</h2>
 *
 * <p>The PostgreSQL JDBC driver has no push API. {@code PGConnection.getNotifications(ms)}
 * blocks up to a timeout and returns whatever arrived - a blocking call, and therefore
 * exactly what a virtual thread is for. The carrier thread is released while parked, so this
 * loop costs essentially nothing.</p>
 *
 * <p>Note the distinction from the polling this replaces: that was polling the
 * <em>database for rows</em>, which costs a query, index traversal and buffer reads per
 * poll. This is polling a <em>socket buffer</em>, which costs nothing when idle.</p>
 *
 * <h2>Delivery is best-effort, deliberately</h2>
 *
 * <p>Notifications are lost on reconnect, and PostgreSQL's notification queue can overflow
 * under extreme load. The design therefore treats every notification as an optimisation and
 * never as a guarantee: {@link QueueSignal} pairs it with a safety-net interval, so a missed
 * notification costs latency and nothing else.</p>
 *
 * <p><b>This is the single most important property of the implementation.</b> Any version
 * where a dropped notification strands a task is broken regardless of how reliable the
 * channel appears in testing - and it will appear extremely reliable, right up until a
 * failover.</p>
 */
@Service
public class PostgresNotificationListener {

    private static final Logger log =
            LoggerFactory.getLogger(PostgresNotificationListener.class);

    public static final String CHANNEL = "task_queue_channel";

    /**
     * How long each {@code getNotifications} call blocks.
     *
     * <p>Not a latency floor - notifications arriving mid-block return immediately. It
     * bounds only how quickly the loop notices a dead connection or a shutdown request.</p>
     */
    private static final Duration POLL_BLOCK = Duration.ofSeconds(5);

    private static final Duration RECONNECT_MIN = Duration.ofMillis(250);
    private static final Duration RECONNECT_MAX = Duration.ofSeconds(30);

    private final DataSource dataSource;
    private final QueueSignal signal;
    private final boolean enabled;

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<Connection> listenConnection = new AtomicReference<>();
    private final AtomicLong notificationsReceived = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private volatile Thread listenerThread;
    private volatile Instant lastNotification = Instant.EPOCH;

    public PostgresNotificationListener(
            DataSource dataSource,
            QueueSignal signal,
            @Value("${ironflow.notify.enabled:true}") boolean enabled) {
        this.dataSource = dataSource;
        this.signal = signal;
        this.enabled = enabled;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!enabled) {
            log.info("LISTEN/NOTIFY disabled; pollers will use safety-net interval only");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        listenerThread = Thread.ofVirtual()
                .name("ironflow-notify-listener")
                .start(this::listenLoop);
        log.info("Notification listener started on channel '{}'", CHANNEL);
    }

    /**
     * Connect, LISTEN, and consume notifications until shutdown.
     *
     * <p>Reconnects with exponential backoff on failure. The reconnect path is the dangerous
     * one: notifications published while disconnected are gone forever, so on every
     * reconnect we broadcast an unconditional wakeup to all pollers. Without that, a task
     * enqueued during a two-second failover would wait for the safety-net interval - correct,
     * but a visible latency spike exactly when the system is already degraded.</p>
     */
    private void listenLoop() {
        long backoffMillis = RECONNECT_MIN.toMillis();

        while (running.get()) {
            try (Connection conn = dataSource.getConnection()) {
                listenConnection.set(conn);

                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + CHANNEL);
                }
                // LISTEN is transactional like anything else. Without an explicit commit
                // under a non-autocommit connection the registration is never durable and
                // no notification ever arrives - a silent, total failure.
                if (!conn.getAutoCommit()) {
                    conn.commit();
                }

                log.info("Listening on '{}'", CHANNEL);
                backoffMillis = RECONNECT_MIN.toMillis();

                // Anything published while we were disconnected is unrecoverable, so assume
                // work is waiting and let the pollers check.
                signal.wakeAll();

                consumeUntilFailure(conn.unwrap(PGConnection.class));

            } catch (Exception e) {
                if (!running.get()) {
                    break;
                }
                reconnects.incrementAndGet();
                log.warn("Notification listener connection lost ({}); reconnecting in {}ms",
                        e.getMessage(), backoffMillis);
                sleepQuietly(backoffMillis);
                backoffMillis = Math.min(backoffMillis * 2, RECONNECT_MAX.toMillis());
            } finally {
                listenConnection.set(null);
            }
        }
        log.info("Notification listener stopped");
    }

    /**
     * Drains notifications until the connection fails.
     *
     * <p>Notice what happens on a batch: N notifications produce one wakeup per distinct
     * queue, not N wakeups. Under load, a thousand enqueues in one second arrive as a batch
     * and the poller is woken once - then its own batch lease picks up all thousand tasks.
     * Waking per notification would produce a thousand redundant lease queries, which is
     * precisely the database load this feature exists to remove.</p>
     */
    private void consumeUntilFailure(PGConnection pgConn) throws SQLException {
        while (running.get()) {
            PGNotification[] notifications =
                    pgConn.getNotifications((int) POLL_BLOCK.toMillis());

            if (notifications == null || notifications.length == 0) {
                continue;   // timeout with nothing pending; loop and block again
            }

            notificationsReceived.addAndGet(notifications.length);
            lastNotification = Instant.now();

            Set<String> wokenKeys = new HashSet<>();
            for (PGNotification n : notifications) {
                wokenKeys.add(n.getParameter());
            }
            for (String key : wokenKeys) {
                signal.wake(key);
            }

            if (log.isTraceEnabled()) {
                log.trace("Woke {} queue(s) from {} notification(s)",
                        wokenKeys.size(), notifications.length);
            }
        }
    }

    @PreDestroy
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Thread t = listenerThread;
        if (t != null) {
            t.interrupt();
        }
        Connection conn = listenConnection.get();
        if (conn != null) {
            try {
                // Forces the blocking getNotifications() to return promptly rather than
                // waiting out its timeout during shutdown.
                conn.close();
            } catch (SQLException ignored) {
                // Shutting down; nothing useful to do.
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- Observability --------------------------------------------------------------

    /**
     * @return {@code true} if the listener currently holds a live LISTEN registration. When
     *         false, dispatch still works via the safety-net interval - degraded, not
     *         broken. Worth alerting on nonetheless, since sustained false means every
     *         dispatch is paying full polling latency.
     */
    public boolean isConnected() {
        return listenConnection.get() != null;
    }

    public long notificationsReceived() {
        return notificationsReceived.get();
    }

    public long reconnectCount() {
        return reconnects.get();
    }

    public Instant lastNotificationAt() {
        return lastNotification;
    }
}
