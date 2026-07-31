package io.ironflow.queue.notify;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Reports listener health.
 *
 * <p>Deliberately {@code DEGRADED} rather than {@code DOWN} when disconnected: dispatch
 * still works through the safety-net poll, so failing the health check would remove a
 * functioning worker from the fleet and make the problem worse.</p>
 *
 * <p>The metric worth alerting on is sustained disconnection, not a transient reconnect. A
 * high {@code reconnects} count with {@code connected=true} usually indicates an aggressive
 * connection reaper - a proxy or pooler timing out a connection that is idle by design.</p>
 */
@Component
public class NotificationListenerHealth implements HealthIndicator {

    private static final Duration STALE_THRESHOLD = Duration.ofMinutes(15);

    private final PostgresNotificationListener listener;

    public NotificationListenerHealth(PostgresNotificationListener listener) {
        this.listener = listener;
    }

    @Override
    public Health health() {
        boolean connected = listener.isConnected();
        Health.Builder builder = connected
                ? Health.up()
                : Health.status("DEGRADED")
                        .withDetail("impact",
                                "dispatch falling back to safety-net polling; "
                                + "latency raised to the safety-net interval");

        return builder
                .withDetail("channel", PostgresNotificationListener.CHANNEL)
                .withDetail("connected", connected)
                .withDetail("notificationsReceived", listener.notificationsReceived())
                .withDetail("reconnects", listener.reconnectCount())
                .withDetail("lastNotificationAt", listener.lastNotificationAt().toString())
                .withDetail("quiet", Duration.between(
                        listener.lastNotificationAt(), Instant.now())
                        .compareTo(STALE_THRESHOLD) > 0)
                .build();
    }
}
