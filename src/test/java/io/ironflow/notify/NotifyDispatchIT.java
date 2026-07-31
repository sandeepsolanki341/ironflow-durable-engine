package io.ironflow.notify;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.notify.PostgresNotificationListener;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * LISTEN/NOTIFY dispatch behaviour.
 */
@SpringBootTest
@Import(TestFixtures.class)
class NotifyDispatchIT extends AbstractPostgresIT {

    @Autowired PostgresNotificationListener listener;
    @Autowired TestFixtures fixtures;
    @Autowired DSLContext dsl;
    @Autowired PlatformTransactionManager txManager;

    @BeforeEach
    void setUp() {
        fixtures.truncateAll();
        await().atMost(Duration.ofSeconds(30)).until(listener::isConnected);
    }

    /**
     * Notifications must not be delivered before the row is visible.
     *
     * <p>This is the property that makes database-native pub/sub safe where an external
     * broker is not. If a poller could be woken before commit, it would find nothing, sleep,
     * and the task would wait for the safety net - reintroducing exactly the latency this
     * feature removes.</p>
     */
    @Test
    void notificationIsNotDeliveredBeforeCommit() throws Exception {
        long before = listener.notificationsReceived();

        var txTemplate = new TransactionTemplate(txManager);
        var insertedLatch = new CountDownLatch(1);
        var releaseLatch = new CountDownLatch(1);

        Thread inserter = Thread.ofVirtual().start(() -> txTemplate.executeWithoutResult(s -> {
            UUID execId = fixtures.newExecution("commit-timing-test");
            fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);
            insertedLatch.countDown();
            try {
                releaseLatch.await(10, TimeUnit.SECONDS);   // hold the transaction open
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        insertedLatch.await(5, TimeUnit.SECONDS);
        Thread.sleep(500);   // ample time for a premature notification to arrive

        assertThat(listener.notificationsReceived())
                .as("no notification may be delivered while the transaction is open")
                .isEqualTo(before);

        releaseLatch.countDown();
        inserter.join(10_000);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(listener.notificationsReceived())
                        .isGreaterThan(before));
    }

    /**
     * The safety net. A dropped notification must cost latency, never a stranded task.
     *
     * <p>Simulated by inserting with the trigger disabled, which is exactly what a lost
     * notification looks like from the poller's side.</p>
     */
    @Test
    void taskEnqueuedWithoutNotificationIsStillDispatched() {
        dsl.execute("ALTER TABLE wf_tasks DISABLE TRIGGER trg_wf_tasks_notify_insert");
        try {
            UUID execId = fixtures.newExecution("no-notify-test");
            fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);

            // No notification fired. Only the safety-net poll can find this.
            await().atMost(Duration.ofSeconds(90))
                    .untilAsserted(() -> assertThat(fixtures.countByStatus("PENDING"))
                            .as("the safety net must eventually dispatch it")
                            .isZero());
        } finally {
            dsl.execute("ALTER TABLE wf_tasks ENABLE TRIGGER trg_wf_tasks_notify_insert");
        }
    }

    /** Timers must not wake pollers until they are actually due. */
    @Test
    void futureDatedTaskDoesNotNotify() throws Exception {
        long before = listener.notificationsReceived();

        UUID execId = fixtures.newExecution("future-timer-test");
        fixtures.insertPendingTimer(execId, Instant.now().plus(Duration.ofDays(30)));

        Thread.sleep(1_000);
        assertThat(listener.notificationsReceived())
                .as("a timer due in 30 days must not wake anyone today")
                .isEqualTo(before);
    }

    /** Lease/ack churn must not notify - that would be noise proportional to throughput. */
    @Test
    void leaseAndAckDoNotNotify() throws Exception {
        UUID execId = fixtures.newExecution("churn-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);
        await().atMost(Duration.ofSeconds(10))
                .until(() -> listener.notificationsReceived() > 0);

        long afterEnqueue = listener.notificationsReceived();

        dsl.execute("""
                UPDATE wf_tasks SET status = 'LEASED', lease_owner = ?,
                       lease_until = now() + INTERVAL '1 minute'
                 WHERE execution_id = ?
                """, UUID.randomUUID(), execId);
        dsl.execute("UPDATE wf_tasks SET status = 'COMPLETED' WHERE execution_id = ?",
                execId);

        Thread.sleep(500);
        assertThat(listener.notificationsReceived())
                .as("lease and ack must be silent")
                .isEqualTo(afterEnqueue);
    }

    /** A transition back to PENDING must notify, or every retry pays safety-net latency. */
    @Test
    void transitionBackToPendingNotifies() {
        UUID execId = fixtures.newExecution("retry-notify-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);
        await().atMost(Duration.ofSeconds(10))
                .until(() -> listener.notificationsReceived() > 0);

        dsl.execute("""
                UPDATE wf_tasks SET status = 'LEASED', lease_owner = ?,
                       lease_until = now() + INTERVAL '1 minute'
                 WHERE execution_id = ?
                """, UUID.randomUUID(), execId);

        long before = listener.notificationsReceived();
        dsl.execute("""
                UPDATE wf_tasks SET status = 'PENDING', lease_owner = NULL,
                       lease_until = NULL, not_before = now()
                 WHERE execution_id = ?
                """, execId);

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(listener.notificationsReceived())
                        .as("a retry must wake the poller immediately")
                        .isGreaterThan(before));
    }

    /** Reconnect must broadcast, since notifications during the outage are unrecoverable. */
    @Test
    void reconnectBroadcastsToAllPollers() {
        // Enqueue with notifications suppressed, simulating an enqueue during an outage.
        dsl.execute("ALTER TABLE wf_tasks DISABLE TRIGGER trg_wf_tasks_notify_insert");
        UUID execId = fixtures.newExecution("reconnect-test");
        fixtures.enqueuePending(execId, "default", TaskKind.ACTIVITY, 1);
        dsl.execute("ALTER TABLE wf_tasks ENABLE TRIGGER trg_wf_tasks_notify_insert");

        long reconnectsBefore = listener.reconnectCount();

        // Kill the listener's backend; it must reconnect and broadcast.
        dsl.execute("""
                SELECT pg_terminate_backend(pid) FROM pg_stat_activity
                 WHERE query LIKE 'LISTEN%' AND pid <> pg_backend_pid()
                """);

        await().atMost(Duration.ofSeconds(60))
                .until(() -> listener.reconnectCount() > reconnectsBefore
                        && listener.isConnected());

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(fixtures.countByStatus("PENDING"))
                        .as("the reconnect broadcast must find work enqueued during the "
                                + "outage, without waiting for the safety net")
                        .isZero());
    }
}
