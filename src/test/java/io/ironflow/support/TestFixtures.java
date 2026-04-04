package io.ironflow.support;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.ShardAssignment;
import org.jooq.DSLContext;
import com.fasterxml.jackson.databind.JsonNode;
import org.jooq.Record;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

/**
 * Direct-to-database helpers for integration tests.
 *
 * <p>Deliberately bypasses the production repositories. A fixture that used
 * {@code WorkflowService.start()} to set up state would make every test depend on the
 * correctness of the thing under test - a bug in start would make queue tests fail for
 * reasons that have nothing to do with the queue. Raw inserts keep the failures
 * attributable.</p>
 */
@TestComponent
public class TestFixtures {

    private final DSLContext dsl;

    public TestFixtures(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Clears all engine state between tests.
     *
     * <p>The append-only trigger on {@code wf_events} rejects DELETE, so history must be
     * removed with TRUNCATE (which does not fire row triggers). {@code RESTART IDENTITY}
     * keeps task ids small and readable in failure output; {@code CASCADE} handles the
     * foreign keys.</p>
     */
    @Transactional
    public void truncateAll() {
        dsl.execute("TRUNCATE TABLE wf_events, wf_tasks, wf_signal_dedupe, "
                + "wf_pending_signals, wf_executions RESTART IDENTITY CASCADE");
    }

    /** Creates a RUNNING execution with no history or tasks. */
    @Transactional
    public UUID newExecution(String workflowType) {
        return newExecution(workflowType, null, "{}");
    }

    @Transactional
    public UUID newExecution(String workflowType, String businessKey, String inputJson) {
        UUID id = UUID.randomUUID();
        dsl.execute("""
                INSERT INTO wf_executions
                    (id, workflow_type, business_key, status, input, next_sequence)
                VALUES (?, ?, ?, 'RUNNING', ?, 1)
                """, id, workflowType, businessKey, inputJson.getBytes());
        return id;
    }

    /**
     * Enqueues {@code count} immediately-visible PENDING tasks.
     *
     * <p>Each gets a distinct {@code scheduled_event_seq} so they do not collide on
     * {@code uq_wf_tasks_scheduled}. Note this means WORKFLOW-kind tasks can only be
     * enqueued one at a time per execution - the one-open-decision index enforces it,
     * which is exactly the invariant {@code secondOpenDecisionTaskIsRejected} tests.</p>
     */
    @Transactional
    public void enqueuePending(UUID executionId, String queue, TaskKind kind, int count) {
        for (int i = 0; i < count; i++) {
            dsl.execute("""
                    INSERT INTO wf_tasks
                        (execution_id, shard, task_queue, kind, status,
                         scheduled_event_seq, not_before, payload)
                    VALUES (?, ?, ?, ?, 'PENDING', ?, now(), ?)
                    """, executionId, ShardAssignment.shardFor(executionId),
                    queue, kind.name(), (long) i, ("task-" + i).getBytes());
        }
    }

    /** Enqueues one task visible only at {@code notBefore} - a timer or backed-off retry. */
    @Transactional
    public void enqueuePendingAt(UUID executionId, String queue, TaskKind kind,
                                 Instant notBefore) {
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
                     not_before, payload)
                VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?)
                """, executionId, ShardAssignment.shardFor(executionId), queue,
                kind.name(), OffsetDateTime.ofInstant(notBefore, ZoneOffset.UTC),
                "timer".getBytes());
    }

    /** Enqueues one task with a pre-set attempt counter, for backoff/exhaustion tests. */
    @Transactional
    public void enqueuePendingWithAttempt(UUID executionId, String queue, TaskKind kind,
                                          int attempt) {
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
                     not_before, attempt, max_attempts, payload)
                VALUES (?, ?, ?, ?, 'PENDING', 0, now(), ?, 5, ?)
                """, executionId, ShardAssignment.shardFor(executionId), queue,
                kind.name(), attempt, "retry".getBytes());
    }

    /** Bulk-inserts COMPLETED tasks, to prove the partial index excludes dead rows. */
    @Transactional
    public void enqueueCompleted(UUID executionId, String queue, TaskKind kind, int count) {
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
                     not_before, payload)
                SELECT ?, ?, ?, ?, 'COMPLETED', g, now(), NULL
                  FROM generate_series(1000000, 1000000 + ? - 1) AS g
                """, executionId, ShardAssignment.shardFor(executionId), queue,
                kind.name(), count);
    }

    public int countByStatus(String status) {
        return dsl.fetchOne("SELECT count(*) FROM wf_tasks WHERE status = ?", status)
                .get(0, Integer.class);
    }

    public int countExecutionsByStatus(String status) {
        return dsl.fetchOne("SELECT count(*) FROM wf_executions WHERE status = ?", status)
                .get(0, Integer.class);
    }

    public int executionCount() {
        return dsl.fetchOne("SELECT count(*) FROM wf_executions").get(0, Integer.class);
    }

    public int countEvents() {
        return dsl.fetchOne("SELECT count(*) FROM wf_events").get(0, Integer.class);
    }

    public int countTasksByStatus(String status) {
        return countByStatus(status);
    }

    public String statusOf(long taskId) {
        return dsl.fetchOne("SELECT status FROM wf_tasks WHERE id = ?", taskId)
                .get(0, String.class);
    }

    public int attemptOf(long taskId) {
        return dsl.fetchOne("SELECT attempt FROM wf_tasks WHERE id = ?", taskId)
                .get(0, Integer.class);
    }

    public UUID leaseOwnerOf(long taskId) {
        return dsl.fetchOne("SELECT lease_owner FROM wf_tasks WHERE id = ?", taskId)
                .get(0, UUID.class);
    }

    public Instant notBeforeOf(long taskId) {
        return dsl.fetchOne("SELECT not_before FROM wf_tasks WHERE id = ?", taskId)
                .get(0, OffsetDateTime.class).toInstant();
    }

    public String taskStatusForExecution(UUID executionId) {
        Record r = dsl.fetchOne(
                "SELECT status FROM wf_tasks WHERE execution_id = ? ORDER BY id LIMIT 1",
                executionId);
        return r == null ? null : r.get(0, String.class);
    }

    public boolean allLeasesExpired() {
        return dsl.fetchOne("""
                SELECT count(*) FROM wf_tasks
                 WHERE status = 'LEASED' AND lease_until >= now()
                """).get(0, Integer.class) == 0;
    }

    /** @return the distinct attempt values across all tasks, for double-reclaim checks. */
    public List<Integer> distinctAttempts() {
        return dsl.fetch("SELECT DISTINCT attempt FROM wf_tasks ORDER BY attempt")
                .map(r -> r.get(0, Integer.class));
    }

    public List<String> eventTypesFor(UUID executionId) {
        return dsl.fetch("""
                SELECT event_type FROM wf_events
                 WHERE execution_id = ? ORDER BY sequence_number
                """, executionId).map(r -> r.get(0, String.class));
    }

    public List<Long> eventSequencesFor(UUID executionId) {
        return dsl.fetch("""
                SELECT sequence_number FROM wf_events
                 WHERE execution_id = ? ORDER BY sequence_number
                """, executionId).map(r -> r.get(0, Long.class));
    }

    /** Refreshes planner statistics so {@code EXPLAIN} reflects the seeded data. */
    public void analyze() {
        dsl.execute("ANALYZE wf_tasks");
    }

    /**
     * Returns the query plan for the dispatch statement.
     *
     * <p>Uses the identical SQL text as production, including the inlined status literal
     * - the whole point of the assertion is that <em>this</em> statement matches the
     * partial index, and a paraphrase would not test that.</p>
     */
    public String explainDispatch(String queue, TaskKind kind, int limit) {
        return dsl.fetch("""
                EXPLAIN
                SELECT id FROM wf_tasks
                 WHERE status = 'PENDING' AND task_queue = ? AND kind = ?
                   AND not_before <= now()
                 ORDER BY not_before, id
                 LIMIT ?
                 FOR UPDATE SKIP LOCKED
                """, queue, kind.name(), limit)
                .map(r -> r.get(0, String.class))
                .stream().reduce("", (a, b) -> a + "\n" + b);
    }

    // ---------------------------------------------------------------------------------
    // Phase 4-6 helpers.
    // ---------------------------------------------------------------------------------

    /** Inserts a LEASED activity task, as if a worker had just claimed it. */
    @Transactional
    public UUID enqueueLeasedActivity(UUID executionId, String queue) {
        UUID taskUuid = UUID.randomUUID();
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, task_uuid, shard, task_queue, kind, status,
                     scheduled_event_seq, not_before, lease_owner, lease_until, payload)
                VALUES (?, ?, ?, ?, 'ACTIVITY', 'LEASED', ?, now(), ?,
                        now() + INTERVAL '5 minutes', NULL)
                """, executionId, taskUuid, ShardAssignment.shardFor(executionId), queue,
                nextScheduledSeq(executionId), UUID.randomUUID());
        return taskUuid;
    }

    @Transactional
    public UUID enqueuePendingActivity(UUID executionId, String queue) {
        UUID taskUuid = UUID.randomUUID();
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, task_uuid, shard, task_queue, kind, status,
                     scheduled_event_seq, not_before, payload)
                VALUES (?, ?, ?, ?, 'ACTIVITY', 'PENDING', ?, now(), NULL)
                """, executionId, taskUuid, ShardAssignment.shardFor(executionId), queue,
                nextScheduledSeq(executionId));
        return taskUuid;
    }

    /** Avoids colliding on uq_wf_tasks_scheduled when a test enqueues several tasks. */
    private long nextScheduledSeq(UUID executionId) {
        return dsl.fetchOne("""
                SELECT COALESCE(max(scheduled_event_seq), 0) + 1
                  FROM wf_tasks WHERE execution_id = ?
                """, executionId).get(0, Long.class);
    }

    @Transactional
    public UUID newExecutionWithId(UUID id, String workflowType) {
        dsl.execute("""
                INSERT INTO wf_executions
                    (id, workflow_type, status, input, next_sequence)
                VALUES (?, ?, 'RUNNING', '{}'::bytea, 1)
                """, id, workflowType);
        return id;
    }

    public long versionOf(UUID executionId) {
        return dsl.fetchOne("SELECT current_version FROM wf_executions WHERE id = ?",
                executionId).get(0, Long.class);
    }

    @Transactional
    public void closeExecution(UUID executionId, String status) {
        dsl.execute("UPDATE wf_executions SET status = ?, end_time = now() WHERE id = ?",
                status, executionId);
    }

    public String executionStatus(UUID executionId) {
        return dsl.fetchOne("SELECT status FROM wf_executions WHERE id = ?", executionId)
                .get(0, String.class);
    }

    public Instant endTimeOf(UUID executionId) {
        OffsetDateTime v = dsl.fetchOne(
                "SELECT end_time FROM wf_executions WHERE id = ?", executionId)
                .get(0, OffsetDateTime.class);
        return v == null ? null : v.toInstant();
    }

    public int divergenceCountOf(UUID executionId) {
        return dsl.fetchOne(
                "SELECT divergence_count FROM wf_executions WHERE id = ?", executionId)
                .get(0, Integer.class);
    }

    public String statusOfTask(UUID taskUuid) {
        return dsl.fetchOne("SELECT status FROM wf_tasks WHERE task_uuid = ?", taskUuid)
                .get(0, String.class);
    }

    public int countTasksByKind(String kind) {
        return dsl.fetchOne("SELECT count(*) FROM wf_tasks WHERE kind = ?", kind)
                .get(0, Integer.class);
    }

    public JsonNode eventPayloadFor(UUID executionId, long seq) {
        try {
            String raw = dsl.fetchOne("""
                    SELECT payload FROM wf_events
                     WHERE execution_id = ? AND sequence_number = ?
                    """, executionId, seq).get(0, String.class);
            return new com.fasterxml.jackson.databind.ObjectMapper().readTree(raw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public JsonNode firstEventPayloadOfType(UUID executionId, String eventType) {
        try {
            String raw = dsl.fetchOne("""
                    SELECT payload FROM wf_events
                     WHERE execution_id = ? AND event_type = ?
                     ORDER BY sequence_number LIMIT 1
                    """, executionId, eventType).get(0, String.class);
            return new com.fasterxml.jackson.databind.ObjectMapper().readTree(raw);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String queueOfPendingDecision(UUID executionId) {
        return dsl.fetchOne("""
                SELECT task_queue FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'WORKFLOW' AND status = 'PENDING'
                 LIMIT 1
                """, executionId).get(0, String.class);
    }

    @Transactional
    public void clearPendingDecisions(UUID executionId) {
        dsl.execute("""
                UPDATE wf_tasks SET status = 'COMPLETED'
                 WHERE execution_id = ? AND kind = 'WORKFLOW' AND status = 'PENDING'
                """, executionId);
    }

    @Transactional
    public void insertPendingTimer(UUID executionId, Instant fireAt) {
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
                     not_before, payload, max_attempts)
                VALUES (?, ?, 'default', 'TIMER', 'PENDING', ?, ?, NULL, 1)
                """, executionId, ShardAssignment.shardFor(executionId),
                nextScheduledSeq(executionId),
                OffsetDateTime.ofInstant(fireAt, ZoneOffset.UTC));
    }

    public short shardOfTimer(UUID executionId) {
        return dsl.fetchOne("""
                SELECT shard FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'TIMER' LIMIT 1
                """, executionId).get(0, Short.class);
    }

    public Instant timerFireAt(UUID executionId) {
        return dsl.fetchOne("""
                SELECT not_before FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'TIMER' ORDER BY id DESC LIMIT 1
                """, executionId).get(0, OffsetDateTime.class).toInstant();
    }

    @Transactional
    public void truncateTasks() {
        dsl.execute("TRUNCATE TABLE wf_tasks RESTART IDENTITY CASCADE");
    }

    /** Query plan for the sharded timer poll, to guard the partial index. */
    public String explainTimerPoll(int shard) {
        return dsl.fetch("""
                EXPLAIN
                SELECT id FROM wf_tasks
                 WHERE status = 'PENDING' AND kind = 'TIMER' AND shard = ?
                   AND not_before <= now()
                 ORDER BY not_before, id LIMIT 10 FOR UPDATE SKIP LOCKED
                """, shard)
                .map(r -> r.get(0, String.class))
                .stream().reduce("", (a, b) -> a + "\n" + b);
    }

    public int countPendingSignals(String businessKey) {
        return dsl.fetchOne("SELECT count(*) FROM wf_pending_signals WHERE business_key = ?",
                businessKey).get(0, Integer.class);
    }

    public int countSignalDedupeRows(UUID executionId) {
        return dsl.fetchOne("SELECT count(*) FROM wf_signal_dedupe WHERE execution_id = ?",
                executionId).get(0, Integer.class);
    }

    /** Forces a buffered signal to look expired, so the reaper will collect it. */
    @Transactional
    public void expirePendingSignals(String businessKey) {
        dsl.execute("""
                UPDATE wf_pending_signals SET expires_at = now() - INTERVAL '1 hour'
                 WHERE business_key = ?
                """, businessKey);
    }

    // ---------------------------------------------------------------------------------
    // Phase 7 (parallel branches) helpers.
    // ---------------------------------------------------------------------------------

    /**
     * Inserts a LEASED activity with an explicit scheduled_event_seq, so fan-out branches
     * can be given the consecutive seqs a real decision commit would assign (2, 3, 4, ...).
     */
    @Transactional
    public UUID enqueueLeasedActivityAtSeq(UUID executionId, String queue, long scheduledSeq) {
        UUID taskUuid = UUID.randomUUID();
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, task_uuid, shard, task_queue, kind, status,
                     scheduled_event_seq, not_before, lease_owner, lease_until, payload)
                VALUES (?, ?, ?, ?, 'ACTIVITY', 'LEASED', ?, now(), ?,
                        now() + INTERVAL '5 minutes', NULL)
                """, executionId, taskUuid, ShardAssignment.shardFor(executionId), queue,
                scheduledSeq, UUID.randomUUID());
        return taskUuid;
    }

    public long nextSequenceOf(UUID executionId) {
        return dsl.fetchOne("SELECT next_sequence FROM wf_executions WHERE id = ?",
                executionId).get(0, Long.class);
    }

    /** Open decisions are PENDING or LEASED WORKFLOW tasks - the ones the barrier depends on. */
    public int countOpenDecisions(UUID executionId) {
        return dsl.fetchOne("""
                SELECT count(*) FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'WORKFLOW'
                   AND status IN ('PENDING','LEASED')
                """, executionId).get(0, Integer.class);
    }

    // ---------------------------------------------------------------------------------
    // Saga compensation helpers (Phase 5).
    // ---------------------------------------------------------------------------------

    /**
     * Appends a COMPENSATION_REGISTERED event at the next sequence number, as if the
     * workflow had called compensateWith after a successful step. Returns its sequence
     * number (the registrationSeq used to discharge it later).
     */
    @Transactional
    public long registerCompensation(UUID executionId, String compensationType, String inputJson) {
        long seq = dsl.fetchOne("""
                UPDATE wf_executions SET next_sequence = next_sequence + 1
                 WHERE id = ? RETURNING next_sequence - 1
                """, executionId).get(0, Long.class);
        dsl.execute("""
                INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
                VALUES (?, ?, 'COMPENSATION_REGISTERED', CAST(? AS jsonb))
                """, executionId, seq,
                "{\"identity\":\"" + compensationType + "\",\"compensationType\":\""
                        + compensationType + "\",\"input\":" + inputJson + "}");
        return seq;
    }

    /** Puts an execution directly into COMPENSATING (bypassing the forward path). */
    @Transactional
    public void setCompensating(UUID executionId) {
        dsl.execute("UPDATE wf_executions SET status = 'COMPENSATING' WHERE id = ?",
                executionId);
    }

    /**
     * Enqueues a LEASED compensation activity task carrying isCompensation=true and the
     * registrationSeq it discharges, as the engine would when driving a rollback.
     */
    @Transactional
    public UUID enqueueLeasedCompensation(UUID executionId, String queue,
                                          String compensationType, long registrationSeq) {
        UUID taskUuid = UUID.randomUUID();
        String payload = "{\"activityType\":\"" + compensationType
                + "\",\"input\":null,\"isCompensation\":true,\"registrationSeq\":"
                + registrationSeq + "}";
        dsl.execute("""
                INSERT INTO wf_tasks
                    (execution_id, task_uuid, shard, task_queue, kind, status,
                     scheduled_event_seq, not_before, lease_owner, lease_until, payload)
                VALUES (?, ?, ?, ?, 'ACTIVITY', 'LEASED', ?, now(), ?,
                        now() + INTERVAL '5 minutes', ?)
                """, executionId, taskUuid, ShardAssignment.shardFor(executionId), queue,
                registrationSeq, UUID.randomUUID(),
                payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return taskUuid;
    }

    /** Ordered list of compensation types scheduled as PENDING/LEASED activity tasks. */
    public List<String> pendingCompensationTypes(UUID executionId) {
        return dsl.fetch("""
                SELECT payload FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'ACTIVITY'
                   AND status IN ('PENDING','LEASED')
                 ORDER BY id
                """, executionId).map(r -> {
            try {
                var node = new com.fasterxml.jackson.databind.ObjectMapper()
                        .readTree(new String(r.get("payload", byte[].class),
                                java.nio.charset.StandardCharsets.UTF_8));
                return node.path("isCompensation").asBoolean(false)
                        ? node.path("activityType").asText() : null;
            } catch (Exception e) { return null; }
        }).stream().filter(java.util.Objects::nonNull).toList();
    }

    public UUID pendingCompensationTaskUuid(UUID executionId) {
        return dsl.fetchOne("""
                SELECT task_uuid FROM wf_tasks
                 WHERE execution_id = ? AND kind = 'ACTIVITY' AND status = 'PENDING'
                 ORDER BY id DESC LIMIT 1
                """, executionId).get(0, UUID.class);
    }

    @Transactional
    public UUID leasePendingCompensation(UUID executionId) {
        UUID taskUuid = pendingCompensationTaskUuid(executionId);
        dsl.execute("""
                UPDATE wf_tasks SET status = 'LEASED', lease_owner = ?,
                       lease_until = now() + INTERVAL '5 minutes'
                 WHERE task_uuid = ?
                """, UUID.randomUUID(), taskUuid);
        return taskUuid;
    }
}
