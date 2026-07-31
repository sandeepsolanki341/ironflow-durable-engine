package io.ironflow.timer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.queue.ShardAssignment;
import io.ironflow.replay.EventTypes;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Fires due timers atomically.
 *
 * <h2>The atomicity requirement</h2>
 *
 * <p>Firing a timer means three things happening together: the timer row leaves PENDING, a
 * {@code TIMER_FIRED} event is appended, and a decision task is enqueued. Splitting them
 * produces exactly the failure modes the whole engine is designed to avoid:</p>
 *
 * <ul>
 *   <li><b>Marked fired, no event</b> - the workflow replays, finds a {@code TIMER_STARTED}
 *       with no outcome, and parks. But the timer row is gone, so nothing will ever fire it
 *       again. The workflow sleeps forever.</li>
 *   <li><b>Event appended, no decision task</b> - history says the timer fired but nothing
 *       wakes the workflow. Same outcome, different cause.</li>
 *   <li><b>Decision task, no event</b> - the workflow replays, still finds no timer outcome,
 *       parks again, and the decision task is consumed and re-created in a hot loop.</li>
 * </ul>
 */
@Repository
public class TimerFiringRepository {

    private static final Logger log = LoggerFactory.getLogger(TimerFiringRepository.class);

    /**
     * Claims due timers on one shard.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} even though shards are meant to be disjoint. The
     * shard assignment is convention, not enforcement - a misconfigured fleet can overlap,
     * and a rolling deploy briefly runs old and new replica-count configuration
     * simultaneously. SKIP LOCKED makes that overlap merely wasteful instead of
     * double-firing timers.</p>
     *
     * <p>Claimed by transitioning straight to COMPLETED rather than taking a lease. A timer
     * has no work to execute - there is nothing to lease it <em>for</em>. The effects all
     * happen inside this transaction, so either the whole firing commits or the row stays
     * PENDING and is retried on the next poll.</p>
     */
    private static final String CLAIM_DUE_SQL = """
        WITH due AS (
            SELECT id
              FROM wf_tasks
             WHERE status = 'PENDING'
               AND kind = 'TIMER'
               AND shard = ?
               AND not_before <= now()
             ORDER BY not_before, id
             LIMIT ?
             FOR UPDATE SKIP LOCKED
        )
        UPDATE wf_tasks t
           SET status = 'COMPLETED', updated_at = now()
          FROM due d
         WHERE t.id = d.id
        RETURNING t.id, t.execution_id, t.scheduled_event_seq, t.task_queue, t.not_before
        """;

    private static final String BUMP_VERSION_SQL = """
        UPDATE wf_executions SET current_version = current_version + 1
         WHERE id = ? AND status = 'RUNNING'
        """;

    private static final String RESERVE_SEQ_SQL = """
        UPDATE wf_executions SET next_sequence = next_sequence + ?
         WHERE id = ? RETURNING next_sequence - ?
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    private static final String ENQUEUE_DECISION_SQL = """
        INSERT INTO wf_tasks
            (execution_id, shard, task_queue, kind, status, scheduled_event_seq,
             not_before, payload, max_attempts)
        VALUES (?, ?, ?, 'WORKFLOW', 'PENDING', ?, now(), NULL, 5)
        ON CONFLICT DO NOTHING
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public TimerFiringRepository(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    /**
     * Fires up to {@code batchSize} due timers on one shard.
     *
     * <p>Batched into a single transaction. The alternative - one transaction per timer -
     * would be cleaner in isolation but costs a round trip each, and at ten thousand
     * simultaneously-due timers that is the difference between a two-second catch-up and a
     * two-minute one. The batch size bounds lock hold time so the trade stays safe.</p>
     *
     * @return number of timers fired
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public int fireDueTimers(int shard, int batchSize) {
        Result<Record> due = dsl.fetch(CLAIM_DUE_SQL, shard, batchSize);
        if (due.isEmpty()) {
            return 0;
        }

        int fired = 0;
        for (Record timer : due) {
            if (fireOne(timer)) {
                fired++;
            }
        }

        if (fired > 0 && log.isDebugEnabled()) {
            log.debug("Fired {} timer(s) on shard {}", fired, shard);
        }
        return fired;
    }

    /**
     * Applies one timer's effects.
     *
     * <p>Returns {@code false} rather than throwing when the execution is no longer RUNNING.
     * A timer outliving its execution is entirely normal - the workflow was cancelled, or
     * failed, while a long sleep was pending. The timer row is already marked COMPLETED by
     * the claim, so it simply drops with no effects, which is correct.</p>
     *
     * <p>Throwing here would abort the whole batch and roll back timers that fired fine, so
     * a single cancelled workflow would block every other timer in its shard.</p>
     */
    private boolean fireOne(Record timer) {
        UUID executionId = timer.get("execution_id", UUID.class);
        long scheduledSeq = timer.get("scheduled_event_seq", Long.class);
        String taskQueue = timer.get("task_queue", String.class);
        OffsetDateTime fireAt = timer.get("not_before", OffsetDateTime.class);

        // Lock ordering: wf_executions before wf_tasks, consistently with every other writer
        // in the engine. This is what keeps concurrent commits deadlock-free.
        if (dsl.execute(BUMP_VERSION_SQL, executionId) != 1) {
            log.debug("Timer for execution {} fired but execution is no longer RUNNING; "
                    + "dropping", executionId);
            return false;
        }

        long baseSeq = reserveSequenceBlock(executionId, 2);

        var firedPayload = mapper.createObjectNode();
        firedPayload.put("scheduledEventSeq", scheduledSeq);
        firedPayload.put("firedAt", fireAt.toInstant().toString());
        dsl.execute(APPEND_EVENT_SQL, executionId, baseSeq,
                EventTypes.TIMER_FIRED, firedPayload.toString());

        dsl.execute(APPEND_EVENT_SQL, executionId, baseSeq + 1,
                EventTypes.WORKFLOW_TASK_SCHEDULED,
                mapper.createObjectNode().put("triggeredBySeq", baseSeq).toString());

        dsl.execute(ENQUEUE_DECISION_SQL,
                executionId, ShardAssignment.shardFor(executionId), taskQueue, baseSeq + 1);

        return true;
    }

    private long reserveSequenceBlock(UUID executionId, int count) {
        Record row = dsl.fetchOne(RESERVE_SEQ_SQL, count, executionId, count);
        if (row == null) {
            throw new IllegalStateException(
                    "Execution " + executionId + " vanished mid-transaction");
        }
        return row.get(0, Long.class);
    }
}
