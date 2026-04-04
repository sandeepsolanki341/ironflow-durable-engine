package io.ironflow.persistence;

import io.ironflow.persistence.model.TaskKind;
import io.ironflow.persistence.repository.WfEventRepository;
import io.ironflow.persistence.repository.WfExecutionRepository;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the schema-level invariants the engine's correctness rests on.
 *
 * <p>These assertions target the <em>database</em>, not application logic. That is
 * deliberate: each of these guarantees is enforced by a constraint or index rather than
 * by careful code, and the point of the tests is to confirm that a careless caller
 * cannot violate them.</p>
 */
@SpringBootTest
@Import(TestFixtures.class)
class PersistenceLayerIT extends AbstractPostgresIT {

    @Autowired
    private DSLContext dsl;
    @Autowired
    private WfExecutionRepository executions;
    @Autowired
    private WfEventRepository events;
    @Autowired
    private TestFixtures fixtures;

    @BeforeEach
    void reset() {
        fixtures.truncateAll();
    }

    /**
     * The structural invariant: the schema, not the application, must reject a second
     * open decision task. This is what makes exactly-once decision processing impossible
     * to violate by accident - a concurrent signal cannot enqueue a duplicate.
     */
    @Test
    void secondOpenDecisionTaskIsRejectedByIndex() {
        UUID executionId = fixtures.newExecution("order-flow");
        fixtures.enqueuePending(executionId, "default", TaskKind.WORKFLOW, 1);

        assertThatThrownBy(() ->
                fixtures.enqueuePending(executionId, "default", TaskKind.WORKFLOW, 1))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("uq_wf_tasks_one_open_decision");
    }

    /**
     * The same execution may hold many open ACTIVITY tasks - the one-open constraint is
     * scoped to decisions only. Without this, parallel activity fan-out is impossible.
     */
    @Test
    void manyOpenActivityTasksAreAllowedForOneExecution() {
        UUID executionId = fixtures.newExecution("order-flow");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 25);

        assertThat(fixtures.countByStatus("PENDING")).isEqualTo(25);
    }

    /**
     * History must be immutable even against a caller that deliberately tries to rewrite
     * it. If history can change after the fact, a workflow replayed tomorrow sees a
     * different stream than it saw today, and determinism silently evaporates.
     */
    @Test
    void historyRejectsUpdateAndDelete() {
        UUID executionId = fixtures.newExecution("order-flow");
        dsl.execute("""
                INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
                VALUES (?, 1, 'WORKFLOW_STARTED', '{}'::jsonb)
                """, executionId);

        assertThatThrownBy(() -> dsl.execute(
                "UPDATE wf_events SET event_type = 'TAMPERED' WHERE execution_id = ?",
                executionId))
                .hasMessageContaining("append-only");

        assertThatThrownBy(() -> dsl.execute(
                "DELETE FROM wf_events WHERE execution_id = ?", executionId))
                .hasMessageContaining("append-only");

        assertThat(events.countByExecutionId(executionId)).isEqualTo(1);
    }

    /** Duplicate sequence numbers within one execution must be impossible. */
    @Test
    void duplicateSequenceNumberIsRejected() {
        UUID executionId = fixtures.newExecution("order-flow");
        dsl.execute("""
                INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
                VALUES (?, 1, 'WORKFLOW_STARTED', '{}'::jsonb)
                """, executionId);

        assertThatThrownBy(() -> dsl.execute("""
                INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
                VALUES (?, 1, 'DUPLICATE', '{}'::jsonb)
                """, executionId))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    /**
     * Sequence reservation must be gap-free and collision-free under contention.
     *
     * <p>A read-then-write implementation passes any sequential test and fails this one:
     * two committers both read {@code next_sequence = 5}, both write 8, and produce
     * colliding event ids.</p>
     */
    @Test
    void sequenceReservationIsGapFreeUnderContention() throws Exception {
        UUID executionId = fixtures.newExecution("order-flow");
        var allocated = new ConcurrentLinkedQueue<Long>();
        final int threads = 100;
        final int blockSize = 3;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, threads)
                    .mapToObj(i -> pool.submit(() -> {
                        long base = executions.reserveSequenceBlock(executionId, blockSize);
                        LongStream.range(base, base + blockSize).forEach(allocated::add);
                    }))
                    .toList();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        }

        List<Long> sorted = allocated.stream().sorted().toList();
        assertThat(sorted).doesNotHaveDuplicates();
        assertThat(sorted).containsExactlyElementsOf(
                LongStream.rangeClosed(1, (long) threads * blockSize).boxed().toList());
    }

    /** Business keys must be unique, while NULL keys must not collide with each other. */
    @Test
    void businessKeyIsUniqueButNullsAreUnconstrained() {
        fixtures.newExecution("order-flow", "key-1", "{}");

        assertThatThrownBy(() -> fixtures.newExecution("order-flow", "key-1", "{}"))
                .isInstanceOf(DataIntegrityViolationException.class);

        // Many NULL business keys must coexist - this is why the index is partial.
        fixtures.newExecution("order-flow", null, "{}");
        fixtures.newExecution("order-flow", null, "{}");
        assertThat(fixtures.executionCount()).isEqualTo(3);
    }

    /**
     * The end_time constraint makes an inconsistent terminal state unrepresentable: a
     * COMPLETED execution with no end time, or a RUNNING one with an end time, cannot be
     * written at all.
     */
    @Test
    void terminalStatusRequiresEndTime() {
        UUID executionId = fixtures.newExecution("order-flow");

        assertThatThrownBy(() -> dsl.execute(
                "UPDATE wf_executions SET status = 'COMPLETED' WHERE id = ?", executionId))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_wf_exec_end_time");

        assertThatThrownBy(() -> dsl.execute(
                "UPDATE wf_executions SET end_time = now() WHERE id = ?", executionId))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_wf_exec_end_time");
    }

    /**
     * A LEASED task must carry a complete lease. "Leased but no expiry" is the state that
     * silently wedges a queue forever - a task nothing can dispatch and nothing can
     * reclaim - so the schema makes it unrepresentable rather than merely unlikely.
     */
    @Test
    void leasedTaskMustCarryCompleteLease() {
        UUID executionId = fixtures.newExecution("order-flow");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 1);

        assertThatThrownBy(() -> dsl.execute(
                "UPDATE wf_tasks SET status = 'LEASED' WHERE execution_id = ?", executionId))
                .isInstanceOf(DataIntegrityViolationException.class)
                .hasMessageContaining("ck_wf_task_lease");
    }

    /** Foreign key must cascade so orphan tasks cannot outlive their execution. */
    @Test
    void deletingExecutionCascadesToTasks() {
        UUID executionId = fixtures.newExecution("order-flow");
        fixtures.enqueuePending(executionId, "default", TaskKind.ACTIVITY, 5);
        assertThat(fixtures.countByStatus("PENDING")).isEqualTo(5);

        // History blocks the cascade by design (the append-only trigger fires on the
        // cascaded DELETE), so this execution has no events to begin with.
        dsl.execute("DELETE FROM wf_executions WHERE id = ?", executionId);

        assertThat(fixtures.countByStatus("PENDING")).isZero();
    }
}
