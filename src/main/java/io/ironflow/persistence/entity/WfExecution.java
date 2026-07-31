package io.ironflow.persistence.entity;

import io.ironflow.persistence.model.ExecutionStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * A workflow instance.
 *
 * <p>{@code currentVersion} is mapped as a JPA {@link Version} field, so every flush of
 * a dirty execution emits {@code UPDATE ... WHERE current_version = ?} and throws
 * {@link jakarta.persistence.OptimisticLockException} on a lost update. This is the
 * concurrency control for the entire engine: two workers that both replay the same
 * decision will both attempt to advance the execution, and exactly one will win.</p>
 *
 * <p>The loser must <em>abandon</em> its decision entirely - not retry in place. Its
 * command list was computed against a history that is now stale, so retrying would
 * append events derived from a superseded view. Redelivery of the task produces a fresh
 * replay against current history, which is the correct recovery.</p>
 *
 * <p><b>Read path only.</b> The hot write path ({@code DecisionCommitter}) uses jOOQ
 * against these same columns. The two coexist because they never participate in the
 * same transaction: JPA serves the API's read queries, jOOQ serves the worker's commit.
 * Mixing them within one transaction would risk Hibernate flushing a stale entity over
 * a jOOQ write.</p>
 */
@Entity
@Table(name = "wf_executions")
public class WfExecution {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "workflow_type", nullable = false, updatable = false)
    private String workflowType;

    @Column(name = "business_key", updatable = false)
    private String businessKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private ExecutionStatus status = ExecutionStatus.RUNNING;

    @Version
    @Column(name = "current_version", nullable = false)
    private long currentVersion;

    /**
     * High-water mark for {@link WfEvent#getSequenceNumber()}.
     *
     * <p>Never read-modify-written through this field in application code. Sequence ids
     * are reserved atomically inside the decision commit via
     * {@code UPDATE ... SET next_sequence = next_sequence + ? RETURNING}, which both
     * allocates the block and takes the row lock that serializes concurrent commits.
     * Mapped read-only here so a stray entity flush cannot clobber that allocation.</p>
     */
    @Column(name = "next_sequence", nullable = false, insertable = false, updatable = false)
    private long nextSequence;

    @Column(name = "input", updatable = false)
    private byte[] input;

    @Column(name = "result")
    private byte[] result;

    @Column(name = "failure")
    private String failure;

    @Column(name = "start_time", nullable = false, updatable = false)
    private Instant startTime;

    @Column(name = "end_time")
    private Instant endTime;

    protected WfExecution() {
        // JPA
    }

    /**
     * Creates a new RUNNING execution.
     *
     * @param workflowType registered workflow type name
     * @param businessKey  optional idempotency key; {@code null} for non-idempotent starts
     * @param input        serialized workflow input
     */
    public WfExecution(String workflowType, String businessKey, byte[] input) {
        this.workflowType = Objects.requireNonNull(workflowType, "workflowType");
        this.businessKey = businessKey;
        this.input = input;
        this.status = ExecutionStatus.RUNNING;
    }

    /**
     * Transitions this execution to a terminal state, stamping {@code end_time} to
     * satisfy the {@code ck_wf_exec_end_time} constraint.
     *
     * @param terminal the terminal status to apply
     * @param result   serialized workflow result, or {@code null} for failures
     * @param failure  failure detail, or {@code null} for successes
     * @throws IllegalStateException    if this execution is already closed
     * @throws IllegalArgumentException if {@code terminal} is not a terminal status
     */
    public void close(ExecutionStatus terminal, byte[] result, String failure) {
        if (!terminal.isTerminal()) {
            throw new IllegalArgumentException("Not a terminal status: " + terminal);
        }
        if (this.status.isTerminal()) {
            throw new IllegalStateException(
                    "Execution %s already closed as %s".formatted(id, status));
        }
        this.status = terminal;
        this.result = result;
        this.failure = failure;
        this.endTime = Instant.now();
    }

    @PrePersist
    void onPersist() {
        if (startTime == null) {
            startTime = Instant.now();
        }
    }

    public UUID getId() {
        return id;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    public String getBusinessKey() {
        return businessKey;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public byte[] getInput() {
        return input;
    }

    public byte[] getResult() {
        return result;
    }

    public String getFailure() {
        return failure;
    }

    public void setFailure(String failure) {
        this.failure = failure;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    /**
     * Identity is the surrogate key only. Deliberately not using
     * {@code Objects.hash(id)} on a null id: an unpersisted entity would otherwise
     * change hash code after flush and be lost from any {@code HashSet} holding it.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WfExecution other)) {
            return false;
        }
        return id != null && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return WfExecution.class.hashCode();
    }

    @Override
    public String toString() {
        return "WfExecution[id=%s, type=%s, status=%s, version=%d]"
                .formatted(id, workflowType, status, currentVersion);
    }
}
