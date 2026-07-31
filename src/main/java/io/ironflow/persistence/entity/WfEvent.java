package io.ironflow.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * An immutable history event.
 *
 * <p>Deliberately has no setters and no {@code @Version}. History is append-only; the
 * database trigger {@code trg_wf_events_immutable} enforces this even against a buggy
 * caller, and {@link Immutable} means Hibernate skips dirty-checking entirely and can
 * never generate an UPDATE for this table.</p>
 *
 * <p>Belt and braces is warranted here. History is the source of truth for replay: if
 * an event can be rewritten after the fact, a workflow replayed tomorrow observes a
 * different stream than it did today, and determinism - the property the whole engine
 * rests on - silently evaporates. A bug that corrupts history is not recoverable by
 * retry, because there is nothing left to recover from.</p>
 */
@Entity
@Table(name = "wf_events")
@Immutable
public class WfEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", updatable = false, nullable = false)
    private Long id;

    @Column(name = "execution_id", nullable = false, updatable = false)
    private UUID executionId;

    @Column(name = "sequence_number", nullable = false, updatable = false)
    private long sequenceNumber;

    @Column(name = "event_type", nullable = false, updatable = false, length = 64)
    private String eventType;

    /**
     * Event attributes. Mapped to {@code jsonb} via {@link SqlTypes#JSON}; Hibernate 6
     * handles this natively with no third-party type contributor.
     */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false, columnDefinition = "jsonb")
    private String payload;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected WfEvent() {
        // JPA
    }

    /**
     * @param executionId    owning execution
     * @param sequenceNumber position in the execution's history; must come from a
     *                       reserved block, never from a client-side counter
     * @param eventType      event discriminator, e.g. {@code WORKFLOW_STARTED}
     * @param payload        serialized JSON attributes
     */
    public WfEvent(UUID executionId, long sequenceNumber, String eventType, String payload) {
        this.executionId = executionId;
        this.sequenceNumber = sequenceNumber;
        this.eventType = eventType;
        this.payload = payload;
        this.createdAt = Instant.now();
    }

    public Long getId() {
        return id;
    }

    public UUID getExecutionId() {
        return executionId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WfEvent other)) {
            return false;
        }
        return id != null && id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return WfEvent.class.hashCode();
    }

    @Override
    public String toString() {
        return "WfEvent[execution=%s, seq=%d, type=%s]"
                .formatted(executionId, sequenceNumber, eventType);
    }
}
