package io.ironflow.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.replay.CorruptHistoryException;
import io.ironflow.replay.HistoryEvent;
import org.jooq.DSLContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Reads an execution's history for replay.
 *
 * <p>Ordered by sequence number, which is the replay contract: the workflow must observe
 * the exact stream it saw last time.</p>
 *
 * <p><b>This is the scaling bottleneck of the replay design.</b> Replay itself is cheap -
 * pure in-process computation over a loaded list - but this read grows linearly with
 * workflow length and happens on every decision. Two mitigations, in order of importance:
 * continue-as-new to bound history per execution, and a sticky cache keyed by execution id
 * so a worker handling consecutive decisions reads incrementally from its cached high-water
 * mark. Neither is implemented yet; see the roadmap.</p>
 */
@Service
public class HistoryReader {

    private static final String READ_HISTORY_SQL = """
        SELECT sequence_number, event_type, payload, created_at
          FROM wf_events
         WHERE execution_id = ?
         ORDER BY sequence_number ASC
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public HistoryReader(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    @Transactional(readOnly = true)
    public List<HistoryEvent> read(UUID executionId) {
        return dsl.fetch(READ_HISTORY_SQL, executionId).map(r -> {
            try {
                return new HistoryEvent(
                        r.get("sequence_number", Long.class),
                        r.get("event_type", String.class),
                        mapper.readTree(r.get("payload", String.class)),
                        r.get("created_at", OffsetDateTime.class).toInstant());
            } catch (Exception e) {
                throw new CorruptHistoryException(
                        "Cannot parse history payload for execution " + executionId, e);
            }
        });
    }
}
