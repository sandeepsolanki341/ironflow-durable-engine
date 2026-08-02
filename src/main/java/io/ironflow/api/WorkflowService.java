package io.ironflow.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.api.dto.ExecutionDetailResponse;
import io.ironflow.api.dto.ExecutionSummaryResponse;
import io.ironflow.api.dto.PageResponse;
import io.ironflow.api.dto.StartWorkflowRequest;
import io.ironflow.api.dto.StartWorkflowResponse;
import io.ironflow.persistence.model.ExecutionStatus;
import io.ironflow.persistence.model.TaskKind;
import io.ironflow.queue.PostgresTaskQueueRepository;
import io.ironflow.replay.EventTypes;
import io.ironflow.replay.WorkflowRegistry;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.jooq.exception.DataAccessException;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Transactional orchestration of execution lifecycle from the API side.
 *
 * <p>Separate from {@link WorkflowController} because a {@code @Transactional} controller
 * method holds a database connection for the full duration of request handling - including
 * serializing the response body over a slow client connection. On a large history that can
 * mean holding a pooled connection for seconds while writing bytes to a mobile client.</p>
 */
@Service
public class WorkflowService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowService.class);

    /** Sequence number of the WORKFLOW_STARTED event, which is always first. */
    private static final long STARTED_SEQ = 1L;

    private static final String INSERT_EXECUTION_SQL = """
        INSERT INTO wf_executions
            (id, workflow_type, business_key, status, input, input_fingerprint,
             next_sequence)
        VALUES (?, ?, ?, 'RUNNING', ?, ?, ?)
        """;

    private static final String APPEND_EVENT_SQL = """
        INSERT INTO wf_events (execution_id, sequence_number, event_type, payload)
        VALUES (?, ?, ?, CAST(? AS jsonb))
        """;

    private static final String SELECT_EXECUTION_SQL = """
        SELECT id, workflow_type, business_key, status, current_version,
               start_time, end_time, result, failure,
               divergence_detail, divergence_count
          FROM wf_executions
         WHERE id = ?
        """;

    private static final String SELECT_BY_BUSINESS_KEY_SQL = """
        SELECT id, workflow_type, status, start_time, input_fingerprint
          FROM wf_executions
         WHERE business_key = ?
        """;

    private static final String SELECT_HISTORY_AFTER_SQL = """
        SELECT sequence_number, event_type, payload, created_at
          FROM wf_events
         WHERE execution_id = ? AND sequence_number > ?
         ORDER BY sequence_number ASC
        """;

    private static final String SELECT_HISTORY_SQL = """
        SELECT sequence_number, event_type, payload, created_at
          FROM wf_events
         WHERE execution_id = ?
         ORDER BY sequence_number ASC
        """;

    // ---------------------------------------------------------------------------------
    // Executions list (dashboard). Both queries share the same WHERE clause so the count
    // and the page can never disagree about what "matches". The filter predicates use
    // (? IS NULL OR col = ?) so a single prepared statement serves every combination of
    // optional filters - no dynamic SQL assembly, no injection surface.
    //
    // business_key search is a case-insensitive prefix match (col ILIKE ? || '%'), which
    // an index on lower(business_key) can serve; a leading-wildcard contains-match could
    // not, and this is an operator typing a known key, not full-text search.
    // ---------------------------------------------------------------------------------

    private static final String LIST_WHERE = """
             (CAST(? AS TEXT) IS NULL OR status = ?)
         AND (CAST(? AS TEXT) IS NULL OR business_key ILIKE ? || '%')
        """;

    private static final String COUNT_EXECUTIONS_SQL =
            "SELECT count(*) AS n FROM wf_executions WHERE" + LIST_WHERE;

    private static final String LIST_EXECUTIONS_SQL =
            """
            SELECT id, workflow_type, business_key, status, start_time, end_time
              FROM wf_executions
             WHERE""" + LIST_WHERE + """
             ORDER BY start_time DESC, id DESC
             LIMIT ? OFFSET ?
            """;

    private static final String DRAIN_PENDING_SIGNALS_SQL = """
        DELETE FROM wf_pending_signals
         WHERE business_key = ?
        RETURNING signal_name, signal_id, payload
        """;

    private static final String BUMP_NEXT_SEQ_SQL = """
        UPDATE wf_executions SET next_sequence = next_sequence + ? WHERE id = ?
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;
    private final WorkflowRegistry registry;
    private final PostgresTaskQueueRepository queue;

    public WorkflowService(DSLContext dsl, ObjectMapper mapper,
                           WorkflowRegistry registry,
                           PostgresTaskQueueRepository queue) {
        this.dsl = dsl;
        this.mapper = mapper;
        this.registry = registry;
        this.queue = queue;
    }

    /**
     * Creates an execution, appends {@code WORKFLOW_STARTED}, drains any buffered signals,
     * and enqueues the first decision task - atomically.
     *
     * <p>This atomicity is the entire justification for a database-backed queue. With an
     * external broker, a crash between the row insert and the publish would leave a
     * durably-started workflow that nothing will ever advance, and no retry can fix it
     * because the record saying "this needs a task" is the record that already committed.</p>
     *
     * <p>Idempotency is enforced by the database, not by a check-then-act read. Two
     * concurrent starts with the same business key will both pass a preliminary SELECT; only
     * the unique index reliably rejects the loser. So we attempt the insert and catch the
     * violation.</p>
     *
     * @throws UnknownWorkflowTypeException  if the type is not registered - checked eagerly
     *         so a typo fails at the API boundary with a 400 rather than creating an
     *         execution that sits RUNNING forever because no worker can resolve it
     * @throws IdempotencyConflictException  if the business key exists with different input
     */
    @Transactional
    public StartWorkflowResponse start(StartWorkflowRequest request) {
        if (!registry.isRegistered(request.workflowType())) {
            throw new UnknownWorkflowTypeException(
                    request.workflowType(), registry.registeredTypes());
        }

        JsonNode input = request.input() == null ? mapper.createObjectNode() : request.input();
        byte[] inputBytes = input.toString().getBytes(StandardCharsets.UTF_8);
        String fingerprint = IdempotencyFingerprint.of(request.workflowType(), input);
        UUID executionId = UUID.randomUUID();

                try {
            dsl.execute(INSERT_EXECUTION_SQL,
                    executionId, request.workflowType(), request.businessKey(),
                    inputBytes, fingerprint,
                    // next_sequence starts at 1; WORKFLOW_STARTED consumes it, so the row
                    // is created already advanced to 2.
                    STARTED_SEQ + 1);
        } catch (DuplicateKeyException | DataAccessException e) {
            return resolveExistingStart(request, fingerprint, e);
        }

        var startedPayload = mapper.createObjectNode()
                .put("workflowType", request.workflowType())
                .put("taskQueue", request.taskQueueOrDefault());
        startedPayload.set("input", input);
        dsl.execute(APPEND_EVENT_SQL, executionId, STARTED_SEQ,
                EventTypes.WORKFLOW_STARTED, startedPayload.toString());

        // Drain signals that arrived before this execution existed. Same transaction, so a
        // signal can never be lost between the buffer and history.
        if (request.businessKey() != null) {
            drainPendingSignals(executionId, request.businessKey());
        }

        queue.enqueue(executionId, request.taskQueueOrDefault(), TaskKind.WORKFLOW,
                STARTED_SEQ, Instant.now(), inputBytes, 5);

        return new StartWorkflowResponse(executionId, request.workflowType(),
                ExecutionStatus.RUNNING, Instant.now(), false);
    }

    /**
     * Moves buffered signals into history as part of the start transaction.
     *
     * @return the number of signals drained
     */
    private int drainPendingSignals(UUID executionId, String businessKey) {
        Result<Record> buffered = dsl.fetch(DRAIN_PENDING_SIGNALS_SQL, businessKey);
        if (buffered.isEmpty()) {
            return 0;
        }

        // Reserve a block covering every buffered signal, starting after WORKFLOW_STARTED.
        long seq = STARTED_SEQ + 1;
        dsl.execute(BUMP_NEXT_SEQ_SQL, buffered.size(), executionId);

        for (Record signal : buffered) {
            var payload = mapper.createObjectNode();
            payload.put("signalName", signal.get("signal_name", String.class));
            try {
                payload.set("payload",
                        mapper.readTree(signal.get("payload", String.class)));
            } catch (Exception e) {
                payload.set("payload", mapper.nullNode());
            }
            String signalId = signal.get("signal_id", String.class);
            if (signalId != null) {
                payload.put("signalId", signalId);
            }
            dsl.execute(APPEND_EVENT_SQL, executionId, seq++,
                    EventTypes.SIGNAL_RECEIVED, payload.toString());
        }

        log.info("Drained {} buffered signal(s) into new execution {}",
                buffered.size(), executionId);
        return buffered.size();
    }

    /**
     * Resolves a start that lost the uniqueness race.
     *
     * <p>Distinguishes a legitimate retry from a key collision by comparing fingerprints.
     * This is the whole point of storing one.</p>
     *
     * @throws IllegalStateException if the constraint that fired was not the business key
     *         index. A UUID primary key collision would mean something is very wrong - a
     *         broken random source, most likely - and must not be silently reported to the
     *         caller as "already existed".
     */
     private StartWorkflowResponse resolveExistingStart(StartWorkflowRequest request,
                                                      String fingerprint,
                                                      Throwable cause) {
        if (request.businessKey() == null) {
            throw new IllegalStateException(
                    "Duplicate key violation with no business key set", cause);
        }

        Record existing = dsl.fetchOne(SELECT_BY_BUSINESS_KEY_SQL, request.businessKey());
        if (existing == null) {
            throw new IllegalStateException(
                    "Duplicate on business_key=%s but no row found"
                            .formatted(request.businessKey()), cause);
        }

        String stored = existing.get("input_fingerprint", String.class);
        if (stored != null && !stored.equals(fingerprint)) {
            throw new IdempotencyConflictException(
                    request.businessKey(),
                    existing.get("id", UUID.class),
                    existing.get("workflow_type", String.class),
                    request.workflowType());
        }

        return new StartWorkflowResponse(
                existing.get("id", UUID.class),
                existing.get("workflow_type", String.class),
                ExecutionStatus.valueOf(existing.get("status", String.class)),
                existing.get("start_time", OffsetDateTime.class).toInstant(),
                true);
    }

    /**
     * Returns an execution with, optionally, its full event history.
     *
     * @param includeHistory {@code false} for a status-only poll
     * @throws ExecutionNotFoundException if the id does not resolve
     */
    @Transactional(readOnly = true)
    /**
     * Returns a filtered, paginated page of executions for the dashboard list.
     *
     * <p>Both the count and the page run the identical filter, so the reported total always
     * matches the rows the client can actually page through. Ordering is newest-first by
     * start time, with the id as a deterministic tiebreaker so pages never overlap or skip a
     * row when two executions share a start instant.</p>
     *
     * @param status      optional exact status filter; null returns all statuses
     * @param businessKey optional case-insensitive business-key prefix; null returns all
     * @param page        zero-based page index (negative is clamped to 0)
     * @param size        page size (clamped to 1..200 to bound the response)
     */
    public PageResponse<ExecutionSummaryResponse> list(
            String status, String businessKey, int page, int size) {

        int safeSize = Math.min(200, Math.max(1, size));
        int safePage = Math.max(0, page);
        long offset = (long) safePage * safeSize;

        long total = dsl.fetchOne(COUNT_EXECUTIONS_SQL,
                status, status, businessKey, businessKey).get("n", Long.class);

        List<ExecutionSummaryResponse> items = new ArrayList<>();
        if (total > offset) {
            Result<Record> rows = dsl.fetch(LIST_EXECUTIONS_SQL,
                    status, status, businessKey, businessKey, safeSize, offset);
            for (Record r : rows) {
                OffsetDateTime end = r.get("end_time", OffsetDateTime.class);
                items.add(new ExecutionSummaryResponse(
                        r.get("id", UUID.class),
                        r.get("workflow_type", String.class),
                        r.get("business_key", String.class),
                        ExecutionStatus.valueOf(r.get("status", String.class)),
                        r.get("start_time", OffsetDateTime.class).toInstant(),
                        end == null ? null : end.toInstant()));
            }
        }
        return PageResponse.of(items, safePage, safeSize, total);
    }

    public ExecutionDetailResponse findById(UUID id, boolean includeHistory) {
        Record exec = dsl.fetchOne(SELECT_EXECUTION_SQL, id);
        if (exec == null) {
            throw new ExecutionNotFoundException(id);
        }

        List<ExecutionDetailResponse.EventView> history = List.of();
        if (includeHistory) {
            history = dsl.fetch(SELECT_HISTORY_SQL, id)
                    .map(r -> new ExecutionDetailResponse.EventView(
                            r.get("sequence_number", Long.class),
                            r.get("event_type", String.class),
                            readJson(r.get("payload", String.class)),
                            r.get("created_at", OffsetDateTime.class).toInstant()));
        }

        byte[] result = exec.get("result", byte[].class);
        OffsetDateTime endTime = exec.get("end_time", OffsetDateTime.class);

        return new ExecutionDetailResponse(
                exec.get("id", UUID.class),
                exec.get("workflow_type", String.class),
                exec.get("business_key", String.class),
                ExecutionStatus.valueOf(exec.get("status", String.class)),
                exec.get("current_version", Long.class),
                exec.get("start_time", OffsetDateTime.class).toInstant(),
                endTime == null ? null : endTime.toInstant(),
                result == null ? null : readJson(new String(result, StandardCharsets.UTF_8)),
                exec.get("failure", String.class),
                exec.get("divergence_detail", String.class),
                history);
    }

    /**
     * Reads history events with a sequence number strictly greater than {@code afterSeq}.
     *
     * <p>The SSE stream uses this to push only what is new: a notification says "execution X
     * advanced to seq N", and the stream sends the events between what the client last saw and
     * N, rather than re-sending the whole history on every append. {@code afterSeq = 0} returns
     * the entire history, which is exactly what a freshly-connected client needs as its
     * initial snapshot.</p>
     */
    public List<ExecutionDetailResponse.EventView> historyAfter(UUID id, long afterSeq) {
        return dsl.fetch(SELECT_HISTORY_AFTER_SQL, id, afterSeq)
                .map(r -> new ExecutionDetailResponse.EventView(
                        r.get("sequence_number", Long.class),
                        r.get("event_type", String.class),
                        readJson(r.get("payload", String.class)),
                        r.get("created_at", OffsetDateTime.class).toInstant()));
    }

    private JsonNode readJson(String raw) {
        try {
            return mapper.readTree(raw);
        } catch (Exception e) {
            // Unreachable unless the column was written by something other than this
            // engine. Fail loudly rather than returning a plausible-looking null.
            throw new IllegalStateException("Corrupt JSON payload in history", e);
        }
    }
}
