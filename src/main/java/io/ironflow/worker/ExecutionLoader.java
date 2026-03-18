package io.ironflow.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ironflow.api.ExecutionNotFoundException;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Loads the immutable facts a decision task needs: workflow type, input, and task queue.
 */
@Service
public class ExecutionLoader {

    private static final String LOAD_SQL = """
        SELECT e.workflow_type, e.input, e.current_version,
               COALESCE((SELECT t.task_queue FROM wf_tasks t
                          WHERE t.execution_id = e.id
                          ORDER BY t.id DESC LIMIT 1), 'default') AS task_queue
          FROM wf_executions e
         WHERE e.id = ?
        """;

    private final DSLContext dsl;
    private final ObjectMapper mapper;

    public ExecutionLoader(DSLContext dsl, ObjectMapper mapper) {
        this.dsl = dsl;
        this.mapper = mapper;
    }

    @Transactional(readOnly = true)
    public ExecutionContext load(UUID executionId) {
        Record row = dsl.fetchOne(LOAD_SQL, executionId);
        if (row == null) {
            throw new ExecutionNotFoundException(executionId);
        }
        byte[] input = row.get("input", byte[].class);
        try {
            JsonNode parsed = input == null || input.length == 0
                    ? mapper.createObjectNode()
                    : mapper.readTree(new String(input, StandardCharsets.UTF_8));
            return new ExecutionContext(
                    row.get("workflow_type", String.class),
                    parsed,
                    row.get("task_queue", String.class),
                    row.get("current_version", Long.class));
        } catch (Exception e) {
            throw new IllegalStateException("Corrupt input for execution " + executionId, e);
        }
    }

    public record ExecutionContext(String workflowType, JsonNode input,
                                   String taskQueue, long currentVersion) { }
}
