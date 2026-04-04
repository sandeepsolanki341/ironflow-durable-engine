package io.ironflow.e2e;

import io.ironflow.api.dto.ExecutionDetailResponse;
import io.ironflow.api.dto.StartWorkflowResponse;
import io.ironflow.persistence.model.ExecutionStatus;
import io.ironflow.support.AbstractPostgresIT;
import io.ironflow.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proves the walking skeleton: HTTP start -> queue -> worker -> history -> terminal
 * state, with the real worker loop running and real PostgreSQL underneath.
 *
 * <p>Unlike the queue and persistence suites, this one enables the {@link
 * io.ironflow.worker.WorkerPoller}: the point is precisely that nobody has to drive the
 * engine manually.</p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestFixtures.class)
class WalkingSkeletonIT extends AbstractPostgresIT {

    @DynamicPropertySource
    static void workerProperties(DynamicPropertyRegistry registry) {
        // The whole point of this suite is that the engine runs itself.
        registry.add("ironflow.worker.enabled", () -> true);
        registry.add("ironflow.worker.max-concurrency", () -> 16);
    }

    @Autowired
    private TestRestTemplate http;
    @Autowired
    private TestFixtures fixtures;

    @BeforeEach
    void reset() {
        fixtures.truncateAll();
    }

    // ---------------------------------------------------------------------------------
    // The headline path.
    // ---------------------------------------------------------------------------------

    /** Start over HTTP, and the workflow reaches COMPLETED on its own. */
    @Test
    void startedWorkflowRunsToCompletionWithFullHistory() {
        ResponseEntity<StartWorkflowResponse> started = http.postForEntity(
                "/api/v1/workflows/start",
                Map.of("workflowType", "OrderFulfillment",
                        "input", Map.of("orderId", "ORD-1001")),
                StartWorkflowResponse.class);

        assertThat(started.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(started.getHeaders().getFirst(HttpHeaders.LOCATION)).isNotBlank();

        StartWorkflowResponse body = started.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(body.alreadyExisted()).isFalse();

        awaitStatus(body.executionId(), ExecutionStatus.COMPLETED);
        ExecutionDetailResponse detail = getExecution(body.executionId());

        assertThat(detail.history())
                .extracting(ExecutionDetailResponse.EventView::eventType)
                .containsExactly(
                        "WORKFLOW_STARTED",
                        "STEP_COMPLETED", "STEP_COMPLETED", "STEP_COMPLETED",
                        "WORKFLOW_COMPLETED");

        assertThat(detail.history())
                .extracting(ExecutionDetailResponse.EventView::sequenceNumber)
                .as("history must be gap-free and strictly ordered")
                .containsExactly(1L, 2L, 3L, 4L, 5L);

        assertThat(detail.result().path("outcome").asText()).isEqualTo("FULFILLED");
        assertThat(detail.endTime()).isNotNull();
        assertThat(detail.currentVersion()).isGreaterThan(0);
        assertThat(fixtures.taskStatusForExecution(body.executionId()))
                .as("the decision task must be acked, not left leased")
                .isEqualTo("COMPLETED");
    }

    /** Business keys must make start idempotent under client retry. */
    @Test
    void duplicateBusinessKeyReturnsExistingExecution() {
        Map<String, Object> request = Map.of(
                "workflowType", "OrderFulfillment",
                "input", Map.of("orderId", "ORD-2002"),
                "businessKey", "order-2002");

        ResponseEntity<StartWorkflowResponse> first = http.postForEntity(
                "/api/v1/workflows/start", request, StartWorkflowResponse.class);
        ResponseEntity<StartWorkflowResponse> second = http.postForEntity(
                "/api/v1/workflows/start", request, StartWorkflowResponse.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().executionId()).isEqualTo(first.getBody().executionId());
        assertThat(second.getBody().alreadyExisted()).isTrue();
        assertThat(fixtures.executionCount()).isEqualTo(1);
    }

    /**
     * The race the unique index exists for. A check-then-act implementation passes the
     * sequential test above and fails this one, which is exactly why the production code
     * attempts the insert and catches the violation rather than looking first.
     */
    @Test
    void concurrentStartsWithSameBusinessKeyCreateOneExecution() throws Exception {
        final int callers = 16;
        var ids = new ConcurrentLinkedQueue<UUID>();
        var barrier = new CyclicBarrier(callers);

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, callers)
                    .mapToObj(i -> pool.submit(() -> {
                        barrier.await(30, TimeUnit.SECONDS);
                        ResponseEntity<StartWorkflowResponse> r = http.postForEntity(
                                "/api/v1/workflows/start",
                                Map.of("workflowType", "OrderFulfillment",
                                        "input", Map.of("orderId", "ORD-RACE"),
                                        "businessKey", "order-race"),
                                StartWorkflowResponse.class);
                        if (r.getBody() != null && r.getBody().executionId() != null) {
                            ids.add(r.getBody().executionId());
                        }
                        return null;
                    }))
                    .toList();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        }

        assertThat(new HashSet<>(ids)).as("all callers must see one execution").hasSize(1);
        assertThat(fixtures.executionCount()).isEqualTo(1);
    }

    /**
     * Many concurrent workflows must all complete - proves the virtual-thread fan-out
     * and the semaphore backpressure hold up together.
     */
    @Test
    void manyConcurrentWorkflowsAllComplete() throws Exception {
        final int count = 200;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = IntStream.range(0, count)
                    .mapToObj(i -> pool.submit(() -> http.postForEntity(
                            "/api/v1/workflows/start",
                            Map.of("workflowType", "OrderFulfillment",
                                    "input", Map.of("orderId", "ORD-" + i)),
                            StartWorkflowResponse.class)))
                    .toList();
            for (Future<?> f : futures) {
                f.get(120, TimeUnit.SECONDS);
            }
        }

        await().atMost(Duration.ofSeconds(180)).pollInterval(Duration.ofSeconds(1))
                .untilAsserted(() -> assertThat(
                        fixtures.countExecutionsByStatus("COMPLETED")).isEqualTo(count));

        assertThat(fixtures.countTasksByStatus("PENDING")).isZero();
        assertThat(fixtures.countTasksByStatus("LEASED")).isZero();
        assertThat(fixtures.countEvents())
                .as("5 events per workflow: started + 3 steps + completed")
                .isEqualTo(count * 5);
    }

    /**
     * A throwing workflow body terminates the execution rather than hanging, and the
     * partial progress that did happen is still recorded.
     */
    @Test
    void failingWorkflowTerminatesAsFailed() {
        StartWorkflowResponse started = startWorkflow("AlwaysFails", Map.of());
        awaitStatus(started.executionId(), ExecutionStatus.FAILED);

        ExecutionDetailResponse detail = getExecution(started.executionId());
        assertThat(detail.failure()).isNotBlank();
        assertThat(detail.history())
                .extracting(ExecutionDetailResponse.EventView::eventType)
                .containsExactly("WORKFLOW_STARTED", "STEP_COMPLETED", "WORKFLOW_FAILED");
        assertThat(detail.endTime()).isNotNull();
        assertThat(fixtures.taskStatusForExecution(started.executionId()))
                .as("failed workflow must still ack its decision task, not loop forever")
                .isEqualTo("COMPLETED");
    }

    // ---------------------------------------------------------------------------------
    // API contract.
    // ---------------------------------------------------------------------------------

    @Test
    void unknownWorkflowTypeIsRejectedAtApiBoundary() {
        ResponseEntity<String> response = http.postForEntity(
                "/api/v1/workflows/start",
                Map.of("workflowType", "NoSuchWorkflow", "input", Map.of()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).contains("Unknown workflow type");
        assertThat(fixtures.executionCount())
                .as("a rejected start must not leave an orphan execution")
                .isZero();
    }

    @Test
    void blankWorkflowTypeIsRejected() {
        ResponseEntity<String> response = http.postForEntity(
                "/api/v1/workflows/start",
                Map.of("workflowType", "", "input", Map.of()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(fixtures.executionCount()).isZero();
    }

    @Test
    void unknownExecutionReturns404() {
        ResponseEntity<String> response = http.getForEntity(
                "/api/v1/workflows/{id}", String.class, UUID.randomUUID());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void historyCanBeOmittedForStatusPolling() {
        StartWorkflowResponse started =
                startWorkflow("OrderFulfillment", Map.of("orderId", "ORD-3003"));
        awaitStatus(started.executionId(), ExecutionStatus.COMPLETED);

        ResponseEntity<ExecutionDetailResponse> response = http.getForEntity(
                "/api/v1/workflows/{id}?includeHistory=false",
                ExecutionDetailResponse.class, started.executionId());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().status()).isEqualTo(ExecutionStatus.COMPLETED);
        assertThat(response.getBody().history()).isEmpty();
    }

    // ---------------------------------------------------------------------------------
    // Helpers.
    // ---------------------------------------------------------------------------------

    private StartWorkflowResponse startWorkflow(String type, Map<String, Object> input) {
        StartWorkflowResponse body = http.postForEntity(
                "/api/v1/workflows/start",
                Map.of("workflowType", type, "input", input),
                StartWorkflowResponse.class).getBody();
        assertThat(body).isNotNull();
        return body;
    }

    private ExecutionDetailResponse getExecution(UUID id) {
        ExecutionDetailResponse body = http.getForEntity(
                "/api/v1/workflows/{id}", ExecutionDetailResponse.class, id).getBody();
        assertThat(body).isNotNull();
        return body;
    }

    private void awaitStatus(UUID id, ExecutionStatus expected) {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() ->
                        assertThat(getExecution(id).status()).isEqualTo(expected));
    }
}
