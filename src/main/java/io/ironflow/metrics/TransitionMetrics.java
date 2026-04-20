package io.ironflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * The engine's throughput and latency instrumentation, in one injectable place.
 *
 * <h2>Why a dedicated bean rather than scattered {@code registry.counter(...)} calls</h2>
 *
 * <p>A load test needs a single, stable, authoritative number for "state transitions per
 * second" to assert the documented ceiling against. Registering the counter inline at each call
 * site risks name drift and double-registration; centralizing it means the metric name is
 * defined once ({@code ironflow.transitions.total}) and every committer increments the same
 * meter. The load-test script scrapes exactly this via
 * {@code /actuator/metrics/ironflow.transitions.total} and differentiates it over the run to get
 * throughput, so the name is effectively part of the DevOps contract and belongs in one file.</p>
 *
 * <p>A "state transition" here is one durable advance of a workflow: a decision commit, an
 * activity completion, an activity failure, or a compensation step. These are the units the
 * ~5,000-10,000/sec Postgres ceiling is expressed in, because each is one serialized
 * version-bumped write against {@code wf_executions}.</p>
 */
@Component
public class TransitionMetrics {

    public static final String TRANSITIONS_COUNTER = "ironflow.transitions.total";
    public static final String DISPATCH_LATENCY_TIMER = "ironflow.task.dispatch.latency";

    private final Counter transitions;
    private final Timer dispatchLatency;

    public TransitionMetrics(MeterRegistry registry) {
        this.transitions = Counter.builder(TRANSITIONS_COUNTER)
                .description("Total durable workflow state transitions committed")
                .register(registry);
        this.dispatchLatency = Timer.builder(DISPATCH_LATENCY_TIMER)
                .description("Time from a task becoming dispatchable to a worker leasing it")
                .publishPercentiles(0.95, 0.99)
                .register(registry);
    }

    /** One durable state transition committed. Cheap: a lock-free counter increment. */
    public void recordTransition() {
        transitions.increment();
    }

    /** Record several transitions at once (e.g. a decision that committed N commands). */
    public void recordTransitions(int n) {
        if (n > 0) {
            transitions.increment(n);
        }
    }

    /** Record how long a task waited between becoming dispatchable and being leased. */
    public void recordDispatchLatency(long millis) {
        dispatchLatency.record(millis, TimeUnit.MILLISECONDS);
    }
}
