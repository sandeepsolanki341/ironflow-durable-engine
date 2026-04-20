package io.ironflow.examples;

import io.ironflow.sdk.Workflow;
import io.ironflow.sdk.WorkflowContext;

/**
 * A deliberately trivial workflow whose only purpose is to generate durable state transitions
 * as fast as the engine can commit them, for load testing.
 *
 * <h2>Why a dedicated benchmark workflow</h2>
 *
 * <p>The flagship {@link OrderFulfillmentWorkflow} sleeps for three days and calls activities
 * that need real implementations - it is the wrong thing to load-test, because it measures the
 * timer and the activity workers, not the engine's transition ceiling. This workflow instead
 * executes a short fixed sequence of no-op activities with no sleeps and no external
 * dependencies, so each execution turns into a predictable handful of decision commits and
 * activity completions - exactly the version-bumped writes the ~5k-10k/sec Postgres ceiling is
 * about. Throughput measured here is the engine's, not a downstream service's.</p>
 *
 * <p>The activities ({@code noopA}, {@code noopB}, {@code noopC}) must be registered as trivial
 * pass-throughs in the worker running under load. Each returns immediately; the point is the
 * scheduling/completion round trip through Postgres, not the work.</p>
 */
public final class BenchmarkWorkflow implements Workflow<BenchmarkWorkflow.Input, Integer> {

    /** Number of sequential activities; keep small so each execution is a quick burst. */
    private static final int STEPS = 3;

    public record Input(String label) {}

    @Override
    public String type() {
        return "Benchmark";
    }

    @Override
    public Class<Input> inputType() {
        return Input.class;
    }

    @Override
    public Integer run(Input input, WorkflowContext ctx) throws Exception {
        int acc = 0;
        // A short sequential chain. Each activity is one schedule + one completion = durable
        // transitions, driving the queue and the commit path without any real work or waiting.
        for (int i = 0; i < STEPS; i++) {
            Integer r = ctx.activity("noop" + (char) ('A' + i), i, Integer.class);
            acc += r == null ? 0 : r;
        }
        return acc;
    }
}
