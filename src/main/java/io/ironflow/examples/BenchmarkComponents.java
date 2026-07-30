package io.ironflow.examples;

import io.ironflow.worker.Activity;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * Registers the benchmark workflow and its no-op activities, for load testing only.
 *
 * <h2>Gated by a property, off by default</h2>
 *
 * <p>These beans exist to let the k6 load test drive real state transitions through the engine,
 * but they have no business in a production deployment - a live system should not expose a
 * workflow whose entire purpose is to churn the database as fast as possible. The whole set is
 * therefore behind {@code ironflow.benchmark.enabled}, which the load-test compose profile sets
 * to true and a real deployment leaves unset (default false).</p>
 *
 * <p>The activities are intentionally the cheapest possible work: they return their input
 * immediately. What the benchmark measures is the cost of the durable round trip - schedule the
 * activity, commit, lease it, complete it, commit again - not the activity body. That round trip
 * is the engine's true throughput unit.</p>
 */
@Configuration
@ConditionalOnProperty(name = "ironflow.benchmark.enabled", havingValue = "true")
public class BenchmarkComponents {

    @Bean
    public BenchmarkWorkflow benchmarkWorkflow() {
        return new BenchmarkWorkflow();
    }

    //@Bean
    //public BenchmarkActivities benchmarkActivities() {
      //  return new BenchmarkActivities();
    //}

    /**
     * The no-op activity implementations. Each returns its input unchanged; the value is
     * irrelevant, the durable scheduling/completion round trip is the point.
     */
    @Component
    @ConditionalOnProperty(name = "ironflow.benchmark.enabled", havingValue = "true")
    public static class BenchmarkActivities {

        @Activity("noopA")
        public int noopA(int in) {
            return in;
        }

        @Activity("noopB")
        public int noopB(int in) {
            return in;
        }

        @Activity("noopC")
        public int noopC(int in) {
            return in;
        }
    }
}
