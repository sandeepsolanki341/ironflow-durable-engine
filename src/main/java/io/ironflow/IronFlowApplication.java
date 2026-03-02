package io.ironflow;

import io.ironflow.queue.ReaperProperties;
import io.ironflow.timer.TimerPollerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * IronFlow - an open-source durable execution engine backed entirely by PostgreSQL.
 *
 * <p>No Kafka, no Redis, no RabbitMQ. Queue, timers, history and state all live in one
 * database so that every state transition can be a single ACID transaction.</p>
 */
@SpringBootApplication
@EnableConfigurationProperties({ ReaperProperties.class, TimerPollerProperties.class })
public class IronFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(IronFlowApplication.class, args);
    }
}
