package io.ironflow.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Scheduling infrastructure for background engine tasks.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {

    /**
     * Dedicated scheduler for the reaper.
     *
     * <p>Spring's default {@code @Scheduled} pool has a single thread shared by every
     * scheduled bean in the application. A slow unrelated job would otherwise delay lease
     * recovery - the one background task whose latency directly determines how long a
     * crashed worker's work sits stranded.</p>
     */
    @Bean
    public TaskScheduler ironflowScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("ironflow-sched-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        return scheduler;
    }
}
