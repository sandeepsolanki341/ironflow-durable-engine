package io.ironflow.support;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Shared PostgreSQL container for all integration tests.
 *
 * <p>Runs against real PostgreSQL 16 rather than H2 or an embedded substitute. This is
 * non-negotiable for this codebase: {@code SKIP LOCKED} semantics, partial-index
 * planning, {@code jsonb} behaviour, plpgsql triggers and {@code RETURNING} in CTEs are
 * precisely the features a substitute database emulates differently or not at all. A
 * green test on H2 would prove nothing about production.</p>
 *
 * <p>The container is {@code static} and deliberately not annotated {@code @Container},
 * so Testcontainers' JUnit extension does not stop it between classes. One container is
 * started on first access and reused for the whole suite - starting a fresh PostgreSQL
 * per test class would add minutes to the build. Isolation comes from
 * {@link TestFixtures#truncateAll()} between tests instead.</p>
 */
@Testcontainers
@ActiveProfiles("test")
public abstract class AbstractPostgresIT {

    protected static final PostgreSQLContainer<?> POSTGRES;

    static {
        POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
                // The default max_connections (100) is too low once a 64-poller
                // contention test, HikariCP and Flyway are all competing.
                .withCommand("postgres",
                        "-c", "max_connections=300",
                        // Speeds up tests substantially and is safe here: if the
                        // container dies mid-test we discard it anyway.
                        "-c", "fsync=off",
                        "-c", "synchronous_commit=off",
                        "-c", "full_page_writes=off")
                .withReuse(false);
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 80);
    }
}
