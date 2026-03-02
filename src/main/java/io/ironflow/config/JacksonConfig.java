package io.ironflow.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * JSON configuration.
 *
 * <p>{@code WRITE_DATES_AS_TIMESTAMPS} is disabled so {@link java.time.Instant} fields
 * serialize as ISO-8601 strings rather than epoch decimals. For an engine whose entire
 * value proposition is durable, inspectable history, timestamps a human can read in a
 * response body are worth the handful of extra bytes.</p>
 */
@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
