package ca.ttc.fleet.config;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import java.time.Duration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class MetricsConfiguration {
    @Bean
    @ConditionalOnProperty(name = "ttc.metrics-logging-enabled", matchIfMissing = true)
    LoggingMeterRegistry loggingMeterRegistry() {
        var config = new LoggingRegistryConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofMinutes(1);
            }
        };
        return new LoggingMeterRegistry(config, Clock.SYSTEM);
    }
}
