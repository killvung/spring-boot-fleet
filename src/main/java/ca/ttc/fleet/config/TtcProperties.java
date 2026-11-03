package ca.ttc.fleet.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("ttc")
public record TtcProperties(
        @NotNull URI feedUrl,
        @NotNull Duration pollInterval,
        @NotNull Duration connectTimeout,
        @NotNull Duration readTimeout,
        @Min(1) @Max(3650) int retentionDays,
        @Min(1) @Max(1000) int maxRecords,
        @Min(1024) @Max(67108864) int maxFeedBytes,
        boolean ingestionEnabled) {

    @AssertTrue(message = "feed URL must use HTTP(S); polling/timeouts must be positive and at most 5 minutes")
    public boolean isConfigurationValid() {
        return feedUrl != null && feedUrl.getHost() != null
                && ("https".equals(feedUrl.getScheme()) || "http".equals(feedUrl.getScheme()))
                && valid(pollInterval) && valid(connectTimeout) && valid(readTimeout);
    }

    private static boolean valid(Duration value) {
        return value != null && value.toMillis() >= 1 && value.compareTo(Duration.ofMinutes(5)) <= 0;
    }
}
