package ca.ttc.fleet.ingestion;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class PollBackoffTest {
    @Test
    void growsCapsAndResets() {
        var backoff = new PollBackoff(Duration.ofSeconds(20), () -> 0.5);
        assertThat(backoff.failure()).isEqualTo(Duration.ofSeconds(30));
        assertThat(backoff.failure()).isEqualTo(Duration.ofSeconds(60));
        for (int i = 0; i < 100; i++)
            assertThat(backoff.failure()).isLessThanOrEqualTo(Duration.ofMinutes(5));
        assertThat(backoff.success()).isEqualTo(Duration.ofSeconds(20));
        assertThat(backoff.failure()).isEqualTo(Duration.ofSeconds(30));
    }
}
