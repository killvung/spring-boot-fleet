package ca.ttc.fleet.ingestion;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class IngestionMetrics {
    private final Counter successes;
    private final Counter failures;
    private final Counter inserted;
    private final DistributionSummary entities;
    private final Timer duration;
    private volatile Instant lastFeedTimestamp;

    public IngestionMetrics(MeterRegistry registry, Clock clock) {
        successes = registry.counter("ttc.ingestion.success");
        failures = registry.counter("ttc.ingestion.failure");
        inserted = registry.counter("ttc.ingestion.rows.inserted");
        entities = registry.summary("ttc.ingestion.entities");
        duration = registry.timer("ttc.ingestion.poll.duration");
        Gauge.builder("ttc.ingestion.stale.feed.age", this, metrics -> {
            Instant timestamp = metrics.lastFeedTimestamp;
            return timestamp == null ? Double.NaN
                    : Math.max(0, Duration.between(timestamp, clock.instant()).toSeconds());
        }).baseUnit("seconds").register(registry);
    }

    public void success(FeedDecoder.DecodedFeed feed, int rows) {
        successes.increment();
        inserted.increment(rows);
        entities.record(feed.entityCount());
        lastFeedTimestamp = feed.timestamp() != null ? feed.timestamp()
                : feed.observations().stream().map(p -> p.sourceTimestamp()).max(Instant::compareTo)
                        .orElse(lastFeedTimestamp);
    }

    public void failure() {
        failures.increment();
    }

    public void duration(Duration elapsed) {
        duration.record(elapsed);
    }
}
