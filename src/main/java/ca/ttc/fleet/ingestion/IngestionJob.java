package ca.ttc.fleet.ingestion;

import ca.ttc.fleet.config.TtcProperties;
import ca.ttc.fleet.persistence.PositionRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class IngestionJob {
    private static final Logger log = LoggerFactory.getLogger(IngestionJob.class);
    private final FeedHttpClient client;
    private final FeedDecoder decoder;
    private final PositionRepository repository;
    private final IngestionMetrics metrics;
    private final TtcProperties properties;
    private final Clock clock;
    private final PollBackoff backoff;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Instant nextPoll = Instant.MIN;

    public IngestionJob(FeedHttpClient client, FeedDecoder decoder, PositionRepository repository,
            IngestionMetrics metrics, TtcProperties properties, Clock clock) {
        this.client = client;
        this.decoder = decoder;
        this.repository = repository;
        this.metrics = metrics;
        this.properties = properties;
        this.clock = clock;
        this.backoff = new PollBackoff(properties.pollInterval());
    }

    // Fixed-rate dispatch uses Boot's virtual-thread scheduler. Fixed-delay tasks
    // would run on its scheduler thread.
    @Scheduled(fixedRate = 1000, initialDelay = 1000)
    public void tick() {
        if (!properties.ingestionEnabled() || clock.instant().isBefore(nextPoll) || !running.compareAndSet(false, true))
            return;
        long started = System.nanoTime();
        FeedDecoder.DecodedFeed feed = null;
        int rows = 0;
        String outcome = "failure";
        Duration delay = properties.pollInterval();
        try {
            feed = decoder.decode(client.fetch(), clock.instant());
            rows = repository.ingest(feed.observations());
            metrics.success(feed, rows);
            outcome = "success";
            delay = backoff.success();
        } catch (Exception failure) {
            metrics.failure();
            delay = backoff.failure();
            if (failure instanceof InterruptedException)
                Thread.currentThread().interrupt();
            log.atWarn().addKeyValue("error_type", failure.getClass().getSimpleName())
                    .addKeyValue("retry_in_ms", delay.toMillis()).log("TTC poll failed: {}", failure.getMessage());
        } finally {
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
            metrics.duration(elapsed);
            log.atInfo().addKeyValue("feed_timestamp", feed == null ? null : feed.timestamp())
                    .addKeyValue("entity_count", feed == null ? 0 : feed.entityCount())
                    .addKeyValue("rejected_count", feed == null ? 0 : feed.rejectedCount())
                    .addKeyValue("outcome", outcome).addKeyValue("duration_ms", elapsed.toMillis())
                    .addKeyValue("rows_inserted", rows).log("TTC ingestion summary");
            nextPoll = clock.instant().plus(delay);
            running.set(false);
        }
    }
}
