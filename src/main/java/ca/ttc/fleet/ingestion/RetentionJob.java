package ca.ttc.fleet.ingestion;

import ca.ttc.fleet.config.TtcProperties;
import ca.ttc.fleet.persistence.PositionRepository;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class RetentionJob {
    private static final Logger log = LoggerFactory.getLogger(RetentionJob.class);
    private final PositionRepository repository;
    private final TtcProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean();

    public RetentionJob(PositionRepository repository, TtcProperties properties, Clock clock) {
        this.repository = repository;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(cron = "${ttc.cleanup-cron}", zone = "UTC")
    public void clean() {
        if (!running.compareAndSet(false, true))
            return;
        long deleted = 0;
        try {
            var cutoff = clock.instant().minus(properties.retentionDays(), ChronoUnit.DAYS);
            int batch;
            do {
                batch = repository.deleteHistoryBatch(cutoff, 10_000);
                deleted += batch;
            } while (batch == 10_000 && !Thread.currentThread().isInterrupted());
            log.atInfo().addKeyValue("cutoff", cutoff).addKeyValue("rows_deleted", deleted)
                    .log("History retention complete");
        } catch (Exception failure) {
            log.warn("History retention failed after {} deletes: {}", deleted, failure.getMessage());
        } finally {
            running.set(false);
        }
    }
}
