package ca.ttc.fleet.ingestion;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import ca.ttc.fleet.persistence.PositionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class IngestionJobTest {
    @Test
    void failedPollNeverWritesAndDoesNotImmediatelyRetry() throws Exception {
        var client = mock(FeedHttpClient.class);
        var repository = mock(PositionRepository.class);
        var registry = new SimpleMeterRegistry();
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        when(client.fetch()).thenThrow(new IOException("503"));
        var job = new IngestionJob(client, new FeedDecoder(), repository,
                new IngestionMetrics(registry, clock), properties(), clock);
        job.tick();
        job.tick();
        verify(client).fetch();
        verifyNoInteractions(repository);
        assertThat(registry.get("ttc.ingestion.failure").counter().count()).isEqualTo(1);
    }

    @Test
    void databaseFailureIsCountedAsFailureAndSuccessfulPollReportsCommittedRows() throws Exception {
        var client = mock(FeedHttpClient.class);
        var repository = mock(PositionRepository.class);
        var registry = new SimpleMeterRegistry();
        var clock = Clock.fixed(NOW, ZoneOffset.UTC);
        when(client.fetch()).thenReturn(feed().addEntity(entity("1")).build().toByteArray());
        when(repository.ingest(anyList())).thenThrow(new IllegalStateException("rollback")).thenReturn(1);
        var metrics = new IngestionMetrics(registry, clock);
        new IngestionJob(client, new FeedDecoder(), repository, metrics, properties(), clock).tick();
        new IngestionJob(client, new FeedDecoder(), repository, metrics, properties(), clock).tick();
        assertThat(registry.get("ttc.ingestion.failure").counter().count()).isEqualTo(1);
        assertThat(registry.get("ttc.ingestion.success").counter().count()).isEqualTo(1);
        assertThat(registry.get("ttc.ingestion.rows.inserted").counter().count()).isEqualTo(1);
    }
}
