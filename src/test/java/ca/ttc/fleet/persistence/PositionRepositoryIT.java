package ca.ttc.fleet.persistence;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;

import ca.ttc.fleet.PostgresIntegrationSupport;
import ca.ttc.fleet.domain.Observation;
import ca.ttc.fleet.domain.PositionFilter;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

class PositionRepositoryIT extends PostgresIntegrationSupport {
    @Autowired
    PositionRepository repository;
    @Autowired
    JdbcClient jdbc;
    private final PositionFilter all = new PositionFilter(null, null, null, null, null, null, null, 1000);

    @BeforeEach
    void clear() {
        jdbc.sql("TRUNCATE vehicle_positions, latest_vehicle_positions").update();
    }

    @Test
    void appendsHistoryAndUpsertsLatestWithoutRewritingHistory() {
        var old = position("1", "501", 20);
        var newer = position("1", "504", 10);
        assertThat(repository.ingest(List.of(old))).isEqualTo(1);
        assertThat(repository.ingest(List.of(newer, old, newer))).isEqualTo(1);
        assertThat(repository.ingest(List.of(old, newer))).isZero();
        assertThat(repository.latest(all)).containsExactly(newer);
        assertThat(repository.history(all)).containsExactly(newer, old);
        assertThatThrownBy(() -> jdbc.sql("UPDATE vehicle_positions SET route_id = 'x'").update())
                .hasMessageContaining("append-only");
    }

    @Test
    void acceptsSeveralTimestampsForSameVehicleInRewrittenBatch() {
        var first = position("1", "501", 20);
        var second = position("1", "501", 10);
        assertThat(repository.ingest(List.of(second, first, second))).isEqualTo(2);
        assertThat(repository.history(all)).containsExactly(second, first);
        assertThat(repository.latest(all)).containsExactly(second);
    }

    @Test
    void rollbackLeavesBothTablesUnchangedWhenOneRowViolatesConstraints() {
        var original = position("1", "501", 20);
        repository.ingest(List.of(original));
        var invalid = new Observation("bad", null, null, null, 999, -79, null, null, null, null, null, NOW, NOW);
        assertThatThrownBy(() -> repository.ingest(List.of(position("1", "504", 10), invalid)))
                .isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(repository.latest(all)).containsExactly(original);
        assertThat(repository.history(all)).containsExactly(original);
    }

    @Test
    void retentionDeletesOnlyHistoryAndKeepsHighWaterMark() {
        var old = position("1", "501", 31 * 86400L);
        var recent = position("2", "504", 10);
        repository.ingest(List.of(old, recent));
        assertThat(repository.deleteHistoryBatch(NOW.minusSeconds(30 * 86400L), 100)).isEqualTo(1);
        assertThat(repository.history(all)).containsExactly(recent);
        assertThat(repository.latest(all)).containsExactly(old, recent);
        assertThat(repository.ingest(List.of(old))).isZero();
    }

    @Test
    void concurrentIdenticalPollsInsertOnlyOnce() throws Exception {
        var barrier = new CyclicBarrier(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> {
                barrier.await();
                return repository.ingest(List.of(position("1", "501", 1)));
            });
            var second = executor.submit(() -> {
                barrier.await();
                return repository.ingest(List.of(position("1", "501", 1)));
            });
            assertThat(first.get() + second.get()).isEqualTo(1);
        }
        assertThat(repository.history(all)).hasSize(1);
    }
}
