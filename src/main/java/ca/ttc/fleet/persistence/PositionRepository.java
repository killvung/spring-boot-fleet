package ca.ttc.fleet.persistence;

import ca.ttc.fleet.domain.Observation;
import ca.ttc.fleet.domain.ObservationPolicy;
import ca.ttc.fleet.domain.PositionFilter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

@Repository
public class PositionRepository {
    private static final String COLUMNS = """
            vehicle_id, route_id, trip_id, direction_id, latitude, longitude, bearing, speed,
            occupancy_status, current_stop_sequence, current_stop_id, source_timestamp, ingested_at
            """;
    private static final String VALUES = """
            :vehicle_id, :route_id, :trip_id, :direction_id, :latitude, :longitude, :bearing, :speed,
            :occupancy_status, :current_stop_sequence, :current_stop_id, :source_timestamp, :ingested_at
            """;
    private static final String INSERT_HISTORY = "INSERT INTO vehicle_positions (" + COLUMNS + ") VALUES (" + VALUES
            + ")";
    private static final String UPSERT_LATEST = "INSERT INTO latest_vehicle_positions (" + COLUMNS + ") VALUES ("
            + VALUES + ") " + """
                    ON CONFLICT (vehicle_id) DO UPDATE SET
                        route_id = EXCLUDED.route_id, trip_id = EXCLUDED.trip_id,
                        direction_id = EXCLUDED.direction_id, latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude, bearing = EXCLUDED.bearing, speed = EXCLUDED.speed,
                        occupancy_status = EXCLUDED.occupancy_status,
                        current_stop_sequence = EXCLUDED.current_stop_sequence,
                        current_stop_id = EXCLUDED.current_stop_id,
                        source_timestamp = EXCLUDED.source_timestamp, ingested_at = EXCLUDED.ingested_at
                    WHERE EXCLUDED.source_timestamp > latest_vehicle_positions.source_timestamp
                    """;
    private static final RowMapper<Observation> MAPPER = (rs, row) -> map(rs);

    private final JdbcClient jdbc;
    private final NamedParameterJdbcTemplate batches;
    private final TransactionTemplate transactions;

    public PositionRepository(JdbcClient jdbc, NamedParameterJdbcTemplate batches, TransactionTemplate transactions) {
        this.jdbc = jdbc;
        this.batches = batches;
        this.transactions = transactions;
    }

    public int ingest(List<Observation> incoming) {
        if (incoming.isEmpty())
            return 0;
        Integer inserted = transactions.execute(status -> {
            // The single writer lock makes comparison + both batches atomic across
            // concurrent polls/processes.
            // READ COMMITTED ensures the following SELECT sees the previous lock holder's
            // commit.
            jdbc.sql("SET LOCAL lock_timeout = '5s'").update();
            jdbc.sql("SELECT pg_advisory_xact_lock(8847210051)").query((rs, row) -> true).list();
            var newest = new HashMap<String, Instant>();
            var ids = incoming.stream().map(Observation::vehicleId).distinct().toList();
            for (int start = 0; start < ids.size(); start += 500) {
                jdbc.sql("SELECT vehicle_id, source_timestamp FROM latest_vehicle_positions WHERE vehicle_id IN (:ids)")
                        .param("ids", ids.subList(start, Math.min(start + 500, ids.size())))
                        .query((rs, row) -> Map.entry(rs.getString(1),
                                rs.getObject(2, OffsetDateTime.class).toInstant()))
                        .list().forEach(entry -> newest.put(entry.getKey(), entry.getValue()));
            }
            List<Observation> accepted = ObservationPolicy.acceptNewer(incoming, newest);
            batch(INSERT_HISTORY, accepted);
            // Only one upsert per vehicle: PostgreSQL rewritten batches must not affect a
            // row twice.
            var latest = new java.util.TreeMap<String, Observation>();
            accepted.forEach(position -> latest.put(position.vehicleId(), position));
            batch(UPSERT_LATEST, new ArrayList<>(latest.values()));
            return accepted.size();
        });
        return inserted == null ? 0 : inserted;
    }

    public List<Observation> latest(PositionFilter filter) {
        return query("latest_vehicle_positions", filter, false);
    }

    public List<Observation> history(PositionFilter filter) {
        return query("vehicle_positions", filter, true);
    }

    private List<Observation> query(String table, PositionFilter filter, boolean history) {
        var sql = new StringBuilder("SELECT ").append(COLUMNS).append(" FROM ").append(table).append(" WHERE TRUE");
        var params = new HashMap<String, Object>();
        clause(sql, params, "vehicle_id = :vehicle", "vehicle", filter.vehicleId());
        clause(sql, params, "route_id = :route", "route", filter.routeId());
        clause(sql, params, "direction_id = :direction", "direction", filter.directionId());
        if (history) {
            clause(sql, params, "source_timestamp >= :from", "from", utc(filter.from()));
            clause(sql, params, "source_timestamp < :to", "to", utc(filter.to()));
            if (filter.beforeTimestamp() != null) {
                sql.append(" AND (source_timestamp < :before OR (source_timestamp = :before AND vehicle_id > :after))");
                params.put("before", utc(filter.beforeTimestamp()));
                params.put("after", filter.afterVehicleId());
            }
            sql.append(" ORDER BY source_timestamp DESC, vehicle_id ASC");
        } else {
            clause(sql, params, "vehicle_id > :after", "after", filter.afterVehicleId());
            sql.append(" ORDER BY vehicle_id ASC");
        }
        sql.append(" LIMIT :limit");
        params.put("limit", filter.limit());
        return jdbc.sql(sql.toString()).params(params).query(MAPPER).list();
    }

    /**
     * Small commits avoid a long transaction and bound cleanup work on the small
     * deployment.
     */
    public int deleteHistoryBatch(Instant cutoff, int size) {
        return jdbc.sql("""
                DELETE FROM vehicle_positions WHERE ctid IN (
                    SELECT ctid FROM vehicle_positions WHERE source_timestamp < :cutoff
                    ORDER BY source_timestamp LIMIT :size FOR UPDATE SKIP LOCKED
                )
                """).param("cutoff", utc(cutoff)).param("size", size).update();
    }

    public boolean isAvailable() {
        return jdbc.sql("SELECT 1").query(Integer.class).single() == 1;
    }

    private void batch(String sql, List<Observation> observations) {
        for (int start = 0; start < observations.size(); start += 500) {
            var params = observations.subList(start, Math.min(start + 500, observations.size()))
                    .stream().map(PositionRepository::parameters).toArray(MapSqlParameterSource[]::new);
            batches.batchUpdate(sql, params);
        }
    }

    private static MapSqlParameterSource parameters(Observation p) {
        return new MapSqlParameterSource()
                .addValue("vehicle_id", p.vehicleId(), Types.VARCHAR)
                .addValue("route_id", p.routeId(), Types.VARCHAR)
                .addValue("trip_id", p.tripId(), Types.VARCHAR)
                .addValue("direction_id", p.directionId(), Types.SMALLINT)
                .addValue("latitude", p.latitude()).addValue("longitude", p.longitude())
                .addValue("bearing", p.bearing(), Types.REAL).addValue("speed", p.speed(), Types.REAL)
                .addValue("occupancy_status", p.occupancyStatus(), Types.VARCHAR)
                .addValue("current_stop_sequence", p.currentStopSequence(), Types.BIGINT)
                .addValue("current_stop_id", p.currentStopId(), Types.VARCHAR)
                .addValue("source_timestamp", utc(p.sourceTimestamp()), Types.TIMESTAMP_WITH_TIMEZONE)
                .addValue("ingested_at", utc(p.ingestedAt()), Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private static void clause(StringBuilder sql, Map<String, Object> params, String condition, String key,
            Object value) {
        if (value != null) {
            sql.append(" AND ").append(condition);
            params.put(key, value);
        }
    }

    private static OffsetDateTime utc(Instant value) {
        return value == null ? null : value.atOffset(ZoneOffset.UTC);
    }

    private static Observation map(ResultSet rs) throws SQLException {
        return new Observation(rs.getString("vehicle_id"), rs.getString("route_id"), rs.getString("trip_id"),
                rs.getObject("direction_id", Integer.class), rs.getDouble("latitude"), rs.getDouble("longitude"),
                rs.getObject("bearing", Float.class), rs.getObject("speed", Float.class),
                rs.getString("occupancy_status"),
                rs.getObject("current_stop_sequence", Long.class), rs.getString("current_stop_id"),
                rs.getObject("source_timestamp", OffsetDateTime.class).toInstant(),
                rs.getObject("ingested_at", OffsetDateTime.class).toInstant());
    }
}
