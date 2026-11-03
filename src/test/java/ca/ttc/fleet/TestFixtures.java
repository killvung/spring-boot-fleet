package ca.ttc.fleet;

import ca.ttc.fleet.config.TtcProperties;
import ca.ttc.fleet.domain.Observation;
import com.google.transit.realtime.GtfsRealtime;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;

public final class TestFixtures {
    public static final Instant NOW = Instant.parse("2026-11-03T15:00:00Z");

    private TestFixtures() {
    }

    public static TtcProperties properties() {
        return properties(URI.create("https://bustime.ttc.ca/gtfsrt/vehicles"));
    }

    public static TtcProperties properties(URI uri) {
        return new TtcProperties(uri, Duration.ofSeconds(20), Duration.ofSeconds(1), Duration.ofSeconds(1),
                30, 1000, 1024, true);
    }

    public static Observation position(String vehicle, String route, long age) {
        return new Observation(vehicle, route, "trip-1", 0, 43.65, -79.38, 180f, 5f,
                "MANY_SEATS_AVAILABLE", 3L, "stop-1", NOW.minusSeconds(age), NOW);
    }

    public static GtfsRealtime.FeedMessage.Builder feed() {
        return GtfsRealtime.FeedMessage.newBuilder().setHeader(GtfsRealtime.FeedHeader.newBuilder()
                .setGtfsRealtimeVersion("2.0").setTimestamp(NOW.minusSeconds(10).getEpochSecond()));
    }

    public static GtfsRealtime.FeedEntity.Builder entity(String id) {
        return GtfsRealtime.FeedEntity.newBuilder().setId(id).setVehicle(GtfsRealtime.VehiclePosition.newBuilder()
                .setVehicle(GtfsRealtime.VehicleDescriptor.newBuilder().setId(id))
                .setPosition(GtfsRealtime.Position.newBuilder().setLatitude(43.65f).setLongitude(-79.38f)));
    }
}
