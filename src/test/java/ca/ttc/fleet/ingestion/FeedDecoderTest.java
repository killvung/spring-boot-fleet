package ca.ttc.fleet.ingestion;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;

import com.google.transit.realtime.GtfsRealtime;
import org.junit.jupiter.api.Test;

class FeedDecoderTest {
    private final FeedDecoder decoder = new FeedDecoder();

    @Test
    void decodesBinaryAndPrefersVehicleTimestampWhilePreservingPresence() throws Exception {
        var entity = entity("1001");
        entity.getVehicleBuilder().setTimestamp(NOW.minusSeconds(5).getEpochSecond())
                .setTrip(GtfsRealtime.TripDescriptor.newBuilder().setRouteId("501").setTripId("t1").setDirectionId(0))
                .setCurrentStopSequence(-1).setStopId("stop1")
                .setOccupancyStatus(GtfsRealtime.VehiclePosition.OccupancyStatus.FULL)
                .getPositionBuilder().setBearing(0).setSpeed(0);
        var decoded = decoder.decode(feed().addEntity(entity).build().toByteArray(), NOW);
        var p = decoded.observations().getFirst();
        assertThat(p.sourceTimestamp()).isEqualTo(NOW.minusSeconds(5));
        assertThat(p.ingestedAt()).isEqualTo(NOW);
        assertThat(p.routeId()).isEqualTo("501");
        assertThat(p.directionId()).isZero();
        assertThat(p.bearing()).isZero();
        assertThat(p.speed()).isZero();
        assertThat(p.currentStopSequence()).isEqualTo(4294967295L);
        assertThat(p.occupancyStatus()).isEqualTo("FULL");
    }

    @Test
    void fallsBackToHeaderTimestampAndDoesNotInventOptionalFields() throws Exception {
        var p = decoder.decode(feed().addEntity(entity("1")).build().toByteArray(), NOW).observations().getFirst();
        assertThat(p.sourceTimestamp()).isEqualTo(NOW.minusSeconds(10));
        assertThat(p.routeId()).isNull();
        assertThat(p.directionId()).isNull();
        assertThat(p.speed()).isNull();
        assertThat(p.bearing()).isNull();
        assertThat(p.occupancyStatus()).isNull();
        assertThat(p.currentStopSequence()).isNull();
    }

    @Test
    void rejectsEmptyMalformedMissingTimestampAndDifferentialFeeds() {
        assertThatThrownBy(() -> decoder.decode(new byte[0], NOW)).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> decoder.decode(new byte[] { 1, 2, 3 }, NOW)).isInstanceOf(Exception.class);
        var missing = feed().addEntity(entity("1"));
        missing.getHeaderBuilder().clearTimestamp();
        assertThatThrownBy(() -> decoder.decode(missing.build().toByteArray(), NOW)).isInstanceOf(Exception.class);
        var differential = feed();
        differential.getHeaderBuilder().setIncrementality(GtfsRealtime.FeedHeader.Incrementality.DIFFERENTIAL);
        assertThatThrownBy(() -> decoder.decode(differential.build().toByteArray(), NOW)).isInstanceOf(Exception.class);
    }

    @Test
    void skipsInvalidCoordinatesMissingIdsAndFutureTimestampsWithoutDroppingGoodRows() throws Exception {
        var invalid = entity("bad");
        invalid.getVehicleBuilder().getPositionBuilder().setLatitude(Float.NaN);
        var future = entity("future");
        future.getVehicleBuilder().setTimestamp(NOW.plusSeconds(301).getEpochSecond());
        var decoded = decoder.decode(feed().addEntity(invalid).addEntity(entity(""))
                .addEntity(future).addEntity(entity("good")).build().toByteArray(), NOW);
        assertThat(decoded.rejectedCount()).isEqualTo(3);
        assertThat(decoded.observations()).extracting(p -> p.vehicleId()).containsExactly("good");
    }

    @Test
    void validZeroEntityFeedIsSuccessfulAndContainsNoObservations() throws Exception {
        assertThat(decoder.decode(feed().build().toByteArray(), NOW).observations()).isEmpty();
    }
}
