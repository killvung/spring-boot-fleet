package ca.ttc.fleet.grpc;

import ca.ttc.fleet.api.v1.VehiclePosition;
import ca.ttc.fleet.domain.Observation;
import com.google.protobuf.Timestamp;
import java.time.Instant;

final class ProtoMapper {
    private ProtoMapper() {
    }

    static Timestamp timestamp(Instant time) {
        return Timestamp.newBuilder().setSeconds(time.getEpochSecond()).setNanos(time.getNano()).build();
    }

    static VehiclePosition position(Observation p) {
        var result = VehiclePosition.newBuilder().setVehicleId(p.vehicleId())
                .setLatitude(p.latitude()).setLongitude(p.longitude())
                .setSourceTimestamp(timestamp(p.sourceTimestamp())).setIngestedAt(timestamp(p.ingestedAt()));
        if (p.routeId() != null)
            result.setRouteId(p.routeId());
        if (p.tripId() != null)
            result.setTripId(p.tripId());
        if (p.directionId() != null)
            result.setDirectionId(p.directionId());
        if (p.bearing() != null)
            result.setBearing(p.bearing());
        if (p.speed() != null)
            result.setSpeed(p.speed());
        if (p.occupancyStatus() != null)
            result.setOccupancyStatus(p.occupancyStatus());
        if (p.currentStopSequence() != null)
            result.setCurrentStopSequence(p.currentStopSequence().intValue());
        if (p.currentStopId() != null)
            result.setCurrentStopId(p.currentStopId());
        return result.build();
    }
}
