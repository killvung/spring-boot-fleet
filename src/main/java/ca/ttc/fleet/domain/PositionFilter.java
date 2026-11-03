package ca.ttc.fleet.domain;

import java.time.Instant;

public record PositionFilter(String vehicleId, String routeId, Integer directionId,
        Instant from, Instant to, Instant beforeTimestamp, String afterVehicleId, int limit) {
}
