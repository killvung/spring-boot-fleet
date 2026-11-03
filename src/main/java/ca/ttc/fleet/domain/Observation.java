package ca.ttc.fleet.domain;

import java.time.Instant;

public record Observation(String vehicleId, String routeId, String tripId, Integer directionId,
                double latitude, double longitude, Float bearing, Float speed, String occupancyStatus,
                Long currentStopSequence, String currentStopId, Instant sourceTimestamp, Instant ingestedAt) {
}
