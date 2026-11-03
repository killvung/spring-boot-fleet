package ca.ttc.fleet.ingestion;

import ca.ttc.fleet.domain.Observation;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.transit.realtime.GtfsRealtime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class FeedDecoder {
    public record DecodedFeed(Instant timestamp, int entityCount, int rejectedCount, List<Observation> observations) {
    }

    public DecodedFeed decode(byte[] bytes, Instant ingestedAt) throws InvalidProtocolBufferException {
        if (bytes.length == 0)
            throw new InvalidProtocolBufferException("Empty feed body");
        var feed = GtfsRealtime.FeedMessage.parseFrom(bytes);
        var header = feed.getHeader();
        if (!List.of("1.0", "2.0").contains(header.getGtfsRealtimeVersion())
                || header.getIncrementality() != GtfsRealtime.FeedHeader.Incrementality.FULL_DATASET) {
            throw new InvalidProtocolBufferException("Unsupported feed version or incrementality");
        }
        Instant timestamp = header.hasTimestamp() ? timestamp(header.getTimestamp(), ingestedAt) : null;
        var observations = new ArrayList<Observation>();
        int rejected = 0;
        for (var entity : feed.getEntityList()) {
            if (!entity.hasVehicle() || entity.getIsDeleted())
                continue;
            var vehicle = entity.getVehicle();
            try {
                String id = text(vehicle.getVehicle().getId());
                if (id == null || !vehicle.hasPosition())
                    throw new IllegalArgumentException("Missing identity or position");
                var position = vehicle.getPosition();
                double latitude = position.getLatitude();
                double longitude = position.getLongitude();
                if (!Double.isFinite(latitude) || latitude < -90 || latitude > 90
                        || !Double.isFinite(longitude) || longitude < -180 || longitude > 180) {
                    throw new IllegalArgumentException("Invalid coordinates");
                }
                Instant sourceTime = vehicle.hasTimestamp() ? timestamp(vehicle.getTimestamp(), ingestedAt) : timestamp;
                if (sourceTime == null)
                    throw new IllegalArgumentException("Missing timestamp");
                var trip = vehicle.getTrip();
                Integer direction = trip.hasDirectionId() ? trip.getDirectionId() : null;
                if (direction != null && direction != 0 && direction != 1)
                    throw new IllegalArgumentException("Invalid direction");
                Float bearing = position.hasBearing() ? position.getBearing() : null;
                Float speed = position.hasSpeed() ? position.getSpeed() : null;
                if (bearing != null && (!Float.isFinite(bearing) || bearing < 0 || bearing >= 360)
                        || speed != null && (!Float.isFinite(speed) || speed < 0)) {
                    throw new IllegalArgumentException("Invalid bearing or speed");
                }
                observations.add(new Observation(id, text(trip.getRouteId()), text(trip.getTripId()), direction,
                        latitude, longitude, bearing, speed,
                        vehicle.hasOccupancyStatus() ? vehicle.getOccupancyStatus().name() : null,
                        vehicle.hasCurrentStopSequence() ? Integer.toUnsignedLong(vehicle.getCurrentStopSequence())
                                : null,
                        text(vehicle.getStopId()), sourceTime, ingestedAt));
            } catch (IllegalArgumentException invalidEntity) {
                rejected++;
            }
        }
        if (rejected > 0 && observations.isEmpty()) {
            throw new InvalidProtocolBufferException("No valid vehicle observations in feed");
        }
        return new DecodedFeed(timestamp, feed.getEntityCount(), rejected, List.copyOf(observations));
    }

    private static Instant timestamp(long seconds, Instant now) {
        // Reject signed overflow/invalid epochs and clock skew that could poison a
        // vehicle's high-water mark.
        if (seconds <= 0 || seconds > 253402300799L || seconds > now.plusSeconds(300).getEpochSecond()) {
            throw new IllegalArgumentException("Invalid or future feed timestamp");
        }
        return Instant.ofEpochSecond(seconds);
    }

    private static String text(String value) {
        if (value.isBlank())
            return null;
        if (value.length() > 256)
            throw new IllegalArgumentException("Identifier too long");
        return value;
    }
}
