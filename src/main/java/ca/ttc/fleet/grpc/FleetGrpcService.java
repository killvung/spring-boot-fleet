package ca.ttc.fleet.grpc;

import ca.ttc.fleet.api.v1.FleetServiceGrpc;
import ca.ttc.fleet.api.v1.GetLatestVehiclePositionsRequest;
import ca.ttc.fleet.api.v1.GetRouteSnapshotRequest;
import ca.ttc.fleet.api.v1.GetVehicleHistoryRequest;
import ca.ttc.fleet.api.v1.RouteSnapshot;
import ca.ttc.fleet.api.v1.VehiclePosition;
import ca.ttc.fleet.config.TtcProperties;
import ca.ttc.fleet.domain.Observation;
import ca.ttc.fleet.domain.PositionFilter;
import ca.ttc.fleet.persistence.PositionRepository;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

@Service
public class FleetGrpcService extends FleetServiceGrpc.FleetServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FleetGrpcService.class);
    private final PositionRepository repository;
    private final TtcProperties properties;
    private final Clock clock;

    public FleetGrpcService(PositionRepository repository, TtcProperties properties, Clock clock) {
        this.repository = repository;
        this.properties = properties;
        this.clock = clock;
    }

    @Override
    public void getLatestVehiclePositions(GetLatestVehiclePositionsRequest request,
            StreamObserver<VehiclePosition> observer) {
        respond(observer, () -> {
            var filter = new PositionFilter(id(request.hasVehicleId(), request.getVehicleId()),
                    id(request.hasRouteId(), request.getRouteId()),
                    direction(request.hasDirectionId(), request.getDirectionId()),
                    null, null, null, id(request.hasAfterVehicleId(), request.getAfterVehicleId()),
                    limit(request.getLimit()));
            stream(repository.latest(filter), filter.vehicleId(), filter.afterVehicleId() != null, observer);
        });
    }

    @Override
    public void getVehicleHistory(GetVehicleHistoryRequest request, StreamObserver<VehiclePosition> observer) {
        respond(observer, () -> {
            Instant from = request.hasFrom() ? instant(request.getFrom()) : null;
            Instant to = request.hasTo() ? instant(request.getTo()) : null;
            if (from != null && to != null && !from.isBefore(to))
                invalid("from must be earlier than to");
            if (request.hasBeforeSourceTimestamp() != request.hasAfterVehicleId())
                invalid("History cursor requires timestamp and vehicle ID");
            var filter = new PositionFilter(id(request.hasVehicleId(), request.getVehicleId()),
                    id(request.hasRouteId(), request.getRouteId()),
                    direction(request.hasDirectionId(), request.getDirectionId()),
                    from, to, request.hasBeforeSourceTimestamp() ? instant(request.getBeforeSourceTimestamp()) : null,
                    id(request.hasAfterVehicleId(), request.getAfterVehicleId()), limit(request.getLimit()));
            stream(repository.history(filter), filter.vehicleId(), filter.afterVehicleId() != null, observer);
        });
    }

    @Override
    public void getRouteSnapshot(GetRouteSnapshotRequest request, StreamObserver<RouteSnapshot> observer) {
        respond(observer, () -> {
            String route = id(true, request.getRouteId());
            int limit = limit(request.getLimit());
            var filter = new PositionFilter(id(request.hasVehicleId(), request.getVehicleId()), route,
                    direction(request.hasDirectionId(), request.getDirectionId()), null, null, null,
                    id(request.hasAfterVehicleId(), request.getAfterVehicleId()), limit + 1);
            var positions = repository.latest(filter);
            if (positions.isEmpty() && filter.afterVehicleId() == null) {
                throw Status.NOT_FOUND.withDescription("No positions match the route snapshot").asRuntimeException();
            }
            boolean truncated = positions.size() > limit;
            var result = RouteSnapshot.newBuilder().setRouteId(route)
                    .setQueriedAt(ProtoMapper.timestamp(clock.instant()))
                    .setTruncated(truncated);
            positions.stream().limit(limit).map(ProtoMapper::position).forEach(result::addPositions);
            if (truncated)
                result.setNextAfterVehicleId(positions.get(limit - 1).vehicleId());
            if (!Context.current().isCancelled()) {
                observer.onNext(result.build());
                observer.onCompleted();
            }
        });
    }

    private void stream(List<Observation> positions, String vehicle, boolean continuation,
            StreamObserver<VehiclePosition> observer) {
        if (positions.isEmpty() && vehicle != null && !continuation) {
            throw Status.NOT_FOUND.withDescription("No observations match this vehicle and filters")
                    .asRuntimeException();
        }
        // A hard response cap bounds buffering even when a client reads slowly; no
        // unbounded producer is used.
        for (var position : positions) {
            if (Context.current().isCancelled())
                return;
            observer.onNext(ProtoMapper.position(position));
        }
        if (!Context.current().isCancelled())
            observer.onCompleted();
    }

    private void respond(StreamObserver<?> observer, Runnable operation) {
        if (Context.current().isCancelled())
            return;
        try {
            operation.run();
        } catch (StatusRuntimeException status) {
            observer.onError(status);
        } catch (DataAccessException unavailable) {
            log.warn("Fleet query unavailable: {}", unavailable.getClass().getSimpleName());
            observer.onError(Status.UNAVAILABLE.withDescription("Position storage unavailable").asRuntimeException());
        } catch (Exception unexpected) {
            log.error("Fleet query failed", unexpected);
            observer.onError(Status.INTERNAL.withDescription("Fleet query failed").asRuntimeException());
        }
    }

    private int limit(int requested) {
        if (requested < 0 || requested > properties.maxRecords())
            invalid("limit exceeds configured maximum " + properties.maxRecords());
        return requested == 0 ? properties.maxRecords() : requested;
    }

    private static String id(boolean present, String value) {
        if (!present)
            return null;
        if (value.isBlank() || value.length() > 256)
            invalid("Identifiers must contain 1 to 256 characters");
        return value;
    }

    private static Integer direction(boolean present, int value) {
        if (!present)
            return null;
        if (value != 0 && value != 1)
            invalid("direction_id must be 0 or 1");
        return value;
    }

    private static Instant instant(Timestamp timestamp) {
        if (timestamp.getSeconds() < -62135596800L || timestamp.getSeconds() > 253402300799L
                || timestamp.getNanos() < 0 || timestamp.getNanos() > 999999999)
            invalid("Invalid protobuf timestamp");
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static void invalid(String message) {
        throw Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }
}
