package ca.ttc.fleet.grpc;

import ca.ttc.fleet.api.v1.FleetServiceGrpc;
import ca.ttc.fleet.persistence.PositionRepository;
import io.grpc.Status;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.stereotype.Service;

/**
 * Standard unary gRPC health Check. Feed outages do not make retained positions
 * unavailable.
 */
@Service
public class FleetHealthService extends HealthGrpc.HealthImplBase {
    private final PositionRepository repository;
    private final ApplicationAvailability availability;

    public FleetHealthService(PositionRepository repository, ApplicationAvailability availability) {
        this.repository = repository;
        this.availability = availability;
    }

    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> observer) {
        if (!request.getService().isEmpty() && !request.getService().equals(FleetServiceGrpc.SERVICE_NAME)) {
            observer.onError(Status.NOT_FOUND.withDescription("Unknown service").asRuntimeException());
            return;
        }
        boolean healthy;
        try {
            healthy = availability.getReadinessState() == ReadinessState.ACCEPTING_TRAFFIC && repository.isAvailable();
        } catch (Exception unavailable) {
            healthy = false;
        }
        observer.onNext(HealthCheckResponse.newBuilder().setStatus(healthy
                ? HealthCheckResponse.ServingStatus.SERVING
                : HealthCheckResponse.ServingStatus.NOT_SERVING).build());
        observer.onCompleted();
    }
}
