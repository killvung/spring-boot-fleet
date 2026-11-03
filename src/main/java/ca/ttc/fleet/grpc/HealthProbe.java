package ca.ttc.fleet.grpc;

import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.util.concurrent.TimeUnit;

/**
 * Container probe using the same binary gRPC protocol; no HTTP/JSON management
 * port.
 */
public final class HealthProbe {
    private HealthProbe() {
    }

    public static void main(String[] args) throws InterruptedException {
        String port = System.getenv().getOrDefault("GRPC_SERVER_PORT", "50051");
        var channel = ManagedChannelBuilder.forAddress("127.0.0.1", Integer.parseInt(port)).usePlaintext().build();
        int exit = 1;
        try {
            var result = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.getDefaultInstance());
            if (result.getStatus() == HealthCheckResponse.ServingStatus.SERVING)
                exit = 0;
        } catch (Exception unavailable) {
            System.err.println("gRPC health check failed");
        } finally {
            channel.shutdownNow().awaitTermination(1, TimeUnit.SECONDS);
        }
        System.exit(exit);
    }
}
