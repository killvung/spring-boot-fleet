package ca.ttc.fleet.grpc;

import static org.assertj.core.api.Assertions.*;

import ca.ttc.fleet.PostgresIntegrationSupport;
import ca.ttc.fleet.api.v1.FleetServiceGrpc;
import ca.ttc.fleet.api.v1.GetLatestVehiclePositionsRequest;
import io.grpc.*;
import io.grpc.health.v1.*;
import java.time.Instant;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.grpc.server.GlobalServerInterceptor;
import org.springframework.grpc.server.lifecycle.GrpcServerLifecycle;
import org.springframework.scheduling.TaskScheduler;

@Import(RuntimeIT.ProbeConfiguration.class)
class RuntimeIT extends PostgresIntegrationSupport {
    @Autowired
    GrpcServerLifecycle lifecycle;
    @Autowired
    TaskScheduler scheduler;
    @Autowired
    ThreadProbe probe;

    @Test
    void nettyRequestsAndScheduledWorkRunOnVirtualThreadsAndHealthUsesGrpc() throws Exception {
        var scheduled = new ArrayBlockingQueue<Boolean>(1);
        scheduler.schedule(() -> scheduled.add(Thread.currentThread().isVirtual()), Instant.now());
        assertThat(scheduled.poll(5, TimeUnit.SECONDS)).isTrue();
        var channel = ManagedChannelBuilder.forAddress("localhost", lifecycle.getPort()).usePlaintext().build();
        try {
            var records = FleetServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getLatestVehiclePositions(GetLatestVehiclePositionsRequest.getDefaultInstance());
            records.forEachRemaining(ignored -> {
            });
            assertThat(probe.requests.poll(5, TimeUnit.SECONDS)).isTrue();
            var health = HealthGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.getDefaultInstance());
            assertThat(health.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
            assertThatThrownBy(() -> HealthGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.newBuilder().setService("missing").build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            error -> assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ProbeConfiguration {
        @Bean
        @GlobalServerInterceptor
        ThreadProbe threadProbe() {
            return new ThreadProbe();
        }
    }

    static class ThreadProbe implements ServerInterceptor {
        final ArrayBlockingQueue<Boolean> requests = new ArrayBlockingQueue<>(10);

        @Override
        public <Q, S> ServerCall.Listener<Q> interceptCall(ServerCall<Q, S> call, Metadata headers,
                ServerCallHandler<Q, S> next) {
            return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
                    next.startCall(call, headers)) {
                @Override
                public void onHalfClose() {
                    requests.offer(Thread.currentThread().isVirtual());
                    super.onHalfClose();
                }
            };
        }
    }
}
