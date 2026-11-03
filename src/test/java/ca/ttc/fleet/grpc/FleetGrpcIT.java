package ca.ttc.fleet.grpc;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;

import ca.ttc.fleet.PostgresIntegrationSupport;
import ca.ttc.fleet.api.v1.*;
import ca.ttc.fleet.persistence.PositionRepository;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.simple.JdbcClient;

class FleetGrpcIT extends PostgresIntegrationSupport {
    @Autowired
    FleetGrpcService service;
    @Autowired
    PositionRepository repository;
    @Autowired
    JdbcClient jdbc;
    @Autowired
    @Qualifier("fleetVirtualExecutor")
    ExecutorService executor;
    private Server server;
    private ManagedChannel channel;
    private FleetServiceGrpc.FleetServiceBlockingStub client;

    @BeforeEach
    void start() throws Exception {
        jdbc.sql("TRUNCATE vehicle_positions, latest_vehicle_positions").update();
        repository.ingest(List.of(position("1", "501", 20), position("1", "501", 10),
                position("2", "504", 10), position("3", "501", 10)));
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).executor(executor).addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        client = FleetServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void stop() throws Exception {
        if (channel != null)
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        if (server != null)
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void latestRouteAndDirectionFiltersUseLatestTable() {
        assertThat(list(client.getLatestVehiclePositions(GetLatestVehiclePositionsRequest.getDefaultInstance())))
                .extracting(VehiclePosition::getVehicleId).containsExactly("1", "2", "3");
        var route = GetLatestVehiclePositionsRequest.newBuilder().setRouteId("501").setDirectionId(0).build();
        assertThat(list(client.getLatestVehiclePositions(route))).extracting(VehiclePosition::getVehicleId)
                .containsExactly("1", "3");
        assertThat(list(client.getLatestVehiclePositions(route.toBuilder().setDirectionId(1).build()))).isEmpty();
    }

    @Test
    void historyTimeRangeIsInclusiveExclusiveAndQueriesHistoryTable() {
        var request = GetVehicleHistoryRequest.newBuilder().setVehicleId("1")
                .setFrom(ProtoMapper.timestamp(NOW.minusSeconds(20))).setTo(ProtoMapper.timestamp(NOW.minusSeconds(10)))
                .build();
        var result = list(client.getVehicleHistory(request));
        assertThat(result).hasSize(1);
        assertThat(result.getFirst().getSourceTimestamp()).isEqualTo(ProtoMapper.timestamp(NOW.minusSeconds(20)));
        assertThat(list(client.getVehicleHistory(GetVehicleHistoryRequest.newBuilder().setVehicleId("1").build())))
                .hasSize(2);
    }

    @Test
    void limitsAndCursorsDoNotLoseEqualTimestampRows() {
        var latest = list(
                client.getLatestVehiclePositions(GetLatestVehiclePositionsRequest.newBuilder().setLimit(1).build()));
        assertThat(latest).hasSize(1);
        assertThat(list(client.getLatestVehiclePositions(GetLatestVehiclePositionsRequest.newBuilder().setLimit(1)
                .setAfterVehicleId(latest.getFirst().getVehicleId()).build())))
                .extracting(VehiclePosition::getVehicleId).containsExactly("2");
        var first = list(client.getVehicleHistory(GetVehicleHistoryRequest.newBuilder().setLimit(2).build()));
        assertThat(first).extracting(VehiclePosition::getVehicleId).containsExactly("1", "2");
        var cursor = first.getLast();
        var next = list(client.getVehicleHistory(GetVehicleHistoryRequest.newBuilder().setLimit(2)
                .setBeforeSourceTimestamp(cursor.getSourceTimestamp()).setAfterVehicleId(cursor.getVehicleId())
                .build()));
        assertThat(next).extracting(VehiclePosition::getVehicleId).containsExactly("3", "1");
    }

    @Test
    void defaultMaximumIsOneThousandAndExplicitOverflowIsRejected() {
        var bulk = new ArrayList<ca.ttc.fleet.domain.Observation>();
        for (int i = 0; i < 1010; i++)
            bulk.add(position("bulk-" + i, "510", 1));
        repository.ingest(bulk);
        assertThat(list(client.getLatestVehiclePositions(GetLatestVehiclePositionsRequest.getDefaultInstance())))
                .hasSize(1000);
        assertThat(list(client.getVehicleHistory(GetVehicleHistoryRequest.getDefaultInstance()))).hasSize(1000);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> list(client.getVehicleHistory(
                GetVehicleHistoryRequest.newBuilder().setLimit(1001).build())));
    }

    @Test
    void routeSnapshotIsBoundedAndPaginated() {
        var snapshot = client
                .getRouteSnapshot(GetRouteSnapshotRequest.newBuilder().setRouteId("501").setLimit(1).build());
        assertThat(snapshot.getPositionsCount()).isEqualTo(1);
        assertThat(snapshot.getTruncated()).isTrue();
        assertThat(snapshot.getNextAfterVehicleId()).isEqualTo("1");
        var next = client.getRouteSnapshot(GetRouteSnapshotRequest.newBuilder().setRouteId("501").setLimit(1)
                .setAfterVehicleId(snapshot.getNextAfterVehicleId()).build());
        assertThat(next.getPositions(0).getVehicleId()).isEqualTo("3");
        assertThat(next.getTruncated()).isFalse();
    }

    @Test
    void invalidArgumentsAndNotFoundUseGrpcStatus() {
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> client.getRouteSnapshot(GetRouteSnapshotRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> list(client.getLatestVehiclePositions(
                GetLatestVehiclePositionsRequest.newBuilder().setDirectionId(2).build())));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> list(client.getVehicleHistory(
                GetVehicleHistoryRequest.newBuilder().setFrom(Timestamp.newBuilder().setNanos(-1)).build())));
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> list(client.getVehicleHistory(GetVehicleHistoryRequest.newBuilder()
                        .setFrom(ProtoMapper.timestamp(NOW)).setTo(ProtoMapper.timestamp(NOW.minusSeconds(1)))
                        .build())));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> list(client.getVehicleHistory(
                GetVehicleHistoryRequest.newBuilder().setAfterVehicleId("1").build())));
        assertStatus(Status.Code.NOT_FOUND, () -> list(client.getLatestVehiclePositions(
                GetLatestVehiclePositionsRequest.newBuilder().setVehicleId("missing").build())));
        assertStatus(Status.Code.NOT_FOUND, () -> client.getRouteSnapshot(
                GetRouteSnapshotRequest.newBuilder().setRouteId("missing").build()));
    }

    private static <T> List<T> list(Iterator<T> iterator) {
        var result = new ArrayList<T>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    private static void assertStatus(Status.Code code, Runnable call) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(StatusRuntimeException.class,
                error -> assertThat(error.getStatus().getCode()).isEqualTo(code));
    }
}
