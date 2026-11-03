package ca.ttc.fleet.ingestion;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class FeedHttpClientTest {
    @Test
    void handlesBinaryNon200OversizeAndTimeout() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        try (var threads = Executors.newVirtualThreadPerTaskExecutor();
                var http = HttpClient.newBuilder().executor(threads).connectTimeout(Duration.ofSeconds(1)).build()) {
            byte[] payload = feed().addEntity(entity("1")).build().toByteArray();
            server.setExecutor(threads);
            server.createContext("/ok", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, payload.length);
                    exchange.getResponseBody().write(payload);
                }
            });
            server.createContext("/error", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(503, -1);
                }
            });
            server.createContext("/large", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, 2048);
                    exchange.getResponseBody().write(new byte[2048]);
                }
            });
            server.createContext("/slow", exchange -> {
                try (exchange) {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.sendResponseHeaders(200, -1);
                }
            });
            server.createContext("/slow-body", exchange -> {
                try (exchange) {
                    exchange.sendResponseHeaders(200, 100);
                    exchange.getResponseBody().write(1);
                    exchange.getResponseBody().flush();
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            server.start();
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            assertThat(new FeedHttpClient(http, properties(URI.create(base + "/ok")), threads).fetch())
                    .isEqualTo(payload);
            assertThatThrownBy(() -> new FeedHttpClient(http, properties(URI.create(base + "/error")), threads).fetch())
                    .isInstanceOf(IOException.class).hasMessageContaining("503");
            assertThatThrownBy(() -> new FeedHttpClient(http, properties(URI.create(base + "/large")), threads).fetch())
                    .isInstanceOf(IOException.class);
            assertThatThrownBy(() -> new FeedHttpClient(http, properties(URI.create(base + "/slow")), threads).fetch())
                    .isInstanceOf(java.net.http.HttpTimeoutException.class);
            assertThatThrownBy(
                    () -> new FeedHttpClient(http, properties(URI.create(base + "/slow-body")), threads).fetch())
                    .isInstanceOf(java.net.http.HttpTimeoutException.class);
        } finally {
            server.stop(0);
        }
    }
}
