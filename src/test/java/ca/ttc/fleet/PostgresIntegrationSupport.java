package ca.ttc.fleet;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

@SpringBootTest(properties = { "ttc.ingestion-enabled=false", "ttc.metrics-logging-enabled=false",
        "spring.grpc.server.port=0" })
public abstract class PostgresIntegrationSupport {
    // Shared for the test JVM; Testcontainers/Ryuk removes it at JVM exit.
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.11-bookworm");
    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl() + "&reWriteBatchedInserts=true");
        properties.add("spring.datasource.username", POSTGRES::getUsername);
        properties.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
