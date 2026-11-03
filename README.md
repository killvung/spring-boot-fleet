# ttc-fleet-service

A microservice for something bigger, eventually...

Java 21 / Spring Boot 4.1.2 service that polls TTC's binary GTFS-Realtime feed and serves protobuf over gRPC. PostgreSQL 17 stores append-only history and a separate latest row per vehicle. No REST server, JSON data serialization, raw feed storage, or frontend.

## Run with Docker Compose

Install Docker Engine with Compose v2, then:

```sh
cp .env.example .env
# Edit .env and replace DB_PASSWORD with a strong password.
docker compose up -d
docker compose ps
docker compose logs -f app
```

The application starts after PostgreSQL is healthy and runs Flyway automatically. Only TCP 50051 is published. PostgreSQL uses the internal `database` network and the named `postgres_data` volume. The app also has an outbound network for TTC HTTPS access. Both containers restart unless stopped. `docker compose down` preserves data; adding `--volumes` destroys it.

The multi-stage Dockerfile builds on ARM64 natively on the target A1 VM; it also works on AMD64. To build ARM64 explicitly on a supported Docker builder:

```sh
docker build --platform linux/arm64 -t ttc-fleet-service:0.1.0 .
```

## Local development and tests

Install JDK 21 and `unzip` (on Linux, `sudo apt-get install unzip`). Maven 3.9.11 is downloaded by the included, checksum-pinned Maven Wrapper; no system Maven is required. Docker must be running for the PostgreSQL integration tests.

```sh
./mvnw test                  # Unit tests, including a local HTTP test server
./mvnw verify                # Unit + real PostgreSQL + in-process/Netty gRPC tests
./mvnw -DskipITs package     # Build with unit tests when Docker is unavailable
```

`verify` fails if Docker is unavailable; integration tests are never silently skipped. Testcontainers supplies disposable PostgreSQL 17.11 instances and Flyway applies the real migration. Tests cover decoding/presence/timestamp fallback, HTTP errors/body deadlines/size caps, backoff, deduplication, rollback, concurrent polls, append-only protection, retention, filters, time ranges, pagination, limits, statuses, gRPC health, and actual virtual-thread execution.

For a JVM process outside Compose, start a separate development database with a **loopback-only** port:

```sh
export DB_PASSWORD='choose-a-local-development-password'
docker run -d --name ttc-fleet-dev-db \
  -e POSTGRES_DB=ttc_fleet -e POSTGRES_USER=ttc_fleet \
  -e POSTGRES_PASSWORD="$DB_PASSWORD" \
  -p 127.0.0.1:5432:5432 -v ttc_fleet_dev_data:/var/lib/postgresql/data \
  postgres:17.11-bookworm
./mvnw spring-boot:run
# Or, after packaging:
java -jar target/ttc-fleet-service-0.1.0.jar
```

Compose reads `.env`; Spring Boot launched directly does not. Export the desired variables for local JVM runs. Inspect the production database through its container, without publishing a database port:

```sh
docker compose exec postgres psql -U ttc_fleet -d ttc_fleet
```

## gRPC calls

Install [grpcurl](https://github.com/fullstorydev/grpcurl). Reflection is enabled. Always use `-format text`: input/output are protobuf TextFormat at the CLI, while transport is binary protobuf. No JSON transcoder is installed.

```sh
grpcurl -plaintext localhost:50051 list

grpcurl -plaintext -format text -d 'limit: 10' localhost:50051 \
  ttc.fleet.v1.FleetService/GetLatestVehiclePositions

grpcurl -plaintext -format text -d 'route_id: "501" direction_id: 0 limit: 20' \
  localhost:50051 ttc.fleet.v1.FleetService/GetLatestVehiclePositions

# Replace vehicle ID and epoch seconds with the desired vehicle and UTC interval.
grpcurl -plaintext -format text \
  -d 'vehicle_id: "4400" from { seconds: 1793664000 } to { seconds: 1793750400 } limit: 100' \
  localhost:50051 ttc.fleet.v1.FleetService/GetVehicleHistory

grpcurl -plaintext -format text -d 'route_id: "501" limit: 100' localhost:50051 \
  ttc.fleet.v1.FleetService/GetRouteSnapshot

grpcurl -plaintext -format text -d 'service: "ttc.fleet.v1.FleetService"' \
  localhost:50051 grpc.health.v1.Health/Check
```

API semantics:

- `GetLatestVehiclePositions` queries only `latest_vehicle_positions`; latest means last accepted observation, even if a vehicle has disappeared from subsequent feeds. Check `source_timestamp` for freshness.
- `GetVehicleHistory` queries only `vehicle_positions`. All filters are optional and combined with AND. `from` is inclusive; `to` is exclusive. Both use protobuf `Timestamp` in UTC.
- Zero/omitted `limit` means the configured maximum, initially 1,000. Explicit limits above the maximum are `INVALID_ARGUMENT`; the maximum cannot be configured above 1,000. Each stream is finite and buffered at most to that bound. Streams are ordered, not live subscriptions.
- Latest results sort by vehicle ID ascending. Continue with `after_vehicle_id` equal to the final returned vehicle. Route snapshots use the same cursor, with explicit `truncated` and `next_after_vehicle_id` fields.
- History sorts by source timestamp descending, then vehicle ID ascending. Continue with **both** `before_source_timestamp` and `after_vehicle_id` from the final record, preserving all original filters. Equal timestamps across different vehicles are handled without omissions. Stop after a short/empty page. Concurrent ingestion can change later pages; cursors do not create a multi-request database snapshot.
- Invalid/blank IDs, invalid direction (must be 0 or 1), malformed timestamps, inverted time ranges, or incomplete cursors return `INVALID_ARGUMENT`. No matches for a specific vehicle or an initial route snapshot return `NOT_FOUND`. Other empty collection queries and exhausted cursor pages complete normally. Database failures return `UNAVAILABLE`.
- Optional protobuf fields retain presence; absent direction/speed/occupancy is not converted to zero. Speed is metres/second, bearing is degrees, occupancy is the GTFS enum name, and stop sequence is unsigned 32-bit.

## Configuration

Defaults live in `src/main/resources/application.yml`. `grpc.server.port` is explicitly bridged to Spring Boot's `spring.grpc.server.port`.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `GRPC_SERVER_PORT` | `50051` | Container and published gRPC port |
| `DB_PASSWORD` | required | Database password |
| `DB_USERNAME` | `ttc_fleet` | JVM database username; Compose uses the same fixed role |
| `DB_URL` | local PostgreSQL JDBC URL | JVM database address; Compose supplies its internal URL |
| `DB_POOL_SIZE` | `8` | JDBC connection cap |
| `TTC_FEED_URL` | `https://bustime.ttc.ca/gtfsrt/vehicles` | Binary GTFS-RT source |
| `TTC_POLL_INTERVAL` | `20s` | Delay after a completed successful poll |
| `TTC_CONNECT_TIMEOUT` | `5s` | HTTP connection timeout |
| `TTC_READ_TIMEOUT` | `10s` | Entire HTTP call deadline, including body transfer |
| `TTC_RETENTION_DAYS` | `30` | History retention by source timestamp |
| `TTC_CLEANUP_CRON` | `0 0 * * * *` | Hourly cleanup, UTC |
| `TTC_MAX_RECORDS` | `1000` | Maximum records per API response (1–1,000) |
| `TTC_MAX_FEED_BYTES` | `8388608` | Bounded HTTP body (8 MiB) |
| `TTC_INGESTION_ENABLED` | `true` | Disable scheduled polling for maintenance/tests |
| `TTC_METRICS_LOGGING_ENABLED` | `true` | Log Micrometer meters each minute |

Durations must be positive and at most five minutes. To add JVM-only variables to Compose, put them in the app's `environment` map; a `.env` variable is not automatically passed to the container unless referenced there. Restart/recreate the app after changing configuration. Changing `POSTGRES_PASSWORD` does not change an already-initialized database role's password.

## Correctness and operations

The official, unmodified `gtfs-realtime.proto` is vendored with its license and checksum in `NOTICE`. Both schemas generate Java classes during Maven compilation. Polling decodes binary data directly; all stored fields are SQL scalars and `TIMESTAMPTZ` values. Nothing persists raw protobuf blobs.

A poll first validates the complete feed, then enters one database transaction. A transaction-scoped PostgreSQL advisory lock serializes ingestion writers. Under READ COMMITTED it reads existing vehicle high-water marks, sorts incoming observations, rejects equal/older timestamps, batch-inserts every accepted observation, then batch-upserts one newest row per vehicle using `ON CONFLICT (vehicle_id) DO UPDATE`. A guarded upsert and unique history key provide database defenses. Any database error rolls back both tables. Repeating the same poll inserts nothing, including after process restart. Multiple distinct newer timestamps for a vehicle in one feed are retained in history.

Vehicle timestamp takes precedence over the header. Invalid IDs/coordinates/direction/speed/bearing and missing timestamps are rejected per entity and counted; a feed with invalid vehicles and no valid observations fails. Timestamps beyond five minutes into the future are rejected to protect the high-water mark. A bad header, malformed protobuf, unsupported differential feed, empty HTTP body, non-200 response, oversized response, timeout, or database error fails the poll. A valid feed with zero entities succeeds without changing positions. Missing vehicles and failed polls never delete or overwrite latest data.

Successful polling waits the configured interval after completion. A one-second scheduling tick gates attempts, so dispatch can be up to one second late. Repeated failures use equal-jitter exponential backoff: delay is between half and all of `min(300s, interval × 2^failures)`; success resets it. Overlapping polls are suppressed. HTTP cancellation covers stalled bodies as well as headers, and allocation is capped before buffering the body.

History updates are rejected by a database trigger. Retention deletes expired history in 10,000-row transactions, leaving latest rows intact so historical high-water marks survive cleanup. Latest and history queries have vehicle/time, route/time, and history time/cursor indexes. Leave PostgreSQL autovacuum enabled. Deleting rows frees space for reuse but does not immediately shrink the volume file.

Spring's scheduler dispatches ingestion and cleanup on virtual threads (`spring.threads.virtual.enabled=true`). A named `fleetVirtualExecutor` uses `Executors.newVirtualThreadPerTaskExecutor()` for gRPC handlers and HTTP work. JDBC calls execute synchronously on those virtual threads; Hikari bounds concurrent database connections. Netty I/O loops, the JDK HTTP selector, Hikari housekeeping, and Micrometer's single logging scheduler remain dependency-owned platform threads. There is no custom platform-thread worker pool. JDBC driver/native/TLS code may still briefly pin Java 21 carriers; use `-Djdk.tracePinnedThreads=full` or JFR when diagnosing stalls. Application hot paths have no synchronized blocks.

Netty drains requests for up to 20 seconds during shutdown; scheduled tasks have up to 25 seconds to finish. Compose gives the process 45 seconds. HTTP, JDBC statement, socket, connection, and lock waits are bounded. A transaction interrupted before commit rolls back; the next poll can retry it.

Logs use the logging framework's structured format. Every poll records feed timestamp, entity/rejection count, outcome, duration, and committed rows. Micrometer meters `ttc.ingestion.success`, `ttc.ingestion.failure`, `ttc.ingestion.poll.duration`, `ttc.ingestion.entities`, `ttc.ingestion.rows.inserted`, and `ttc.ingestion.stale.feed.age` are also logged each minute. Feed age measures the last successfully committed feed's header timestamp (or newest vehicle timestamp); it keeps increasing during failures and is NaN before the first timestamp. It is not a claim that every vehicle is fresh.

The standard unary gRPC health `Check` reports Spring readiness plus database connectivity. Upstream outages do not make retained data unavailable. `Watch` is not implemented. The container probe invokes `Check` with a Java client from the packaged JAR. There is no HTTP health/metrics listener. gRPC reflection v1 is available on the same port.

Version note: [Spring Boot 4.1.2](https://docs.spring.io/spring-boot/system-requirements.html) manages Spring gRPC 1.1.2. The requested `spring-grpc-spring-boot-starter` name last shipped at 1.0.3. The POM retains that starter's Netty dependencies, excludes its obsolete auto-configuration/client starter, and adds Boot 4.1's `spring-boot-starter-grpc-server` for supported auto-configuration. There is one Netty server. Dependency and plugin versions are pinned directly or by the pinned Boot parent/BOM; both protobuf compilers use platform-detected ARM64 binaries.

## Oracle Cloud Ubuntu deployment

Use an Ubuntu ARM64 VM with shape `VM.Standard.A1.Flex`, **2 OCPUs and 12 GB RAM**, a public IP, and outbound HTTPS access. The Compose limits allocate 2 GB to the application and 4 GB to PostgreSQL, leaving room for the OS and build. The JVM uses at most 60% of its container limit for heap.

Install Docker and Compose using Docker's [official Ubuntu repository](https://docs.docker.com/engine/install/ubuntu/):

```sh
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
. /etc/os-release
printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' \
  "$(dpkg --print-architecture)" "$VERSION_CODENAME" | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo docker compose version

git clone <your-repository-url> ttc-fleet-service
cd ttc-fleet-service
cp .env.example .env
chmod 600 .env
nano .env                         # Set DB_PASSWORD; adjust retention if needed
sudo docker compose up -d
sudo docker compose ps
sudo docker compose logs --tail=100 -f app
```

In OCI Console, open the VM subnet's security list (and any attached NSG): add a **stateful TCP ingress** rule for destination port **50051**, source equal to your clients' CIDR. Keep existing SSH access. Do not add PostgreSQL port 5432. In Ubuntu:

```sh
sudo ufw allow OpenSSH
sudo ufw allow 50051/tcp
sudo ufw enable
sudo ufw status
```

Use your configured port if different. Docker-published ports can bypass UFW; the OCI source-CIDR rule is the external access boundary. Use Docker's `DOCKER-USER` chain if host-level source filtering is also required. The MVP uses plaintext gRPC and has no authentication, so restrict the OCI rule to intended clients. Call the public IP with the grpcurl examples above.

Disk capacity is the main retention constraint: 2,000 newly timestamped vehicles every 20 seconds means up to 8.64 million observations/day and 259 million over 30 days. Actual volume depends on timestamp changes. Provision sufficient block storage for rows, indexes, WAL and maintenance headroom; measure `pg_database_size('ttc_fleet')` and lower `TTC_RETENTION_DAYS` if necessary. The named volume survives container replacement; it is not a backup. Take regular PostgreSQL backups appropriate to your deployment.

## Repository tree

```text
.
├── .dockerignore
├── .env.example
├── .gitignore
├── .mvn/wrapper/maven-wrapper.properties
├── Dockerfile
├── LICENSE-GTFS-Realtime
├── NOTICE
├── README.md
├── docker-compose.yml
├── mvnw
├── mvnw.cmd
├── pom.xml
└── src
    ├── main
    │   ├── java/ca/ttc/fleet
    │   │   ├── FleetApplication.java
    │   │   ├── config
    │   │   │   ├── MetricsConfiguration.java
    │   │   │   ├── RuntimeConfiguration.java
    │   │   │   └── TtcProperties.java
    │   │   ├── domain
    │   │   │   ├── Observation.java
    │   │   │   ├── ObservationPolicy.java
    │   │   │   └── PositionFilter.java
    │   │   ├── grpc
    │   │   │   ├── FleetGrpcService.java
    │   │   │   ├── FleetHealthService.java
    │   │   │   ├── HealthProbe.java
    │   │   │   └── ProtoMapper.java
    │   │   ├── ingestion
    │   │   │   ├── FeedDecoder.java
    │   │   │   ├── FeedHttpClient.java
    │   │   │   ├── IngestionJob.java
    │   │   │   ├── IngestionMetrics.java
    │   │   │   ├── PollBackoff.java
    │   │   │   └── RetentionJob.java
    │   │   └── persistence/PositionRepository.java
    │   ├── proto
    │   │   ├── gtfs-realtime.proto
    │   │   └── ttc_fleet.proto
    │   └── resources
    │       ├── application.yml
    │       └── db/migration/V1__vehicle_positions.sql
    └── test/java/ca/ttc/fleet
        ├── PostgresIntegrationSupport.java
        ├── TestFixtures.java
        ├── domain/ObservationPolicyTest.java
        ├── grpc
        │   ├── FleetGrpcIT.java
        │   └── RuntimeIT.java
        ├── ingestion
        │   ├── FeedDecoderTest.java
        │   ├── FeedHttpClientTest.java
        │   ├── IngestionJobTest.java
        │   └── PollBackoffTest.java
        └── persistence/PositionRepositoryIT.java
```

Generated Java protobuf classes, the executable JAR, and test reports live under `target/` and are intentionally not source-controlled.
