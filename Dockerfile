# Both Temurin images publish linux/arm64 and linux/amd64 manifests.
FROM eclipse-temurin:21-jdk-jammy AS build
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
COPY src/ src/
RUN ./mvnw -B -ntp -DskipTests package

FROM eclipse-temurin:21-jre-jammy AS runtime
RUN groupadd --system --gid 10001 fleet && useradd --system --uid 10001 --gid fleet fleet
WORKDIR /app
COPY --from=build --chown=fleet:fleet /build/target/ttc-fleet-service-0.1.0.jar /app/app.jar
USER fleet
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -XX:ActiveProcessorCount=2 -Duser.timezone=UTC"
EXPOSE 50051
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD java -Xmx64m -Dloader.main=ca.ttc.fleet.grpc.HealthProbe -cp /app/app.jar org.springframework.boot.loader.launch.PropertiesLauncher
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
