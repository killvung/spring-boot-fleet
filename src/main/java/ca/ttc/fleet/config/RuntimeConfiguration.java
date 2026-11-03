package ca.ttc.fleet.config;

import io.grpc.BindableService;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.net.http.HttpClient;
import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.task.SimpleAsyncTaskSchedulerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.server.ServerBuilderCustomizer;

@Configuration(proxyBeanMethods = false)
public class RuntimeConfiguration {
    @Bean
    BindableService reflectionService() {
        return ProtoReflectionServiceV1.newInstance();
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean(name = "fleetVirtualExecutor", destroyMethod = "close")
    ExecutorService fleetVirtualExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean(destroyMethod = "close")
    HttpClient jdkFeedHttpClient(TtcProperties properties,
            @Qualifier("fleetVirtualExecutor") ExecutorService executor) {
        return HttpClient.newBuilder().connectTimeout(properties.connectTimeout())
                .executor(executor).followRedirects(HttpClient.Redirect.NORMAL).build();
    }

    @Bean
    <T extends ServerBuilder<T>> ServerBuilderCustomizer<T> virtualGrpcExecutor(
            @Qualifier("fleetVirtualExecutor") ExecutorService executor) {
        return builder -> builder.executor(executor);
    }

    @Bean
    SimpleAsyncTaskSchedulerCustomizer gracefulScheduledTasks() {
        return scheduler -> scheduler.setTaskTerminationTimeout(25_000);
    }
}
