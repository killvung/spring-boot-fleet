package ca.ttc.fleet.ingestion;

import ca.ttc.fleet.config.TtcProperties;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class FeedHttpClient {
    private final HttpClient client;
    private final TtcProperties properties;
    private final ExecutorService executor;

    public FeedHttpClient(HttpClient client, TtcProperties properties,
            @Qualifier("fleetVirtualExecutor") ExecutorService executor) {
        this.client = client;
        this.properties = properties;
        this.executor = executor;
    }

    public byte[] fetch() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(properties.feedUrl()).timeout(properties.readTimeout())
                .header("Accept", "application/x-protobuf, application/octet-stream")
                .header("User-Agent", "ttc-fleet-service/0.1.0").GET().build();
        // JDK request timeouts alone may finish timing at the response headers. Enforce
        // a wall-clock
        // deadline covering the entire bounded body, using one cancellable
        // virtual-thread task.
        var pending = executor.submit(() -> client.send(request, info -> new LimitedBody(properties.maxFeedBytes())));
        try {
            var response = pending.get(properties.readTimeout().toNanos(), TimeUnit.NANOSECONDS);
            if (response.statusCode() != 200)
                throw new IOException("Feed HTTP status " + response.statusCode());
            return response.body();
        } catch (TimeoutException timeout) {
            pending.cancel(true);
            throw new HttpTimeoutException("Feed response deadline exceeded");
        } catch (InterruptedException interrupted) {
            pending.cancel(true);
            throw interrupted;
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof IOException io)
                throw io;
            throw new IOException("Feed request failed", failed.getCause());
        }
    }

    /**
     * The JDK subscriber API is required to enforce a body cap before allocation.
     * The caller still blocks.
     */
    private static final class LimitedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
        private final long maximum;
        private long received;
        private Flow.Subscription subscription;
        private boolean failed;

        private LimitedBody(long maximum) {
            this.maximum = maximum;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return delegate.getBody();
        }

        @Override
        public void onSubscribe(Flow.Subscription value) {
            subscription = value;
            delegate.onSubscribe(value);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (failed)
                return;
            for (var buffer : buffers)
                received += buffer.remaining();
            if (received > maximum) {
                failed = true;
                subscription.cancel();
                delegate.onError(new IOException("Feed exceeds configured byte limit"));
            } else {
                delegate.onNext(buffers);
            }
        }

        @Override
        public void onError(Throwable error) {
            if (!failed)
                delegate.onError(error);
        }

        @Override
        public void onComplete() {
            if (!failed)
                delegate.onComplete();
        }
    }
}
