package ca.ttc.fleet.ingestion;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

public final class PollBackoff {
    private final Duration interval;
    private final DoubleSupplier random;
    private int failures;

    public PollBackoff(Duration interval) {
        this(interval, () -> ThreadLocalRandom.current().nextDouble());
    }

    PollBackoff(Duration interval, DoubleSupplier random) {
        this.interval = interval;
        this.random = random;
    }

    public Duration success() {
        failures = 0;
        return interval;
    }

    public Duration failure() {
        failures = Math.min(failures + 1, 20);
        long cap = Math.min(300_000L, interval.toMillis() * (1L << failures));
        // Equal jitter: half to all of the exponentially growing cap, never above five
        // minutes.
        return Duration.ofMillis((long) (cap * (0.5 + 0.5 * random.getAsDouble())));
    }
}
