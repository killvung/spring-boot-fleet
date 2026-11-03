package ca.ttc.fleet.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ObservationPolicy {
    private ObservationPolicy() {
    }

    /**
     * Accept each distinct newer timestamp, including multiple observations in one
     * poll.
     */
    public static List<Observation> acceptNewer(List<Observation> incoming, Map<String, Instant> stored) {
        var newest = new HashMap<>(stored);
        var accepted = new ArrayList<Observation>();
        incoming.stream().sorted(Comparator.comparing(Observation::sourceTimestamp)
                .thenComparing(Observation::vehicleId)).forEach(position -> {
                    Instant previous = newest.get(position.vehicleId());
                    if (previous == null || position.sourceTimestamp().isAfter(previous)) {
                        accepted.add(position);
                        newest.put(position.vehicleId(), position.sourceTimestamp());
                    }
                });
        return List.copyOf(accepted);
    }
}
