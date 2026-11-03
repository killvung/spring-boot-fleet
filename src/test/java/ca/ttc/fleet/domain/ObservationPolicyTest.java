package ca.ttc.fleet.domain;

import static ca.ttc.fleet.TestFixtures.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservationPolicyTest {
    @Test
    void rejectsStaleAndDuplicateObservationsAndAcceptsNewVehicles() {
        var incoming = List.of(position("1", "501", 20), position("1", "501", 10),
                position("1", "501", 5), position("1", "501", 5), position("2", "504", 50));
        var accepted = ObservationPolicy.acceptNewer(incoming, Map.of("1", NOW.minusSeconds(10)));
        assertThat(accepted).extracting(Observation::vehicleId).containsExactly("2", "1");
        assertThat(accepted.getLast().sourceTimestamp()).isEqualTo(NOW.minusSeconds(5));
    }

    @Test
    void ordersWithinPollAndRetainsEveryDistinctNewerObservation() {
        var a = position("1", "501", 1);
        var b = position("1", "501", 2);
        var accepted = ObservationPolicy.acceptNewer(List.of(a, b, a), Map.of());
        assertThat(accepted).containsExactly(b, a);
        assertThat(ObservationPolicy.acceptNewer(accepted, Map.of("1", a.sourceTimestamp()))).isEmpty();
    }
}
