package fiction.brisa27.replenishment;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class ConfirmIdempotencyTest {
  @Test
  void acceptsIdenticalRetry() {
    var items =
        List.of(
            Map.<String, Object>of("product_id", "agua-500", "requested", 6, "received", 4));
    assertTrue(ConfirmIdempotency.matchesPrevious(items, Map.of("agua-500", 4)));
  }

  @Test
  void rejectsDivergentRetry() {
    var items =
        List.of(
            Map.<String, Object>of("product_id", "agua-500", "requested", 6, "received", 4));
    assertFalse(ConfirmIdempotency.matchesPrevious(items, Map.of("agua-500", 5)));
  }

  @Test
  void samePayloadDetectsEquality() {
    assertTrue(ConfirmIdempotency.samePayload(Map.of("agua-500", 4), Map.of("agua-500", 4)));
    assertFalse(ConfirmIdempotency.samePayload(Map.of("agua-500", 4), Map.of("agua-500", 3)));
  }
}
