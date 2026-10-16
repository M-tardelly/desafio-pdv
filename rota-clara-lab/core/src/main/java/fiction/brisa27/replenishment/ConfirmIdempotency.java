package fiction.brisa27.replenishment;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ConfirmIdempotency {
  private ConfirmIdempotency() {}

  public static boolean matchesPrevious(
      List<Map<String, Object>> items, Map<String, Integer> received) {
    if (items.size() != received.size()) return false;
    for (var item : items) {
      String product = (String) item.get("product_id");
      Object previous = item.get("received");
      Integer amount = received.get(product);
      if (previous == null || amount == null) return false;
      if (((Number) previous).intValue() != amount) return false;
    }
    return true;
  }

  public static boolean samePayload(
      Map<String, Integer> first, Map<String, Integer> second) {
    return Objects.equals(first, second);
  }
}
