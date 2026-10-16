package fiction.brisa27.replenishment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class NeedsCache {
  private static final TypeReference<List<Map<String, Object>>> TYPE = new TypeReference<>() {};

  private final StringRedisTemplate redis;
  private final ObjectMapper mapper;
  private final Duration ttl;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong misses = new AtomicLong();
  private final AtomicLong dbLoads = new AtomicLong();
  private final AtomicLong redisErrors = new AtomicLong();
  private final AtomicLong invalidations = new AtomicLong();

  public NeedsCache(
      StringRedisTemplate redis,
      ObjectMapper mapper,
      @Value("${lab.needs-cache.ttl-seconds:60}") long ttlSeconds) {
    this.redis = redis;
    this.mapper = mapper;
    this.ttl = Duration.ofSeconds(ttlSeconds);
  }

  static String epochKey(String store) {
    return "needs:epoch:" + store;
  }

  static String valueKey(String store, String epoch) {
    return "needs:v1:" + store + ":" + epoch;
  }

  public List<Map<String, Object>> getOrLoad(
      String store, Supplier<List<Map<String, Object>>> loader) {
    String epoch = currentEpoch(store);
    try {
      String cached = redis.opsForValue().get(valueKey(store, epoch));
      if (cached != null) {
        hits.incrementAndGet();
        return mapper.readValue(cached, TYPE);
      }
      misses.incrementAndGet();
    } catch (Exception e) {
      redisErrors.incrementAndGet();
    }

    dbLoads.incrementAndGet();
    List<Map<String, Object>> loaded = loader.get();
    try {
      String epochAfter = currentEpoch(store);
      if (epoch.equals(epochAfter)) {
        redis.opsForValue().set(valueKey(store, epoch), mapper.writeValueAsString(loaded), ttl);
      }
    } catch (Exception e) {
      redisErrors.incrementAndGet();
    }
    return loaded;
  }

  public void invalidate(String store) {
    invalidations.incrementAndGet();
    try {
      String epoch = currentEpoch(store);
      redis.delete(valueKey(store, epoch));
      redis.opsForValue().increment(epochKey(store));
    } catch (Exception e) {
      redisErrors.incrementAndGet();
    }
  }

  private String currentEpoch(String store) {
    try {
      String epoch = redis.opsForValue().get(epochKey(store));
      return epoch == null ? "0" : epoch;
    } catch (Exception e) {
      redisErrors.incrementAndGet();
      return "0";
    }
  }

  public Map<String, Object> stats() {
    return Map.of(
        "hits", hits.get(),
        "misses", misses.get(),
        "db_loads", dbLoads.get(),
        "redis_errors", redisErrors.get(),
        "invalidations", invalidations.get(),
        "ttl_seconds", ttl.toSeconds(),
        "key_pattern", "needs:v1:{store}:{epoch}");
  }

  public void resetStats() {
    hits.set(0);
    misses.set(0);
    dbLoads.set(0);
    redisErrors.set(0);
    invalidations.set(0);
  }
}
