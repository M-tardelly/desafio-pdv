package fiction.brisa27.replenishment;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NeedsCacheTest {
  @Test
  void keysSeparateStoresAndEpochs() {
    assertEquals("needs:epoch:loja-aurora", NeedsCache.epochKey("loja-aurora"));
    assertEquals("needs:epoch:loja-lagoa", NeedsCache.epochKey("loja-lagoa"));
    assertEquals("needs:v1:loja-aurora:0", NeedsCache.valueKey("loja-aurora", "0"));
    assertEquals("needs:v1:loja-lagoa:3", NeedsCache.valueKey("loja-lagoa", "3"));
    assertNotEquals(
        NeedsCache.valueKey("loja-aurora", "1"), NeedsCache.valueKey("loja-lagoa", "1"));
  }
}
