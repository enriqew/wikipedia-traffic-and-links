package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.Random;
import org.junit.jupiter.api.Test;

class PageRankMassTest {

  private static long floor85(long r) {
    return BigInteger.valueOf(r).multiply(BigInteger.valueOf(85)).divide(BigInteger.valueOf(100)).longValueExact();
  }

  @Test
  void dampedIsTheFloorOf85PercentForAnyLong() {
    assertEquals(85, PageRankMass.damped(100));
    assertEquals(84, PageRankMass.damped(99));   // 84.15
    assertEquals(0, PageRankMass.damped(1));
    assertEquals(floor85(Long.MAX_VALUE), PageRankMass.damped(Long.MAX_VALUE));  // r * 85 would overflow
    Random random = new Random(1);
    for (int i = 0; i < 100_000; i++) {
      long r = random.nextLong() & Long.MAX_VALUE;
      assertEquals(floor85(r), PageRankMass.damped(r));
    }
  }

  @Test
  void whatIsNotPassedOnGoesToThePool() {
    long r = 1_000_000_000L;
    assertEquals(850_000_000L / 3, PageRankMass.perLink(r, 3));
    assertEquals(r - 3 * (850_000_000L / 3), PageRankMass.toPool(r, 3));  // the 15% and the remainder
    assertEquals(0, PageRankMass.perLink(r, 0));
    assertEquals(r, PageRankMass.toPool(r, 0));  // no out-links: all of it
    for (int k = 1; k < 5000; k += 7) {
      assertEquals(r, PageRankMass.perLink(r, k) * k + PageRankMass.toPool(r, k));
    }
  }
}
