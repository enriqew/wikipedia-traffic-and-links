package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LooksAutomatedTest {

  private static long[] cycle(long low, long high) {
    long[] h = new long[24];
    for (int i = 0; i < 24; i++) {
      h[i] = i >= 8 && i < 22 ? high : low;  // awake from 08:00 to 22:00
    }
    return h;
  }

  @Test
  void aSpikeThatFollowsItsWikiIsNotFlagged() {
    double r = LooksAutomated.correlation(cycle(100, 1000), cycle(10_000, 100_000));
    assertEquals(1.0, r, 1e-12);
    assertEquals("-", LooksAutomated.flags(r, 0.6, 0.35));
  }

  @Test
  void aSpikeAgainstItsWikisCycleIsFlaggedForShape() {
    long[] night = new long[24];
    for (int i = 0; i < 24; i++) {
      night[i] = i < 6 ? 5000 : 10;  // all of it between midnight and six
    }
    double r = LooksAutomated.correlation(night, cycle(10_000, 100_000));
    assertEquals("shape", LooksAutomated.flags(r, 0.6, 0.35));
  }

  @Test
  void aFlatDayHasNoCorrelationAndCountsAsZero() {
    long[] flat = new long[24];
    java.util.Arrays.fill(flat, 700);
    assertEquals(0.0, LooksAutomated.correlation(flat, cycle(1, 9)));
    assertEquals("shape", LooksAutomated.flags(0.0, 0.5, 0.4));
  }

  @Test
  void oneAccessMethodCountsOnlyWhereItIsAMinority() {
    assertEquals("access", LooksAutomated.flags(0.9, 0.97, 0.35));  // all desktop where desktop is 35%
    assertEquals("-", LooksAutomated.flags(0.9, 0.97, 0.65));       // all desktop where desktop is 65%
    assertEquals("-", LooksAutomated.flags(0.9, 0.90, 0.35));       // under 95%
    assertEquals("shape+access", LooksAutomated.flags(0.1, 0.99, 0.2));
  }
}
