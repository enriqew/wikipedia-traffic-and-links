package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class LooksAutomatedTest {

  private static long[] awake(int from, int to, long low, long high) {
    long[] h = new long[24];
    for (int i = 0; i < 24; i++) {
      h[i] = i >= from && i < to ? high : low;
    }
    return h;
  }

  private static final long[] WIKI = awake(8, 22, 100_000, 1_000_000);  // awake 08:00 to 22:00 UTC

  @Test
  void aSpikeThatFollowsItsWikiIsNotFlat() {
    double f = LooksAutomated.flatness(awake(8, 22, 100, 1000), WIKI);
    assertEquals(1.0, f, 1e-12);
    assertEquals("-", LooksAutomated.flags(f, 0.6, 0.35));
  }

  @Test
  void readersInAnotherTimeZoneAreNotFlat() {
    // Read from 01:00 to 09:00 UTC, the day in Asia, on a wiki whose day is Europe's and America's.
    double f = LooksAutomated.flatness(awake(1, 9, 0, 5000), WIKI);
    assertTrue(f > 1, "a peak elsewhere is still a peak: " + f);
    assertEquals("-", LooksAutomated.flags(f, 0.8, 0.6));
  }

  @Test
  void anEvenDayIsFlat() {
    long[] even = new long[24];
    Arrays.fill(even, 700);
    assertEquals(0.0, LooksAutomated.variation(even));
    assertEquals(0.0, LooksAutomated.flatness(even, WIKI));
    assertEquals("flat", LooksAutomated.flags(0.0, 0.5, 0.4));
  }

  @Test
  void aWikiWithEqualHoursMakesNothingFlat() {
    long[] even = new long[24];
    Arrays.fill(even, 5);
    assertEquals(Double.POSITIVE_INFINITY, LooksAutomated.flatness(even, even));
  }

  @Test
  void oneAccessMethodCountsOnlyWhereItIsAMinority() {
    assertEquals("access", LooksAutomated.flags(0.9, 0.97, 0.35));  // all desktop where desktop is 35%
    assertEquals("-", LooksAutomated.flags(0.9, 0.97, 0.65));       // all desktop where desktop is 65%
    assertEquals("-", LooksAutomated.flags(0.9, 0.90, 0.35));       // under 95%
    assertEquals("flat+access", LooksAutomated.flags(0.1, 0.99, 0.2));
  }
}
