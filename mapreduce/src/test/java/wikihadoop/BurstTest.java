package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class BurstTest {

  @Test
  void aDayInOneHourIsABurst() {
    long[] hours = new long[24];
    hours[17] = 886_853;
    hours[1] = 1;
    hours[9] = 1;
    double share = Burst.peakShare(hours);
    assertEquals(886_853.0 / 886_855, share, 1e-12);
    assertTrue(Burst.is(share));
  }

  @Test
  void anEvenDayIsNot() {
    long[] hours = new long[24];
    Arrays.fill(hours, 100);
    assertEquals(1.0 / 24, Burst.peakShare(hours), 1e-12);
    assertFalse(Burst.is(Burst.peakShare(hours)));
  }

  @Test
  void theThresholdIsInclusive() {
    long[] hours = new long[24];
    hours[3] = 90;
    hours[4] = 10;
    assertTrue(Burst.is(Burst.peakShare(hours)));
    hours[4] = 11;
    assertFalse(Burst.is(Burst.peakShare(hours)));
  }

  @Test
  void noViewsIsNoBurst() {
    assertEquals(0, Burst.peakShare(new long[24]));
    assertFalse(Burst.is(0));
  }
}
