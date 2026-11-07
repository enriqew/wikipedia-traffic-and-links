package wikihadoop;

/**
 * This project's rule for a spike that does not look like people reading (plan section 12). It is
 * not Wikimedia's classification, and a flagged spike is not called a bot.
 *
 * <p>People read in peaks, whatever their time zone; a program fetches at an even rate. So a spike
 * is flagged as <b>flat</b> when its 24 hourly user views vary less than half as much as its wiki's
 * do that day, variation measured as the coefficient of variation (standard deviation over mean).
 * Comparing with the wiki matters because wikis differ: on 28 October 2026 German Wikipedia's
 * busiest hour had 10.6 times its quietest hour's user views and English only 1.7. A spike is
 * flagged for its <b>access</b> when at least {@link #ONE_ACCESS} of its user views came through one
 * access method that has under half of the wiki's user views.
 *
 * <p>(Changed 2026-11-07.) The first version flagged a spike whose hours did not correlate with its
 * wiki's (Pearson below 0.3). On 1 to 10 October that flagged 2,540 of 8,264 spikes, most of them
 * people: English Wikipedia's Asian Games pages read during the day in India, US news read in the
 * American evening. Flatness flags readers in another time zone only if they read evenly around
 * the clock.
 */
public final class LooksAutomated {

  public static final double FLAT_FRACTION = 0.5;
  public static final double ONE_ACCESS = 0.95;

  private LooksAutomated() {}

  /** Standard deviation over mean of the hourly counts; zero when there are none. */
  public static double variation(long[] hours) {
    double mean = 0;
    for (long h : hours) {
      mean += h;
    }
    mean /= hours.length;
    if (mean == 0) {
      return 0;
    }
    double var = 0;
    for (long h : hours) {
      var += (h - mean) * (h - mean);
    }
    return Math.sqrt(var / hours.length) / mean;
  }

  /** The spike's variation as a fraction of its wiki's; infinite if the wiki's hours are all equal. */
  public static double flatness(long[] spike, long[] wiki) {
    double w = variation(wiki);
    return w == 0 ? Double.POSITIVE_INFINITY : variation(spike) / w;
  }

  /** The verdict for one spike: which tests flagged it, or {@code -} when none did. */
  public static String flags(double flatness, double dominantShare, double wikiShareOfIt) {
    boolean flat = flatness < FLAT_FRACTION;
    boolean access = dominantShare >= ONE_ACCESS && wikiShareOfIt < 0.5;
    return flat && access ? "flat+access" : flat ? "flat" : access ? "access" : "-";
  }
}
