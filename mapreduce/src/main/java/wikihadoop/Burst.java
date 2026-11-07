package wikihadoop;

/**
 * A spike that came almost all in one hour: at least {@link #SHARE} of its day's user views in its
 * busiest hour.
 *
 * <p>Shown beside a spike and kept out of {@link LooksAutomated}, because the same shape has two
 * causes this data cannot tell apart. In October 2026 English Wikipedia's "Ying" went from a
 * baseline of 4.5 views a day to 886,855, all but two of them between 17:00 and 18:00 UTC and 90%
 * from desktop, which no reading public does; and some Japanese Wikipedia biographies took over
 * 90% of a day's views in one hour from phones, as readers reacting to one moment would.
 */
public final class Burst {

  public static final double SHARE = 0.9;

  private Burst() {}

  /** The busiest hour's share of the day's views; zero when there are none. */
  public static double peakShare(long[] hours) {
    long total = 0;
    long peak = 0;
    for (long h : hours) {
      total += h;
      peak = Math.max(peak, h);
    }
    return total == 0 ? 0 : (double) peak / total;
  }

  public static boolean is(double peakShare) {
    return peakShare >= SHARE;
  }
}
