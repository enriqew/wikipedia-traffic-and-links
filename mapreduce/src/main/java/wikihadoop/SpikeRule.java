package wikihadoop;

import java.util.Arrays;

/**
 * When a day is a reading spike for an article (plan section 12, approved 2026-11-07).
 *
 * <p>The baseline for day {@code d} is the median of the article's user views over the window's
 * other days, leaving out {@code d - 1}, {@code d} and {@code d + 1}, so that a two-day story does
 * not raise its own baseline; days without a row count as zero views. A day is a spike when its
 * views are at least {@link #FACTOR} times the baseline (a baseline under one counts as one) and at
 * least {@link #FLOOR_SHARE} of the wiki's user views that day. Spikes are ranked by excess, views
 * minus baseline. The median uses later days as well as earlier ones: the rule is retrospective.
 */
public final class SpikeRule {

  public static final double FACTOR = 10.0;
  public static final double FLOOR_SHARE = 1e-4;

  private SpikeRule() {}

  /** The median of {@code daily} without days {@code d - 1}, {@code d} and {@code d + 1}. */
  public static double baseline(long[] daily, int d) {
    long[] rest = new long[daily.length];
    int n = 0;
    for (int i = 0; i < daily.length; i++) {
      if (Math.abs(i - d) > 1) {
        rest[n++] = daily[i];
      }
    }
    if (n == 0) {
      return 0;
    }
    Arrays.sort(rest, 0, n);
    return n % 2 == 1 ? rest[n / 2] : (rest[n / 2 - 1] + rest[n / 2]) / 2.0;
  }

  /** The fewest views a spike can have on a wiki with {@code wikiViews} user views that day. */
  public static double floor(long wikiViews) {
    return wikiViews * FLOOR_SHARE;
  }

  public static boolean isSpike(long views, double baseline, long wikiViews) {
    return views >= FACTOR * Math.max(baseline, 1.0) && views >= floor(wikiViews);
  }
}
