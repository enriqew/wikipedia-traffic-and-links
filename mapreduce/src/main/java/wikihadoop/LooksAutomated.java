package wikihadoop;

/**
 * This project's rule for a spike that does not look like people reading (plan section 12, approved
 * 2026-11-07). It is not Wikimedia's classification, and a flagged spike is not called a bot.
 *
 * <p>A flat day is not the test, because wikis differ: on 28 October 2026 German Wikipedia's busiest
 * hour had 10.6 times its quietest hour's user views and English only 1.7. So the spike is compared
 * with its own wiki that day. It is flagged for its <b>shape</b> when the Pearson correlation of its
 * 24 hourly user views with its wiki's is below {@link #MIN_CORRELATION}; a spike or wiki whose 24
 * hours are all equal has no defined correlation and counts as zero. It is flagged for its
 * <b>access</b> when at least {@link #ONE_ACCESS} of its user views came through one access method
 * that has under half of the wiki's user views.
 */
public final class LooksAutomated {

  public static final double MIN_CORRELATION = 0.3;
  public static final double ONE_ACCESS = 0.95;

  private LooksAutomated() {}

  public static double correlation(long[] a, long[] b) {
    int n = a.length;
    double ma = 0;
    double mb = 0;
    for (int i = 0; i < n; i++) {
      ma += a[i];
      mb += b[i];
    }
    ma /= n;
    mb /= n;
    double cov = 0;
    double va = 0;
    double vb = 0;
    for (int i = 0; i < n; i++) {
      cov += (a[i] - ma) * (b[i] - mb);
      va += (a[i] - ma) * (a[i] - ma);
      vb += (b[i] - mb) * (b[i] - mb);
    }
    return va == 0 || vb == 0 ? 0 : cov / Math.sqrt(va * vb);
  }

  /** The verdict for one spike: which tests flagged it, or {@code -} when none did. */
  public static String flags(double correlation, double dominantShare, double wikiShareOfIt) {
    boolean shape = correlation < MIN_CORRELATION;
    boolean access = dominantShare >= ONE_ACCESS && wikiShareOfIt < 0.5;
    return shape && access ? "shape+access" : shape ? "shape" : access ? "access" : "-";
  }
}
