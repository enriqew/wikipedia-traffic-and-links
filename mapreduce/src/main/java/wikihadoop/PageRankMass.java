package wikihadoop;

/**
 * PageRank in integer units (plan section 14): every article starts with {@link #UNITS}, so the
 * total mass is the article count times that, and no step loses or makes a unit.
 *
 * <p>An article with rank r keeps {@link #damped} r for its out-links, each getting an equal floor
 * share; what is not passed on (the other 15%, the remainders, all of r when there are no
 * out-links) goes to the pool, which the next iteration shares among all articles.
 */
public final class PageRankMass {

  public static final long UNITS = 1_000_000_000L;

  private PageRankMass() {}

  /** floor(0.85 r) for r &gt;= 0, exact for any long: with r = 100 q + s it is 85 q + floor(85 s / 100). */
  public static long damped(long r) {
    return r / 100 * 85 + r % 100 * 85 / 100;
  }

  /** What one out-link of an article with rank r and out-degree k receives; zero when k is zero. */
  public static long perLink(long r, int k) {
    return k == 0 ? 0 : damped(r) / k;
  }

  /** What an article with rank r and out-degree k leaves to the pool. */
  public static long toPool(long r, int k) {
    return r - perLink(r, k) * k;
  }
}
