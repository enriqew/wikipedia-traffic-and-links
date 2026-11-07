package wikihadoop;

import java.util.HashMap;
import java.util.Map;

/**
 * A wiki's user views per day and hour, read from the DomainHourTotals job's output
 * ({@code date<TAB>domain<TAB>hour<TAB>views}).
 *
 * <p>A wiki's views are those of its desktop domain code plus its mobile one, which also holds its
 * mobile-app views (docs/PHASE0.md).
 */
public final class WikiTotals {

  private final Map<String, long[]> hours = new HashMap<>();

  public void add(String line) {
    String[] f = line.split("\t", -1);
    if (f.length != 4) {
      throw new IllegalArgumentException("not a DomainHourTotals line: " + line);
    }
    hours.computeIfAbsent(f[0] + "\t" + f[1], k -> new long[24])[Integer.parseInt(f[2])] += Long.parseLong(f[3]);
  }

  /** The wiki's user views per hour that day; zeros if it had none or has no domain code. */
  public long[] hourly(String wiki, String date) {
    long[] out = new long[24];
    for (String access : new String[] {"desktop", "mobile-web"}) {
      String domain = DomainCode.of(wiki, access);
      long[] h = domain == null ? null : hours.get(date + "\t" + domain);
      if (h != null) {
        for (int i = 0; i < 24; i++) {
          out[i] += h[i];
        }
      }
    }
    return out;
  }

  public long userViews(String wiki, String date) {
    long sum = 0;
    for (long v : hourly(wiki, date)) {
      sum += v;
    }
    return sum;
  }
}
