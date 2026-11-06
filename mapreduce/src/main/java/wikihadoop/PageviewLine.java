package wikihadoop;

/**
 * One row of a pageview_complete file, and the fate of the line it came from.
 *
 * <p>A row is six space-separated fields: {@code wiki_code title page_id access_method daily_total
 * hourly}, where {@code hourly} is a run of {@code <hour letter><count>} pairs, A = 00:00 to 01:00
 * UTC through X = 23:00 to 24:00. A line is parsed only if it has six fields, its hourly run is
 * well formed with hours in ascending order, and the hourly counts sum to the daily total; any
 * other line is rejected with the first reason that applies.
 */
public final class PageviewLine {

  public enum Fate { PARSED, FIELDS, BAD_COUNTS, HOURS_OUT_OF_ORDER, DAILY_NOT_SUM }

  public Fate fate;
  public String wiki;
  public String title;
  public String pageId;
  public String access;
  public long daily;
  /** Views per hour of the day; zero where the row has no count for that hour. */
  public final long[] hours = new long[24];

  /** Parses {@code line} into this object, reusing it, and returns its fate. */
  public Fate parse(String line) {
    java.util.Arrays.fill(hours, 0L);
    String[] f = line.split(" ", -1);
    if (f.length != 6) {
      return fate = Fate.FIELDS;
    }
    wiki = f[0];
    title = f[1];
    pageId = f[2];
    access = f[3];
    String hourly = f[5];
    if (!digits(f[4]) || hourly.isEmpty()) {
      return fate = Fate.BAD_COUNTS;
    }
    daily = Long.parseLong(f[4]);
    long sum = 0;
    int last = -1;
    int i = 0;
    while (i < hourly.length()) {
      char letter = hourly.charAt(i);
      if (letter < 'A' || letter > 'X') {
        return fate = Fate.BAD_COUNTS;
      }
      int j = i + 1;
      while (j < hourly.length() && Character.isDigit(hourly.charAt(j))) {
        j++;
      }
      if (j == i + 1) {
        return fate = Fate.BAD_COUNTS;
      }
      int hour = letter - 'A';
      if (hour <= last) {
        return fate = Fate.HOURS_OUT_OF_ORDER;
      }
      long count = Long.parseLong(hourly.substring(i + 1, j));
      hours[hour] = count;
      sum += count;
      last = hour;
      i = j;
    }
    if (sum != daily) {
      return fate = Fate.DAILY_NOT_SUM;
    }
    return fate = Fate.PARSED;
  }

  private static boolean digits(String s) {
    if (s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }
}
