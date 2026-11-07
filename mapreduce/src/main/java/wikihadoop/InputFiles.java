package wikihadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** What a pageview_complete file's name says: {@code pageviews-YYYYMMDD-<user|automated>.bz2}. */
public final class InputFiles {

  private static final Pattern NAME = Pattern.compile("pageviews-(\\d{4})(\\d{2})(\\d{2})-(user|automated)\\b");
  private static final Pattern DATE = Pattern.compile("pageviews-(\\d{4})(\\d{2})(\\d{2})-");

  private InputFiles() {}

  /** The day the file covers, as YYYY-MM-DD. */
  public static String date(String name) throws IOException {
    Matcher m = DATE.matcher(name);
    if (!m.find()) {
      throw new IOException("no date in input file name " + name);
    }
    return m.group(1) + "-" + m.group(2) + "-" + m.group(3);
  }

  /** {@code user} or {@code automated}. */
  public static String agent(String name) throws IOException {
    Matcher m = NAME.matcher(name);
    if (!m.find()) {
      throw new IOException("no agent in input file name " + name);
    }
    return m.group(4);
  }
}
