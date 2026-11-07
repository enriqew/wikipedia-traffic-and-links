package wikihadoop;

import java.time.LocalDate;
import org.apache.hadoop.conf.Configuration;

/** The days a run covers, from the job configuration: {@code wh.window.start} and {@code wh.window.days}. */
public final class Window {

  public static final String START = "wh.window.start";
  public static final String DAYS = "wh.window.days";

  public final LocalDate start;
  public final int days;

  public Window(LocalDate start, int days) {
    this.start = start;
    this.days = days;
  }

  public static Window of(Configuration conf) {
    String start = conf.get(START);
    int days = conf.getInt(DAYS, 0);
    if (start == null || days <= 0) {
      throw new IllegalArgumentException("set " + START + " (YYYY-MM-DD) and " + DAYS);
    }
    return new Window(LocalDate.parse(start), days);
  }

  public static void set(Configuration conf, String start, int days) {
    conf.set(START, start);
    conf.setInt(DAYS, days);
  }

  /** The day's position in the window, 0 for the first day, or -1 outside it. */
  public int index(String date) {
    long i = LocalDate.parse(date).toEpochDay() - start.toEpochDay();
    return i >= 0 && i < days ? (int) i : -1;
  }

  public String date(int index) {
    return start.plusDays(index).toString();
  }
}
