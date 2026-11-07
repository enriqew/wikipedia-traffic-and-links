package wikihadoop;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Reads one line of a link-table dump in a mapper, and counts its fate.
 *
 * <p>The table comes from the file name: {@code simplewiki-20261101-page.sql.gz}, or a chunk cut at
 * ingest, {@code enwiki-20261101-pagelinks-00003.sql.gz}. A row must have its table's column count
 * and be valid UTF-8 (titles are {@code varbinary}); otherwise it is malformed.
 *
 * <p>Every line a link-graph mapper reads, dump or intermediate record, is counted once in the
 * {@link #GROUP} counter group, so {@link #unaccounted} can compare the group with the map input
 * records: equality, as for the pageview jobs.
 */
public final class DumpLine {

  public static final String GROUP = "lines";

  public enum Fate { ROW, NOT_DATA, MALFORMED, WRONG_WIDTH, BAD_UTF8 }

  static final Map<String, Integer> WIDTH = Map.of("page", 12, "pagelinks", 3, "linktarget", 3, "redirect", 5);

  private static final Pattern NAME = Pattern.compile("^[a-z_]+-\\d{8}-([a-z]+)(-\\d{5})?\\.sql\\.gz$");

  private final SqlRow row = new SqlRow();

  /** The table a dump file holds. */
  public static String table(String fileName) throws IOException {
    Matcher m = NAME.matcher(fileName);
    if (!m.matches() || !WIDTH.containsKey(m.group(1))) {
      throw new IOException("not a link-table dump file: " + fileName);
    }
    return m.group(1);
  }

  /** Counts one input line under {@code source}, for {@link #unaccounted}. */
  public static void count(TaskInputOutputContext<?, ?, ?, ?> context, String source, String fate) {
    context.getCounter(GROUP, source + " " + fate).increment(1);
  }

  /** Map input records with no count in {@link #GROUP}: zero when every line was accounted for. */
  public static long unaccounted(Counters counters) {
    long counted = 0;
    CounterGroup group = counters.getGroup(GROUP);
    for (org.apache.hadoop.mapreduce.Counter c : group) {
      counted += c.getValue();
    }
    return counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue() - counted;
  }

  /** Dump lines of {@code table} that did not parse, whatever the reason. */
  public static long malformed(Counters counters, String table) {
    long n = 0;
    for (Fate f : new Fate[] {Fate.MALFORMED, Fate.WRONG_WIDTH, Fate.BAD_UTF8}) {
      n += counters.findCounter(GROUP, table + " " + f).getValue();
    }
    return n;
  }

  /** The row's values when the line is a row of {@code table}, else null; the fate is counted. */
  public List<String> read(Text line, String table, TaskInputOutputContext<?, ?, ?, ?> context) {
    Fate fate;
    try {
      Text.validateUTF8(line.getBytes(), 0, line.getLength());
      SqlRow.Fate f = row.parse(line.toString());
      if (f == SqlRow.Fate.ROW) {
        fate = row.values.size() == WIDTH.get(table) ? Fate.ROW : Fate.WRONG_WIDTH;
      } else {
        fate = f == SqlRow.Fate.NOT_DATA ? Fate.NOT_DATA : Fate.MALFORMED;
      }
    } catch (CharacterCodingException e) {
      fate = Fate.BAD_UTF8;
    }
    count(context, table, fate.name());
    return fate == Fate.ROW ? row.values : null;
  }
}
