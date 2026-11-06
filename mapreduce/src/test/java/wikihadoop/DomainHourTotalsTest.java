package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Runs the whole job in Hadoop's local job runner, on bzip2 files like the real input. */
class DomainHourTotalsTest {

  @TempDir Path dir;

  private static final String DAY_ONE = String.join("\n",
      "en.wikipedia Main_Page 15580374 desktop 5 A2C3",
      "en.wikipedia Main_Page 15580374 mobile-web 4 A4",
      "en.wikipedia Main_Page 15580374 mobile-app 1 A1",
      "en.wikipedia Other 12 desktop 7 C7",
      "commons.wikimedia File:X.jpg 9 desktop 2 X2",
      "xx.unknownproject Y 1 desktop 6 B6",
      "broken line",
      "en.wikipedia Bad 1 desktop 4 A2C3",
      "");

  private static final String DAY_TWO = "en.wikipedia Main_Page 15580374 desktop 3 A3\n";

  private static void bzip2(Configuration conf, Path file, String text) throws IOException {
    BZip2Codec codec = new BZip2Codec();
    codec.setConf(conf);
    try (OutputStream out = codec.createOutputStream(Files.newOutputStream(file))) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void sumsPerDayDomainAndHourAndAccountsForEveryLine() throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    Path input = dir.resolve("in");
    Files.createDirectories(input);
    bzip2(conf, input.resolve("pageviews-20261028-user.bz2"), DAY_ONE);
    bzip2(conf, input.resolve("pageviews-20261029-user.bz2"), DAY_TWO);
    Path output = dir.resolve("out");

    Job job = DomainHourTotals.configure(conf, new String[] {input.toString()}, output.toString());
    assertTrue(job.waitForCompletion(false));

    // The same domain and hour on two days stay two lines.
    assertEquals(List.of(
        "2026-10-28\tcommons.m\t23\t2",
        "2026-10-28\ten\t0\t2",
        "2026-10-28\ten\t2\t10",
        "2026-10-28\ten.m\t0\t5",
        "2026-10-29\ten\t0\t3"), Files.readAllLines(output.resolve("part-r-00000")));

    Counters c = job.getCounters();
    assertEquals(7, c.findCounter(PageviewLine.Fate.PARSED).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.FIELDS).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.DAILY_NOT_SUM).getValue());
    assertEquals(1, c.findCounter(DomainHourTotals.Extra.UNMAPPED_ROWS).getValue());
    assertEquals(6, c.findCounter(DomainHourTotals.Extra.UNMAPPED_VIEWS).getValue());
    assertEquals(0, DomainHourTotals.unaccounted(c));
  }

  @Test
  void theDateComesFromTheFileName() throws Exception {
    assertEquals("2026-10-28", DomainHourTotals.fileDate("pageviews-20261028-automated.bz2"));
    assertThrows(IOException.class, () -> DomainHourTotals.fileDate("part-00000.bz2"));
  }
}
