package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/** Runs the whole job in Hadoop's local job runner, on a bzip2 file like the real input. */
class DomainHourTotalsTest {

  @TempDir Path dir;

  private static final String FIXTURE = String.join("\n",
      "en.wikipedia Main_Page 15580374 desktop 5 A2C3",
      "en.wikipedia Main_Page 15580374 mobile-web 4 A4",
      "en.wikipedia Main_Page 15580374 mobile-app 1 A1",
      "en.wikipedia Other 12 desktop 7 C7",
      "commons.wikimedia File:X.jpg 9 desktop 2 X2",
      "xx.unknownproject Y 1 desktop 6 B6",
      "broken line",
      "en.wikipedia Bad 1 desktop 4 A2C3",
      "") ;

  @Test
  void sumsPerDomainAndHourAndAccountsForEveryLine() throws Exception {
    Path input = dir.resolve("pageviews-20261028-user.bz2");
    BZip2Codec codec = new BZip2Codec();
    Configuration conf = new Configuration();
    codec.setConf(conf);
    try (OutputStream out = codec.createOutputStream(Files.newOutputStream(input))) {
      out.write(FIXTURE.getBytes(StandardCharsets.UTF_8));
    }
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    Path output = dir.resolve("out");

    Job job = DomainHourTotals.configure(conf, new String[] {input.toString()}, output.toString());
    assertTrue(job.waitForCompletion(false));

    List<String> lines = Files.readAllLines(output.resolve("part-r-00000"));
    assertEquals(List.of(
        "commons.m\t23\t2",
        "en\t0\t2",
        "en\t2\t10",
        "en.m\t0\t5"), lines);

    Counters c = job.getCounters();
    assertEquals(6, c.findCounter(PageviewLine.Fate.PARSED).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.FIELDS).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.DAILY_NOT_SUM).getValue());
    assertEquals(1, c.findCounter(DomainHourTotals.Extra.UNMAPPED_ROWS).getValue());
    assertEquals(6, c.findCounter(DomainHourTotals.Extra.UNMAPPED_VIEWS).getValue());
    assertEquals(0, DomainHourTotals.unaccounted(c));
  }
}
