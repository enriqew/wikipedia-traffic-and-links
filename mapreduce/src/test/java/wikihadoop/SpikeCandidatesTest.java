package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpikeCandidatesTest {

  @TempDir Path dir;

  @Test
  void findsEveryArticleThatCouldSpikeAndTotalsEveryView() throws Exception {
    Configuration conf = Jobs.localConf();
    Window.set(conf, "2026-10-28", 2);

    // English Wikipedia: 10 million user views on the 28th (floor 1,000), 20 million on the 29th (2,000).
    Path domainHour = dir.resolve("domainhour");
    Files.createDirectories(domainHour);
    Files.write(domainHour.resolve("part-r-00000"),
        List.of("2026-10-28\ten\t0\t10000000", "2026-10-29\ten.m\t5\t20000000"), StandardCharsets.UTF_8);

    Path in = dir.resolve("in");
    Files.createDirectories(in);
    Jobs.bzip2(conf, in.resolve("pageviews-20261028-user.bz2"), String.join("\n",
        "en.wikipedia Big 1 desktop 600 A600",
        "en.wikipedia Big 1 mobile-web 500 A500",
        "en.wikipedia Split 2 desktop 400 A400",        // a third of the floor: the article is a candidate
        "en.wikipedia Split 2 mobile-app 300 A300",
        "en.wikipedia Small 3 desktop 300 A300",        // under a third of the floor
        "xx.wikipedia Other 4 desktop 9999 A9999",      // not a ranked wiki: totals only
        "broken",
        ""));
    Jobs.bzip2(conf, in.resolve("pageviews-20261029-automated.bz2"),
        "en.wikipedia Small 3 desktop 700 A700\n");     // over a third of the 29th's floor

    Path out = dir.resolve("out");
    Job job = SpikeCandidates.configure(conf, domainHour.toString(), out.toString(), in.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of(
        "en.wikipedia\tBig\t2",
        "en.wikipedia\tSmall\t1",
        "en.wikipedia\tSplit\t1"), Files.readAllLines(out.resolve("candidates-r-00000")));
    assertEquals(List.of(
        "2026-10-28\ten.wikipedia\tuser\tdesktop\t1300",
        "2026-10-28\ten.wikipedia\tuser\tmobile-app\t300",
        "2026-10-28\ten.wikipedia\tuser\tmobile-web\t500",
        "2026-10-28\txx.wikipedia\tuser\tdesktop\t9999",
        "2026-10-29\ten.wikipedia\tautomated\tdesktop\t700"), Files.readAllLines(out.resolve("totals-r-00000")));

    Counters c = job.getCounters();
    assertEquals(12099, c.findCounter(SpikeCandidates.Views.USER).getValue());
    assertEquals(700, c.findCounter(SpikeCandidates.Views.AUTOMATED).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.FIELDS).getValue());
    assertEquals(0, SpikeCandidates.unaccounted(c));
  }
}
