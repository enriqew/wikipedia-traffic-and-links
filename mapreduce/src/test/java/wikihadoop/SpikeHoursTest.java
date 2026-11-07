package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpikeHoursTest {

  @TempDir Path dir;

  /** A pageview_complete hourly run with {@code views} in each of the given hours. */
  private static String hours(long views, int from, int to) {
    StringBuilder s = new StringBuilder();
    for (int h = from; h < to; h++) {
      s.append((char) ('A' + h)).append(views);
    }
    return s.toString();
  }

  private static String csv(long views, int from, int to) {
    StringBuilder s = new StringBuilder();
    for (int h = 0; h < 24; h++) {
      s.append(h == 0 ? "" : ",").append(h >= from && h < to ? views : 0);
    }
    return s.toString();
  }

  @Test
  void joinsEachSpikeWithItsHoursAndFlagsTheOneThatDoesNotLookRead() throws Exception {
    Configuration conf = Jobs.localConf();
    Window.set(conf, "2026-10-05", 1);

    // English Wikipedia is awake from 08:00 to 22:00 UTC, and 35% of its user views are desktop.
    Path domainHour = dir.resolve("domainhour");
    Files.createDirectories(domainHour);
    List<String> wiki = new ArrayList<>();
    for (int h = 0; h < 24; h++) {
      long v = h >= 8 && h < 22 ? 1_000_000 : 100_000;
      wiki.add("2026-10-05\ten\t" + h + "\t" + v);
      wiki.add("2026-10-05\ten.m\t" + h + "\t" + 2 * v);
    }
    Files.write(domainHour.resolve("part-r-00000"), wiki, StandardCharsets.UTF_8);

    Path candidates = dir.resolve("candidates");
    Files.createDirectories(candidates);
    Files.write(candidates.resolve("totals-r-00000"), List.of(
        "2026-10-05\ten.wikipedia\tuser\tdesktop\t35",
        "2026-10-05\ten.wikipedia\tuser\tmobile-app\t5",
        "2026-10-05\ten.wikipedia\tuser\tmobile-web\t60",
        "2026-10-05\ten.wikipedia\tautomated\tdesktop\t900"), StandardCharsets.UTF_8);

    Path articleDays = dir.resolve("articledays");
    Files.createDirectories(articleDays);
    String human = "en.wikipedia\tHuman\t2026-10-05\t1400\t10.0\t1390.0\t0\t700\t700\t0";
    String scraper = "en.wikipedia\tScraper\t2026-10-05\t2400\t5.0\t2395.0\t9\t2400\t0\t0";
    Files.write(articleDays.resolve("spikes-r-00000"), List.of(human, scraper), StandardCharsets.UTF_8);

    Path in = dir.resolve("in");
    Files.createDirectories(in);
    Jobs.bzip2(conf, in.resolve("pageviews-20261005-user.bz2"), String.join("\n",
        "en.wikipedia Human 1 desktop 700 " + hours(50, 8, 22),
        "en.wikipedia Human 1 mobile-web 700 " + hours(50, 8, 22),
        "en.wikipedia Scraper 2 desktop 2400 " + hours(100, 0, 24),  // flat, and all desktop
        "en.wikipedia Other 3 desktop 5 A5",
        ""));

    Path out = dir.resolve("out");
    Job job = SpikeHours.configure(conf, domainHour.toString(), candidates.toString(), articleDays.toString(),
        out.toString(), in.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of(
        human + "\t1.0000\tdesktop\t0.5000\t0.3500\t-\t" + csv(100, 8, 22),
        scraper + "\t0.0000\tdesktop\t1.0000\t0.3500\tshape+access\t" + csv(100, 0, 24)),
        Files.readAllLines(out.resolve("part-r-00000")));

    Counters c = job.getCounters();
    assertEquals(3, c.findCounter(SpikeHours.Counts.SPIKE_ROWS).getValue());
    assertEquals(2, c.findCounter(SpikeHours.Counts.SPIKES).getValue());
    assertEquals(1, c.findCounter(SpikeHours.Counts.FLAGGED).getValue());
    assertEquals(0, c.findCounter(SpikeHours.Counts.HOURS_MISMATCH).getValue());
    assertEquals(0, SpikeCandidates.unaccounted(c));
  }
}
