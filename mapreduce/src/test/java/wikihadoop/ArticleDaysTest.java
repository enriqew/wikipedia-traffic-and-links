package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArticleDaysTest {

  @TempDir Path dir;

  private List<String> lines(Path out, String prefix) throws Exception {
    List<String> all = new ArrayList<>();
    try (Stream<Path> files = Files.list(out)) {
      for (Path p : (Iterable<Path>) files.filter(f -> f.getFileName().toString().startsWith(prefix + "-r-"))::iterator) {
        all.addAll(Files.readAllLines(p));
      }
    }
    all.sort(null);
    return all;
  }

  @Test
  void findsTheDayThatRoseTenTimesOverItsOwnBaseline() throws Exception {
    Configuration conf = Jobs.localConf();
    Window.set(conf, "2026-10-01", 7);

    // English Wikipedia: 10 million user views every day, so the floor is 1,000.
    Path domainHour = dir.resolve("domainhour");
    Files.createDirectories(domainHour);
    List<String> totals = new ArrayList<>();
    for (int d = 1; d <= 7; d++) {
      totals.add("2026-10-0" + d + "\ten\t12\t10000000");
    }
    Files.write(domainHour.resolve("part-r-00000"), totals, StandardCharsets.UTF_8);

    Path candidates = dir.resolve("candidates");
    Files.createDirectories(candidates);
    Files.write(candidates.resolve("candidates-r-00000"),
        List.of("en.wikipedia\tSteady\t7", "en.wikipedia\tStory\t1"), StandardCharsets.UTF_8);
    // Pass one writes its totals beside the candidates; they must not be read as candidates.
    Files.write(candidates.resolve("totals-r-00000"),
        List.of("2026-10-05\ten.wikipedia\tuser\tdesktop\t5000"), StandardCharsets.UTF_8);

    Path in = dir.resolve("in");
    Files.createDirectories(in);
    for (int d = 1; d <= 7; d++) {
      StringBuilder rows = new StringBuilder("en.wikipedia Steady 5 desktop 1500 M1500\n");
      if (d == 5) {
        rows.append("en.wikipedia Story 9 desktop 1200 M1200\n").append("en.wikipedia Story 9 mobile-web 800 N800\n");
        rows.append("en.wikipedia Ignored 7 desktop 5000 M5000\n");  // not a candidate
      } else {
        rows.append("en.wikipedia Story 9 desktop 10 M10\n");
      }
      Jobs.bzip2(conf, in.resolve("pageviews-2026100" + d + "-user.bz2"), rows.toString());
    }
    Jobs.bzip2(conf, in.resolve("pageviews-20261005-automated.bz2"), "en.wikipedia Story 9 desktop 300 M300\n");

    Path out = dir.resolve("out");
    Job job = ArticleDays.configure(conf, domainHour.toString(), candidates.toString(), out.toString(), in.toString());
    assertTrue(job.waitForCompletion(false));

    // Baseline for the 5th: the median of days 1, 2, 3, 7 (the 4th, 5th and 6th left out) is 10.
    assertEquals(List.of("en.wikipedia\tStory\t2026-10-05\t2000\t10.0\t1990.0\t300\t1200\t800\t0"),
        lines(out, "spikes"));
    assertEquals(List.of("en.wikipedia\tSteady\t10500\t0", "en.wikipedia\tStory\t2060\t300"),
        lines(out, "articles"));

    Counters c = job.getCounters();
    assertEquals(2, c.findCounter(ArticleDays.Counts.ARTICLES).getValue());
    assertEquals(1, c.findCounter(ArticleDays.Counts.SPIKES).getValue());
    assertEquals(16, c.findCounter(ArticleDays.Counts.CANDIDATE_ROWS).getValue());
    assertEquals(0, SpikeCandidates.unaccounted(c));
  }
}
