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

class TopSpikesTest {

  @TempDir Path dir;

  private static String spike(String wiki, String title, String date, double excess) {
    return wiki + "\t" + title + "\t" + date + "\t" + (long) (excess + 10) + "\t10.0\t" + excess
        + "\t0\t0\t0\t0\t0.9000\tdesktop\t0.6000\t0.3500\t-\t0";
  }

  @Test
  void keepsTheLargestArticlesPerWikiAndDay() throws Exception {
    Configuration conf = Jobs.localConf();
    conf.setInt(TopSpikes.TOP, 2);
    Path in = dir.resolve("in");
    Files.createDirectories(in);
    String a = spike("en.wikipedia", "Alpha", "2026-10-05", 50_000);
    String b = spike("en.wikipedia", "Beta", "2026-10-05", 90_000);
    String c = spike("en.wikipedia", "Gamma", "2026-10-05", 50_000);  // ties with Alpha, sorts after it
    String main = spike("en.wikipedia", "Main_Page", "2026-10-05", 900_000);
    String special = spike("en.wikipedia", "Special:Search", "2026-10-05", 800_000);
    String nextDay = spike("en.wikipedia", "Delta", "2026-10-06", 30_000);
    String es = spike("es.wikipedia", "Epsilon", "2026-10-05", 3_000);
    String other = spike("xx.wikipedia", "Zeta", "2026-10-05", 1_000_000);  // not a ranked wiki
    Files.write(in.resolve("part-r-00000"), List.of(a, b, c, main, special, nextDay, es, other),
        StandardCharsets.UTF_8);

    Path out = dir.resolve("out");
    Job job = TopSpikes.configure(conf, in.toString(), out.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of("1\t" + b, "2\t" + a, "1\t" + nextDay, "1\t" + es),
        Files.readAllLines(out.resolve("part-r-00000")));
    Counters k = job.getCounters();
    assertEquals(8, k.findCounter(TopSpikes.Counts.SPIKES_READ).getValue());
    assertEquals(3, k.findCounter(TopSpikes.Counts.NOT_RANKED).getValue());
    assertEquals(4, k.findCounter(TopSpikes.Counts.KEPT).getValue());
  }
}
