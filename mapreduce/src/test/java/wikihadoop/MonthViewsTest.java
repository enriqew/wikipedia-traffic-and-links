package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MonthViewsTest {

  @TempDir Path dir;

  /** Two days of English, Simple English and German views; the job keeps the first two. */
  static Path month(Configuration conf, Path in) throws Exception {
    Files.createDirectories(in);
    Jobs.bzip2(conf, in.resolve("pageviews-20261001-user.bz2"), String.join("\n",
        "en.wikipedia Alpha 1 desktop 5 A3B2",
        "en.wikipedia Alpha 1 mobile-web 2 C2",
        "en.wikipedia B 4 desktop 3 A3",
        "en.wikipedia Talk:Alpha 10 desktop 6 A6",
        "en.wikipedia Nowhere null desktop 1 A1",
        "en.wikipedia G2 5 desktop 2 B2",
        "de.wikipedia Alpha 9 desktop 4 A4",
        "too few fields") + "\n");
    Jobs.bzip2(conf, in.resolve("pageviews-20261001-automated.bz2"), String.join("\n",
        "en.wikipedia Alpha 1 desktop 7 A7",
        "en.wikipedia G3 6 desktop 4 D4") + "\n");
    Jobs.bzip2(conf, in.resolve("pageviews-20261002-user.bz2"), String.join("\n",
        "en.wikipedia Alpha 1 desktop 1 X1",
        "en.wikipedia G3 6 mobile-app 3 A1B2",
        "simple.wikipedia Alpha 3 desktop 2 A2") + "\n");
    return in;
  }

  static Configuration conf() {
    Configuration conf = Jobs.localConf();
    conf.set("wh.views.wikis", "en.wikipedia,simple.wikipedia");
    conf.setInt("wh.graph.reducers", 2);
    return conf;
  }

  @Test
  void eachTitleOfTheKeptWikisGetsItsViewsByAgentAndEveryLineAFate() throws Exception {
    Configuration conf = conf();
    Path in = month(conf, dir.resolve("in"));
    Path out = dir.resolve("out");
    Job job = MonthViews.configure(conf, out.toString(), in.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of(
        "en.wikipedia\tAlpha\t8\t7",
        "en.wikipedia\tB\t3\t0",
        "en.wikipedia\tG2\t2\t0",
        "en.wikipedia\tG3\t3\t4",
        "en.wikipedia\tNowhere\t1\t0",
        "en.wikipedia\tTalk:Alpha\t6\t0",
        "simple.wikipedia\tAlpha\t2\t0"), LinkTargetsTest.read(out, MonthViews.OUTPUT));

    Counters c = job.getCounters();
    assertEquals("", MonthViews.failures(c));
    assertEquals(12, c.findCounter(PageviewLine.Fate.PARSED).getValue());
    assertEquals(1, c.findCounter(PageviewLine.Fate.FIELDS).getValue());
    assertEquals(1, c.findCounter(MonthViews.Counts.OTHER_WIKI_ROWS).getValue());
    assertEquals(7, c.findCounter(MonthViews.Counts.TITLES).getValue());
    assertEquals(23, c.findCounter(MonthViews.VIEWS, "en.wikipedia user").getValue());
    assertEquals(11, c.findCounter(MonthViews.VIEWS, "en.wikipedia automated").getValue());
    assertEquals(2, c.findCounter(MonthViews.VIEWS, "simple.wikipedia user").getValue());
  }
}
