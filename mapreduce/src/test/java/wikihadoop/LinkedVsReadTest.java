package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The three joins over GraphFixture's wiki, read as English Wikipedia so its namespaces are known,
 * with MonthViewsTest's views and PageRank's ranks.
 */
class LinkedVsReadTest {

  @TempDir Path dir;

  @Test
  void everyTitleNamesItsArticleThroughAtMostOneRedirect() throws Exception {
    Job job = titleTargets();

    assertEquals(List.of(
        "Alpha\t1\tarticle", "B\t2\tredirect", "Beta\t2\tarticle", "G2\t-\tDOUBLE_REDIRECT",
        "G3\t3\tredirect", "Gamma\t3\tarticle", "Iw\t-\tREDIRECT_INTERWIKI", "Lonely\t11\tarticle",
        "Lost\t-\tREDIRECT_TO_MISSING", "Out\t-\tREDIRECT_OUT_OF_NS0"),
        LinkTargetsTest.read(dir.resolve("title-targets"), TitleTargets.OUTPUT));
    Counters c = job.getCounters();
    assertEquals("", TitleTargets.failures(c));
    assertEquals(4, c.findCounter(TitleTargets.Counts.ARTICLES).getValue());
    assertEquals(2, c.findCounter(TitleTargets.Counts.REDIRECTS_TO_ARTICLES).getValue());
    assertEquals(4, c.findCounter(TitleTargets.Counts.REDIRECTS_ELSEWHERE).getValue());
    assertEquals(0, c.findCounter(TitleTargets.Counts.HOPS_OUTSIDE_NS0).getValue());
  }

  @Test
  void everyViewEndsInOneFateAndTheArticlesGetTheirsWithTheirRedirects() throws Exception {
    titleTargets();
    Configuration conf = MonthViewsTest.conf();
    Path views = dir.resolve("month-views");
    assertTrue(MonthViews.configure(conf, views.toString(),
        MonthViewsTest.month(conf, dir.resolve("pageviews")).toString()).waitForCompletion(false));

    conf.set("wh.views.wiki", "en.wikipedia");
    Path articleViews = dir.resolve("view-targets");
    Job targets = ViewTargets.configure(conf, views.toString(), dir.resolve("title-targets").toString(),
        articleViews.toString());
    assertTrue(targets.waitForCompletion(false));
    assertEquals(List.of("1\t8\t7\t0", "2\t3\t0\t1", "3\t3\t4\t1"),
        LinkTargetsTest.read(articleViews, ViewTargets.OUTPUT));
    Counters c = targets.getCounters();
    assertEquals("", ViewTargets.failures(c));
    long[] user = {8, 6, 2, 6, 1, 0};  // ARTICLE, REDIRECT, REDIRECT_ELSEWHERE, OTHER_NAMESPACE, NO_ARTICLE, UNKNOWN_TITLE
    for (ViewTargets.Fate f : ViewTargets.Fate.values()) {
      assertEquals(user[f.ordinal()], c.findCounter(ViewTargets.USER, f.name()).getValue(), f.name());
    }
    assertEquals(23, ViewTargets.sum(c, ViewTargets.USER));  // all of MonthViews' English user views
    assertEquals(11, ViewTargets.sum(c, ViewTargets.AUTOMATED));
    assertEquals(1, c.findCounter(ViewTargets.Counts.OTHER_WIKI_TITLES).getValue());

    Configuration pr = Jobs.localConf();
    pr.setInt("wh.pagerank.max", 3);
    pr.setInt("wh.graph.reducers", 2);
    Path work = dir.resolve("pagerank");
    PageRank.run(pr, dir.resolve("graph").resolve("adjacency-r-*").toString(), work.toString());
    Map<String, String> ranks = new TreeMap<>();
    long rankSum = 0;
    for (String line : LinkTargetsTest.read(work.resolve("ranks"), "part-m-")) {
      String[] f = line.split("\t");
      ranks.put(f[0], f[1]);
      rankSum += Long.parseLong(f[1]);
    }

    Path linked = dir.resolve("linked");
    Job join = LinkedVsRead.configure(conf, work.resolve("ranks").toString(), dir.resolve("graph").toString(),
        articleViews.toString(), linked.toString());
    assertTrue(join.waitForCompletion(false));
    List<String> expected = new ArrayList<>(List.of(
        "1\tAlpha\t" + ranks.get("1") + "\t8\t7\t0",
        "11\tLonely\t" + ranks.get("11") + "\t0\t0\t0",
        "2\tBeta\t" + ranks.get("2") + "\t3\t0\t3",
        "3\tGamma\t" + ranks.get("3") + "\t3\t4\t3"));
    assertEquals(expected, LinkTargetsTest.read(linked, LinkedVsRead.OUTPUT));
    Counters j = join.getCounters();
    assertEquals("", LinkedVsRead.failures(j));
    assertEquals(4, j.findCounter(LinkedVsRead.Counts.ARTICLES).getValue());
    assertEquals(3, j.findCounter(LinkedVsRead.Counts.ARTICLES_VIEWED).getValue());
    assertEquals(rankSum, j.findCounter(LinkedVsRead.Counts.RANK_SUM).getValue());
    assertEquals(14, j.findCounter(LinkedVsRead.Counts.USER_VIEWS).getValue());  // ARTICLE + REDIRECT
    assertEquals(6, j.findCounter(LinkedVsRead.Counts.USER_THROUGH_REDIRECTS).getValue());
    assertEquals(11, j.findCounter(LinkedVsRead.Counts.AUTOMATED_VIEWS).getValue());
  }

  @Test
  void theUnknownTitleKeepsItsViewsEvenWhenARedirectHasThatTitle() throws Exception {
    Path views = dir.resolve("views");
    java.nio.file.Files.createDirectories(views);
    java.nio.file.Files.write(views.resolve("views-r-00000"), List.of(
        "en.wikipedia	-	40	3", "en.wikipedia	Hyphen-minus	2	0"));
    Path targets = dir.resolve("targets");
    java.nio.file.Files.createDirectories(targets);
    java.nio.file.Files.write(targets.resolve("targets-r-00000"), List.of(
        "-	7	redirect", "Hyphen-minus	7	article"));
    Configuration conf = Jobs.localConf();
    conf.set("wh.views.wiki", "en.wikipedia");
    Path out = dir.resolve("out");
    Job job = ViewTargets.configure(conf, views.toString(), targets.toString(), out.toString());
    assertTrue(job.waitForCompletion(false));
    assertEquals(List.of("7	2	0	0"), LinkTargetsTest.read(out, ViewTargets.OUTPUT));
    Counters c = job.getCounters();
    assertEquals(40, c.findCounter(ViewTargets.USER, ViewTargets.Fate.UNKNOWN_TITLE.name()).getValue());
    assertEquals(3, c.findCounter(ViewTargets.AUTOMATED, ViewTargets.Fate.UNKNOWN_TITLE.name()).getValue());
    assertEquals(42, ViewTargets.sum(c, ViewTargets.USER));
  }

  @Test
  void viewsOfAnIdThatIsNoArticleFailTheJoin() throws Exception {
    Path ranks = dir.resolve("ranks");
    java.nio.file.Files.createDirectories(ranks);
    java.nio.file.Files.write(ranks.resolve("part-m-00000"), List.of("1\t5"));
    Path graph = dir.resolve("g");
    java.nio.file.Files.createDirectories(graph);
    java.nio.file.Files.write(graph.resolve("titles-r-00000"), List.of("1\tAlpha", "2\tBeta"));
    Path views = dir.resolve("v");
    java.nio.file.Files.createDirectories(views);
    java.nio.file.Files.write(views.resolve("articleviews-r-00000"), List.of("1\t4\t0\t0", "9\t2\t0\t1"));
    Job join = LinkedVsRead.configure(Jobs.localConf(), ranks.toString(), graph.toString(), views.toString(),
        dir.resolve("out").toString());
    assertTrue(join.waitForCompletion(false));
    Counters c = join.getCounters();
    assertEquals(1, c.findCounter(LinkedVsRead.Counts.NO_RANK).getValue());  // 2 has a title, no rank
    assertEquals(1, c.findCounter(LinkedVsRead.Counts.VIEWS_WITHOUT_ARTICLE).getValue());  // 9
    assertEquals("1 NO_RANK; 1 VIEWS_WITHOUT_ARTICLE; ", LinkedVsRead.failures(c));
  }

  /** The graph from GraphFixture's dumps, then TitleTargets over it into title-targets/. */
  private Job titleTargets() throws Exception {
    EdgesTest.throughEdges(dir);
    assertTrue(Adjacency.configure(Jobs.localConf(), dir.resolve("edges").toString(),
        dir.resolve("graph").toString(),
        dir.resolve("in").resolve(GraphFixture.WIKI + "page.sql.gz").toString()).waitForCompletion(false));
    Configuration conf = Jobs.localConf();
    conf.setInt("wh.graph.reducers", 2);
    Job job = TitleTargets.configure(conf, dir.resolve("graph").toString(), dir.resolve("targets").toString(),
        dir.resolve("title-targets").toString());
    assertTrue(job.waitForCompletion(false));
    return job;
  }
}
