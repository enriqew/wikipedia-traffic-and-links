package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EdgesTest {

  @TempDir Path dir;

  /** LinkTargets, Redirects and Edges over the fixture; returns the Edges job. */
  static Job throughEdges(Path dir) throws Exception {
    Configuration conf = Jobs.localConf();
    Path in = GraphFixture.all(dir.resolve("in"));
    String page = in.resolve(GraphFixture.WIKI + "page.sql.gz").toString();
    Path targets = dir.resolve("targets");
    assertTrue(LinkTargets.configure(conf, targets.toString(), page,
        in.resolve(GraphFixture.WIKI + "linktarget.sql.gz").toString(),
        in.resolve(GraphFixture.WIKI + "redirect.sql.gz").toString()).waitForCompletion(false));
    Path redirects = dir.resolve("redirects");
    assertTrue(Redirects.configure(conf, targets.toString(), redirects.toString()).waitForCompletion(false));
    Job job = Edges.configure(conf, targets.toString(), redirects.toString(), dir.resolve("edges").toString(),
        in.resolve(GraphFixture.WIKI + "pagelinks.sql.gz").toString());
    assertTrue(job.waitForCompletion(false));
    return job;
  }

  @Test
  void eachRowReachesAnArticleOrEndsInItsTargetsFate() throws Exception {
    Job job = throughEdges(dir);

    assertEquals(List.of(
        "1\t1\tdirect",   // self-link: Adjacency's to judge
        "1\t2\tdirect",
        "1\t2\tvia",      // through the redirect B
        "1\t3\tdirect",
        "12\t2\tdirect",  // from a page that does not exist, likewise
        "2\t3\tvia",      // through G3
        "4\t2\tdirect"),  // from the redirect B
        LinkTargetsTest.read(dir.resolve("edges"), "part-r-"));

    Counters c = job.getCounters();
    assertEquals("", Edges.failures(c));
    assertEquals(15, c.findCounter(DumpLine.GROUP, "pagelinks ROW").getValue());
    assertEquals(7, c.findCounter(Edges.Counts.EDGE_ROWS).getValue());
    for (LinkFate f : new LinkFate[] {LinkFate.FROM_OTHER_NAMESPACE, LinkFate.RED_LINK,
        LinkFate.TARGET_OTHER_NAMESPACE, LinkFate.TARGET_ID_MISSING, LinkFate.DOUBLE_REDIRECT,
        LinkFate.REDIRECT_OUT_OF_NS0, LinkFate.REDIRECT_INTERWIKI, LinkFate.REDIRECT_TO_MISSING}) {
      assertEquals(1, c.findCounter(f).getValue(), f.name());
    }
    assertEquals(8, Edges.fates(c));
  }
}
