package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AdjacencyTest {

  @TempDir Path dir;

  @Test
  void theFourJobsGiveEveryArticleItsDistinctOutLinksAndEveryRowOneFate() throws Exception {
    Counters edges = EdgesTest.throughEdges(dir).getCounters();
    Path out = dir.resolve("graph");
    Job job = Adjacency.configure(Jobs.localConf(), dir.resolve("edges").toString(), out.toString(),
        dir.resolve("in").resolve(GraphFixture.WIKI + "page.sql.gz").toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of("1\t2\t2,3", "11\t0\t", "2\t1\t3", "3\t0\t"),
        LinkTargetsTest.read(out, Adjacency.ADJACENCY));
    assertEquals(List.of("1\tAlpha", "11\tLonely", "2\tBeta", "3\tGamma"),
        LinkTargetsTest.read(out, Adjacency.TITLES));
    assertEquals(List.of("4\tB", "5\tG2", "6\tG3", "7\tOut", "8\tIw", "9\tLost"),
        LinkTargetsTest.read(out, Adjacency.REDIRECT_TITLES));

    Counters c = job.getCounters();
    assertEquals("", Adjacency.failures(c));
    assertEquals(4, c.findCounter(Adjacency.Counts.ARTICLES).getValue());
    assertEquals(2, c.findCounter(Adjacency.Counts.ARTICLES_WITHOUT_LINKS).getValue());
    assertEquals(3, c.findCounter(Adjacency.Counts.EDGES).getValue());
    assertEquals(2, c.findCounter(LinkFate.EDGE_DIRECT).getValue());  // 1 -> 2 stays direct beside its via row
    assertEquals(1, c.findCounter(LinkFate.EDGE_VIA).getValue());
    for (LinkFate f : new LinkFate[] {LinkFate.DUPLICATE, LinkFate.SELF, LinkFate.FROM_REDIRECT,
        LinkFate.FROM_MISSING}) {
      assertEquals(1, c.findCounter(f).getValue(), f.name());
    }

    // Every pagelinks row, one fate across the two jobs; REDIRECT_WITHOUT_ROW is the one not met.
    assertEquals(15, Edges.fates(edges) + Edges.fates(c));
    assertEquals(0, edges.findCounter(LinkFate.REDIRECT_WITHOUT_ROW).getValue()
        + c.findCounter(LinkFate.REDIRECT_WITHOUT_ROW).getValue());
  }
}
