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

class RedirectsTest {

  @TempDir Path dir;

  @Test
  void aLinkTargetThatNamesARedirectTakesItsTargetOneHopOnly() throws Exception {
    Configuration conf = Jobs.localConf();
    Path in = GraphFixture.all(dir.resolve("in"));
    Path targets = dir.resolve("targets");
    assertTrue(LinkTargets.configure(conf, targets.toString(),
        in.resolve(GraphFixture.WIKI + "page.sql.gz").toString(),
        in.resolve(GraphFixture.WIKI + "linktarget.sql.gz").toString(),
        in.resolve(GraphFixture.WIKI + "redirect.sql.gz").toString()).waitForCompletion(false));

    Path out = dir.resolve("out");
    Job job = Redirects.configure(conf, targets.toString(), out.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of(
        "101\tVIA\t2",                    // B -> Beta
        "105\tDOUBLE_REDIRECT\t-",        // G2 -> G3, itself a redirect
        "106\tREDIRECT_OUT_OF_NS0\t-",    // Out -> Category:X
        "107\tREDIRECT_INTERWIKI\t-",     // Iw -> another wiki
        "108\tREDIRECT_TO_MISSING\t-",    // Lost -> Nowhere
        "110\tVIA\t3"),                   // G3 -> Gamma
        LinkTargetsTest.read(out, "part-r-"));

    Counters c = job.getCounters();
    assertEquals("", Redirects.failures(c));
    assertEquals(6, c.findCounter(DumpLine.GROUP, "hops read").getValue());
    assertEquals(6, c.findCounter(DumpLine.GROUP, "targets to a redirect").getValue());
    assertEquals(5, c.findCounter(DumpLine.GROUP, "targets left for Edges").getValue());
  }
}
