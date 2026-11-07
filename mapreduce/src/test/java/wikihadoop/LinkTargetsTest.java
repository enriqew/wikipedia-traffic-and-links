package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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

class LinkTargetsTest {

  @TempDir Path dir;

  /** Every line of the files in {@code out} whose name starts with {@code prefix}, sorted. */
  static List<String> read(Path out, String prefix) throws IOException {
    List<String> lines = new ArrayList<>();
    try (Stream<Path> files = Files.list(out)) {
      for (Path p : (Iterable<Path>) files.sorted()::iterator) {
        if (p.getFileName().toString().startsWith(prefix)) {
          lines.addAll(Files.readAllLines(p));
        }
      }
    }
    lines.sort(null);
    return lines;
  }

  @Test
  void eachLinkTargetAndRedirectGetsThePageItsTitleNames() throws Exception {
    Configuration conf = Jobs.localConf();
    Path in = dir.resolve("in");
    GraphFixture.page(in);
    GraphFixture.linktarget(in);
    GraphFixture.redirect(in);
    Path out = dir.resolve("out");
    Job job = LinkTargets.configure(conf, out.toString(), in.toString());
    assertTrue(job.waitForCompletion(false));

    assertEquals(List.of(
        "100\tARTICLE\t2", "101\tREDIRECT\t4", "102\tARTICLE\t3", "103\tRED_LINK\t-",
        "104\tTARGET_OTHER_NAMESPACE\t-", "105\tREDIRECT\t5", "106\tREDIRECT\t7", "107\tREDIRECT\t8",
        "108\tREDIRECT\t9", "109\tARTICLE\t1", "110\tREDIRECT\t6"), read(out, LinkTargets.TARGETS));
    assertEquals(List.of(
        "4\tARTICLE\t2", "5\tREDIRECT\t6", "6\tARTICLE\t3", "7\tREDIRECT_OUT_OF_NS0\t-",
        "8\tREDIRECT_INTERWIKI\t-", "9\tREDIRECT_TO_MISSING\t-"), read(out, LinkTargets.HOPS));

    Counters c = job.getCounters();
    assertEquals("", LinkTargets.failures(c));
    assertEquals(11, c.findCounter(DumpLine.GROUP, "page ROW").getValue());
    assertEquals(GraphFixture.lines(11) - 11, c.findCounter(DumpLine.GROUP, "page NOT_DATA").getValue());
    assertEquals(11, c.findCounter(LinkTargets.Counts.TARGETS).getValue());
    assertEquals(6, c.findCounter(LinkTargets.Counts.HOPS).getValue());
  }
}
