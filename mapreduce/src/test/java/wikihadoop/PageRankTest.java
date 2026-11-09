package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PageRankTest {

  @TempDir Path dir;

  /** Six articles; 5 links nowhere, so its mass goes to the pool. */
  private static final long[][] GRAPH = {
      {1, 2, 3}, {2, 3}, {3, 1}, {4, 3}, {5}, {6, 1, 2, 3, 4, 5}};

  /** The same integer model written plainly, to compare unit for unit. */
  private static Map<Long, Long> simulate(double tolerance, int max, int[] iterations) {
    int n = GRAPH.length;
    long mass = n * PageRankMass.UNITS;
    long[] link = new long[n];
    long[] prev = new long[n];
    java.util.Arrays.fill(link, PageRankMass.UNITS);
    java.util.Arrays.fill(prev, PageRankMass.UNITS);
    long share = 0;
    long carry = 0;
    for (int j = 1; j <= max; j++) {
      long[] next = new long[n];
      long pool = carry;
      long delta = 0;
      for (int v = 0; v < n; v++) {
        long r = link[v] + share;
        delta += Math.abs(r - prev[v]);
        int k = GRAPH[v].length - 1;
        long per = PageRankMass.perLink(r, k);
        for (int i = 1; i <= k; i++) {
          next[(int) GRAPH[v][i] - 1] += per;
        }
        pool += PageRankMass.toPool(r, k);
        prev[v] = r;
      }
      link = next;
      share = pool / n;
      carry = pool % n;
      iterations[0] = j;
      if (j >= 2 && delta < tolerance * mass) {
        break;
      }
    }
    Map<Long, Long> ranks = new TreeMap<>();
    for (int v = 0; v < n; v++) {
      ranks.put((long) v + 1, link[v] + share);
    }
    return ranks;
  }

  /** PageRank in floating point, iterated to convergence, scaled to the same total mass. */
  private static double[] floating() {
    int n = GRAPH.length;
    double[] x = new double[n];
    java.util.Arrays.fill(x, 1.0 / n);
    for (int it = 0; it < 2000; it++) {
      double[] next = new double[n];
      double dangling = 0;
      for (int v = 0; v < n; v++) {
        int k = GRAPH[v].length - 1;
        if (k == 0) {
          dangling += x[v];
        }
        for (int i = 1; i <= k; i++) {
          next[(int) GRAPH[v][i] - 1] += 0.85 * x[v] / k;
        }
      }
      for (int v = 0; v < n; v++) {
        next[v] += (0.15 + 0.85 * dangling) / n;
      }
      x = next;
    }
    return x;
  }

  @Test
  void theJobsGiveTheIntegerModelUnitForUnitAndPageRankToAMillionth() throws Exception {
    Path adjacency = dir.resolve("graph");
    Files.createDirectories(adjacency);
    StringBuilder text = new StringBuilder();
    for (long[] row : GRAPH) {
      StringBuilder csv = new StringBuilder();
      for (int i = 1; i < row.length; i++) {
        csv.append(i == 1 ? "" : ",").append(row[i]);
      }
      text.append(row[0]).append('\t').append(row.length - 1).append('\t').append(csv).append('\n');
    }
    Files.write(adjacency.resolve("adjacency-r-00000"), text.toString().getBytes(StandardCharsets.UTF_8));

    Configuration conf = Jobs.localConf();
    conf.setDouble("wh.pagerank.tolerance", 1e-9);
    conf.setInt("wh.pagerank.max", 200);
    conf.setInt("wh.graph.reducers", 2);
    Path work = dir.resolve("work");
    PageRank.Result result = PageRank.run(conf, adjacency.resolve("adjacency-r-*").toString(), work.toString());

    int[] iterations = new int[1];
    Map<Long, Long> expected = simulate(1e-9, 200, iterations);
    Map<Long, Long> ranks = new TreeMap<>();
    for (Path p : (Iterable<Path>) Files.list(work.resolve("ranks"))::iterator) {
      if (p.getFileName().toString().startsWith("part-")) {
        for (String line : Files.readAllLines(p)) {
          String[] f = line.split("\t");
          ranks.put(Long.parseLong(f[0]), Long.parseLong(f[1]));
        }
      }
    }
    assertEquals(expected, ranks);
    assertEquals(iterations[0], result.iterations.size());
    assertEquals(6, result.nodes);
    assertEquals(10, result.edges);

    long total = result.carry;
    for (long r : ranks.values()) {
      total += r;
    }
    assertEquals(result.mass, total);

    double[] x = floating();
    for (int v = 0; v < GRAPH.length; v++) {
      double r = (double) ranks.get((long) v + 1) / result.mass;
      assertTrue(Math.abs(r - x[v]) / x[v] < 1e-6, "article " + (v + 1) + ": " + r + " against " + x[v]);
    }

    // Only the last two iterations stay, with the log.
    List<String> left = new java.util.ArrayList<>();
    for (Path p : (Iterable<Path>) Files.list(work)::iterator) {
      left.add(p.getFileName().toString());
    }
    left.sort(null);
    int last = result.iterations.size();
    assertEquals(List.of(String.format("iter-%03d", last - 1), String.format("iter-%03d", last),
        "iterations.json", "ranks"), left.stream().filter(s -> !s.startsWith(".")).collect(java.util.stream.Collectors.toList()));
  }

  private String writeGraph() throws Exception {
    Path adjacency = dir.resolve("graph");
    Files.createDirectories(adjacency);
    StringBuilder text = new StringBuilder();
    for (long[] row : GRAPH) {
      StringBuilder csv = new StringBuilder();
      for (int i = 1; i < row.length; i++) {
        csv.append(i == 1 ? "" : ",").append(row[i]);
      }
      text.append(row[0]).append('\t').append(row.length - 1).append('\t').append(csv).append('\n');
    }
    Files.write(adjacency.resolve("adjacency-r-00000"), text.toString().getBytes(StandardCharsets.UTF_8));
    return adjacency.resolve("adjacency-r-*").toString();
  }

  private static Map<Long, Long> readRanks(Path work) throws Exception {
    Map<Long, Long> ranks = new TreeMap<>();
    for (Path p : (Iterable<Path>) Files.list(work.resolve("ranks"))::iterator) {
      if (p.getFileName().toString().startsWith("part-")) {
        for (String line : Files.readAllLines(p)) {
          String[] f = line.split("\t");
          ranks.put(Long.parseLong(f[0]), Long.parseLong(f[1]));
        }
      }
    }
    return ranks;
  }

  /** Each iteration's line in an iterations.json without the time and the byte counts, which differ between runs. */
  private static List<String> arithmetic(Path log) throws Exception {
    List<String> out = new java.util.ArrayList<>();
    for (String line : Files.readAllLines(log)) {
      if (line.trim().startsWith("{\"iteration\"")) {
        out.add(line.replaceAll("\"seconds\": [0-9.]+, ", "").replaceAll(", \"bytes_read\".*", ""));
      }
    }
    return out;
  }

  @Test
  void aRunResumedFromAnIterationThatSurvivedEndsWhereAnUnbrokenRunDoes() throws Exception {
    String adjacency = writeGraph();
    Configuration conf = Jobs.localConf();
    conf.setDouble("wh.pagerank.tolerance", 1e-9);
    conf.setInt("wh.pagerank.max", 200);
    conf.setInt("wh.graph.reducers", 2);
    Path unbroken = dir.resolve("unbroken");
    PageRank.Result whole = PageRank.run(conf, adjacency, unbroken.toString());

    // The broken run stops after iteration 12; iteration 12 is then lost, as when a disk fills while
    // it is written, and the log is cut back to the 11 iterations whose output survived.
    Path broken = dir.resolve("broken");
    Configuration first = new Configuration(conf);
    first.setInt("wh.pagerank.max", 12);
    PageRank.run(first, adjacency, broken.toString());
    Files.delete(broken.resolve("iter-012").resolve("part-r-00001"));
    List<String> log = Files.readAllLines(broken.resolve("iterations.json"));
    List<String> kept = new java.util.ArrayList<>(log.subList(0, 12));
    kept.set(11, kept.get(11).replaceAll(",$", ""));
    kept.add("]}");
    Path resume = dir.resolve("resume.json");
    Files.write(resume, kept);

    Configuration again = new Configuration(conf);
    again.set("wh.pagerank.resume", resume.toString());
    PageRank.Result resumed = PageRank.run(again, adjacency, broken.toString());

    assertEquals(11, resumed.resumedAfter);
    assertEquals(whole.count(), resumed.count());
    assertEquals(whole.carry, resumed.carry);
    assertEquals(readRanks(unbroken), readRanks(broken));
    assertEquals(arithmetic(unbroken.resolve("iterations.json")), arithmetic(broken.resolve("iterations.json")));
    assertTrue(Files.readAllLines(broken.resolve("iterations.json")).get(0).contains("\"resumed_after\": 11, "));
  }

  @Test
  void aResumeRefusesAnIterationThatDidNotFinish() throws Exception {
    String adjacency = writeGraph();
    Configuration conf = Jobs.localConf();
    conf.setInt("wh.pagerank.max", 3);
    conf.setInt("wh.graph.reducers", 2);
    Path work = dir.resolve("work");
    PageRank.run(conf, adjacency, work.toString());
    Files.delete(work.resolve("iter-003").resolve("_SUCCESS"));
    Path resume = dir.resolve("resume.json");
    Files.copy(work.resolve("iterations.json"), resume);

    Configuration again = new Configuration(conf);
    again.set("wh.pagerank.resume", resume.toString());
    java.io.IOException e = org.junit.jupiter.api.Assertions.assertThrows(java.io.IOException.class,
        () -> PageRank.run(again, adjacency, work.toString()));
    assertTrue(e.getMessage().contains("iter-003 is not a finished iteration"), e.getMessage());
  }

  @Test
  void aRunWithoutAResumeFileRefusesAWorkDirectoryThatHoldsARunUnlessToldToOverwrite() throws Exception {
    String adjacency = writeGraph();
    Configuration conf = Jobs.localConf();
    conf.setInt("wh.pagerank.max", 3);
    conf.setInt("wh.graph.reducers", 2);
    Path work = dir.resolve("work");
    PageRank.run(conf, adjacency, work.toString());
    Map<Long, Long> before = readRanks(work);

    java.io.IOException e = org.junit.jupiter.api.Assertions.assertThrows(java.io.IOException.class,
        () -> PageRank.run(conf, adjacency, work.toString()));
    assertTrue(e.getMessage().contains("already holds a run"), e.getMessage());
    assertTrue(Files.exists(work.resolve("iter-003").resolve("_SUCCESS")));
    assertEquals(before, readRanks(work));

    Configuration overwrite = new Configuration(conf);
    overwrite.setBoolean("wh.pagerank.overwrite", true);
    assertEquals(3, PageRank.run(overwrite, adjacency, work.toString()).count());
    assertEquals(before, readRanks(work));
  }
}
