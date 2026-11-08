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
}
