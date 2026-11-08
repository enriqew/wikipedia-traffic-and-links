package wikihadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.FileSystemCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PageRank over Adjacency's output, one MapReduce job per iteration (plan section 14).
 *
 * <p>The pool an iteration fills is known only from its counters, once every map has run, so the
 * driver shares it out in the next iteration: floor(pool / N) to every article, the remainder
 * carried. After every iteration it requires two equalities, in units: the ranks the mappers read
 * plus the carry equal the total mass M = N x 10^9, and the mass the reducers wrote plus N shares
 * plus the new carry equal M. It stops when the L1 change between two iterations is under
 * {@code wh.pagerank.tolerance} of M (10^-7) or after {@code wh.pagerank.max} iterations (100). The
 * change of an iteration is counted by the next one's mappers, which hold both ranks, so the run
 * ends one iteration past the first change under the tolerance.
 *
 * <p>The work directory keeps the last two iterations, {@code ranks/} ({@code page_id rank}) and
 * {@code iterations.json}: per iteration the time, the change, the pool, the share, the carry and
 * the bytes read, written and shuffled.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.PageRank &lt;Adjacency output glob&gt; &lt;work dir&gt;</pre>
 */
public final class PageRank extends Configured implements Tool {

  /** One iteration's figures, as written to iterations.json. */
  static final class Iteration {
    int number;
    double seconds;
    long rankIn;
    long pool;
    long share;
    long carry;
    long linkMass;
    /** L1 change of the previous iteration's ranks against the ones before; -1 for the first. */
    long change;
    long bytesRead;
    long bytesWritten;
    long shuffleBytes;
    long mapOutputRecords;
    long combineOutputRecords;

    String json(long mass) {
      return String.format(Locale.ROOT, "{\"iteration\": %d, \"seconds\": %.1f, \"change_of_previous\": %s, "
          + "\"pool\": %d, \"share\": %d, \"carry\": %d, \"link_mass\": %d, \"bytes_read\": %d, "
          + "\"bytes_written\": %d, \"shuffle_bytes\": %d, \"map_output_records\": %d, "
          + "\"combine_output_records\": %d}",
          number, seconds, change < 0 ? "null" : String.format(Locale.ROOT, "%.3e", (double) change / mass),
          pool, share, carry, linkMass, bytesRead, bytesWritten, shuffleBytes, mapOutputRecords,
          combineOutputRecords);
    }
  }

  /** What a run ends with. */
  static final class Result {
    long nodes;
    long edges;
    long mass;
    long carry;
    final List<Iteration> iterations = new ArrayList<>();
  }

  private static long bytes(Counters c, String suffix) {
    long n = 0;
    CounterGroup g = c.getGroup(FileSystemCounter.class.getName());
    for (Counter counter : g) {
      if (counter.getName().endsWith(suffix)) {
        n += counter.getValue();
      }
    }
    return n;
  }

  private static long get(Counters c, PageRankJobs.Counts name) {
    return c.findCounter(name).getValue();
  }

  private static Counters complete(Job job) throws Exception {
    if (!job.waitForCompletion(true)) {
      throw new IOException(job.getJobName() + " failed");
    }
    return job.getCounters();
  }

  private static void require(boolean ok, String what) throws IOException {
    if (!ok) {
      throw new IOException("check failed: " + what);
    }
  }

  static Result run(Configuration conf, String adjacency, String work) throws Exception {
    double tolerance = conf.getDouble("wh.pagerank.tolerance", 1e-7);
    int max = conf.getInt("wh.pagerank.max", 100);
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(work);
    fs.delete(dir, true);
    Result result = new Result();

    Counters init = complete(PageRankJobs.init(conf, adjacency, work + "/iter-000"));
    require(get(init, PageRankJobs.Counts.BAD_LINES) == 0, "adjacency lines whose out-degree is not their link count");
    result.nodes = get(init, PageRankJobs.Counts.NODES);
    result.edges = get(init, PageRankJobs.Counts.EDGES);
    long n = result.nodes;
    long mass = Math.multiplyExact(n, PageRankMass.UNITS);
    result.mass = mass;

    long share = 0;
    long carry = 0;
    int last = 0;
    for (int j = 1; j <= max; j++) {
      long started = System.nanoTime();
      Counters c = complete(PageRankJobs.iteration(conf, iter(work, j - 1), iter(work, j), share, j));
      Iteration it = new Iteration();
      it.number = j;
      it.seconds = (System.nanoTime() - started) / 1e9;
      it.rankIn = get(c, PageRankJobs.Counts.RANK_IN);
      require(it.rankIn + carry == mass, "iteration " + j + ": ranks read " + it.rankIn + " + carry " + carry + " != " + mass);
      require(get(c, PageRankJobs.Counts.MASS_WITHOUT_RECORD) == 0, "iteration " + j + ": mass sent to a page with no record");
      require(get(c, PageRankJobs.Counts.RECORDS_TWICE) == 0, "iteration " + j + ": a page with two records");
      require(get(c, PageRankJobs.Counts.NODES) == n, "iteration " + j + ": " + get(c, PageRankJobs.Counts.NODES) + " records, not " + n);
      long pool = get(c, PageRankJobs.Counts.POOL) + carry;
      it.pool = pool;
      it.share = pool / n;
      it.carry = pool % n;
      it.linkMass = get(c, PageRankJobs.Counts.LINK_MASS);
      require(it.linkMass + n * it.share + it.carry == mass, "iteration " + j + ": mass after it is not " + mass);
      it.change = j == 1 ? -1 : get(c, PageRankJobs.Counts.DELTA);
      it.bytesRead = bytes(c, "BYTES_READ");
      it.bytesWritten = bytes(c, "BYTES_WRITTEN");
      it.shuffleBytes = c.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue();
      it.mapOutputRecords = c.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
      it.combineOutputRecords = c.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue();
      result.iterations.add(it);
      share = it.share;
      carry = it.carry;
      last = j;
      if (j >= 2) {
        fs.delete(new Path(iter(work, j - 2)), true);
      }
      writeLog(fs, work, result);
      System.err.println(it.json(mass));
      if (it.change >= 0 && it.change < tolerance * mass) {
        break;
      }
    }

    Counters r = complete(PageRankJobs.ranks(conf, iter(work, last), work + "/ranks", share));
    require(get(r, PageRankJobs.Counts.NODES) == n, "ranks: not every article written");
    require(get(r, PageRankJobs.Counts.MASS_OUT) + carry == mass, "ranks: the ranks plus the carry are not " + mass);
    result.carry = carry;
    writeLog(fs, work, result);
    return result;
  }

  static String iter(String work, int j) {
    return String.format(Locale.ROOT, "%s/iter-%03d", work, j);
  }

  private static void writeLog(FileSystem fs, String work, Result result) throws IOException {
    StringBuilder s = new StringBuilder();
    s.append(String.format(Locale.ROOT, "{\"articles\": %d, \"edges\": %d, \"mass\": %d, \"units_per_article\": %d, "
        + "\"damping\": 0.85, \"iterations\": [\n", result.nodes, result.edges, result.mass, PageRankMass.UNITS));
    for (int i = 0; i < result.iterations.size(); i++) {
      s.append("  ").append(result.iterations.get(i).json(result.mass))
          .append(i + 1 < result.iterations.size() ? ",\n" : "\n");
    }
    s.append("]}\n");
    try (OutputStream out = fs.create(new Path(work, "iterations.json"), true)) {
      out.write(s.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: PageRank <Adjacency output glob> <work dir>");
      return 2;
    }
    Result r = run(getConf(), args[0], args[1]);
    System.err.println("pagerank: " + r.nodes + " articles, " + r.edges + " edges, " + r.iterations.size()
        + " iterations, mass " + r.mass + " kept to the unit");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PageRank(), args));
  }
}
