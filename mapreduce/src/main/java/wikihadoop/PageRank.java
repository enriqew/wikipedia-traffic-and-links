package wikihadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 * <p>A run that stopped can go on from an iteration whose output survived. {@code
 * wh.pagerank.resume} names a file in the format of iterations.json holding iterations 1 to k;
 * the driver keeps the work directory, removes anything past {@code iter-k}, and runs iteration
 * k + 1 from {@code iter-k} with iteration k's share and carry. Everything an iteration needs is in
 * the records of the one before and in those two numbers, all integers, so the ranks are the ones
 * an unbroken run gives, unit for unit. The iterations.json it writes carries the earlier lines as
 * they were and says where the run was resumed.
 *
 * <p>Without a resume file the run starts over, and it refuses a work directory that already
 * exists unless {@code wh.pagerank.overwrite} is true: a resume flag lost on its way to the driver
 * then stops the run instead of deleting the iterations it was meant to go on from.
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
    /** The iteration a resumed run went on from; 0 for a run from the start. */
    int resumedAfter;
    /** Iterations 1 to resumedAfter, as their lines in the file the run was resumed from. */
    final List<String> earlier = new ArrayList<>();
    final List<Iteration> iterations = new ArrayList<>();

    int count() {
      return earlier.size() + iterations.size();
    }
  }

  private static final Pattern HEADER = Pattern.compile(
      "\\{\"articles\": (\\d+), \"edges\": (\\d+), \"mass\": (\\d+), \"units_per_article\": (\\d+),");
  private static final Pattern LINE = Pattern.compile(
      "\\{\"iteration\": (\\d+), .*\"share\": (\\d+), \"carry\": (\\d+), .*\\}");

  /** Reads an iterations.json into a Result to go on from, with the last iteration's share and carry. */
  static long[] readResume(FileSystem fs, String file, Result result) throws IOException {
    List<String> lines = new ArrayList<>();
    try (BufferedReader in = new BufferedReader(
        new InputStreamReader(fs.open(new Path(file)), StandardCharsets.UTF_8))) {
      for (String line; (line = in.readLine()) != null; ) {
        lines.add(line.trim());
      }
    }
    Matcher h = HEADER.matcher(lines.isEmpty() ? "" : lines.get(0));
    require(h.lookingAt(), file + ": no header");
    require(Long.parseLong(h.group(4)) == PageRankMass.UNITS, file + ": another unit per article");
    result.nodes = Long.parseLong(h.group(1));
    result.edges = Long.parseLong(h.group(2));
    result.mass = Long.parseLong(h.group(3));
    require(result.mass == Math.multiplyExact(result.nodes, PageRankMass.UNITS), file + ": mass is not N x units");
    long share = 0;
    long carry = 0;
    for (String line : lines.subList(1, lines.size())) {
      if (!line.startsWith("{\"iteration\"")) {
        continue;
      }
      String object = line.endsWith(",") ? line.substring(0, line.length() - 1) : line;
      Matcher m = LINE.matcher(object);
      require(m.matches(), file + ": cannot read " + object);
      require(Integer.parseInt(m.group(1)) == result.earlier.size() + 1, file + ": iterations not numbered 1, 2, ...");
      share = Long.parseLong(m.group(2));
      carry = Long.parseLong(m.group(3));
      result.earlier.add(object);
    }
    require(!result.earlier.isEmpty(), file + ": no iterations");
    result.resumedAfter = result.earlier.size();
    return new long[] {share, carry};
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
    String resume = conf.get("wh.pagerank.resume");
    FileSystem fs = FileSystem.get(conf);
    Path dir = new Path(work);
    Result result = new Result();
    long share = 0;
    long carry = 0;
    if (resume == null) {
      if (fs.exists(dir)) {
        require(conf.getBoolean("wh.pagerank.overwrite", false), work + " already holds a run: set "
            + "wh.pagerank.resume to go on from it, or wh.pagerank.overwrite=true to start over");
        fs.delete(dir, true);
      }
      Counters init = complete(PageRankJobs.init(conf, adjacency, work + "/iter-000"));
      require(get(init, PageRankJobs.Counts.BAD_LINES) == 0, "adjacency lines whose out-degree is not their link count");
      result.nodes = get(init, PageRankJobs.Counts.NODES);
      result.edges = get(init, PageRankJobs.Counts.EDGES);
      result.mass = Math.multiplyExact(result.nodes, PageRankMass.UNITS);
    } else {
      long[] last = readResume(fs, resume, result);
      share = last[0];
      carry = last[1];
      int k = result.resumedAfter;
      require(fs.exists(new Path(iter(work, k), "_SUCCESS")), iter(work, k) + " is not a finished iteration");
      for (int j = k + 1; fs.exists(new Path(iter(work, j))); j++) {
        fs.delete(new Path(iter(work, j)), true);
      }
      fs.delete(new Path(work, "ranks"), true);
      System.err.println("pagerank: resuming after iteration " + k + " with share " + share + " and carry " + carry);
    }
    long n = result.nodes;
    long mass = result.mass;

    int last = result.resumedAfter;
    for (int j = result.resumedAfter + 1; j <= max; j++) {
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
        + "\"damping\": 0.85, %s\"iterations\": [\n", result.nodes, result.edges, result.mass, PageRankMass.UNITS,
        result.resumedAfter > 0 ? "\"resumed_after\": " + result.resumedAfter + ", " : ""));
    List<String> lines = new ArrayList<>(result.earlier);
    for (Iteration it : result.iterations) {
      lines.add(it.json(result.mass));
    }
    for (int i = 0; i < lines.size(); i++) {
      s.append("  ").append(lines.get(i)).append(i + 1 < lines.size() ? ",\n" : "\n");
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
    System.err.println("pagerank: " + r.nodes + " articles, " + r.edges + " edges, " + r.count()
        + " iterations" + (r.resumedAfter > 0 ? " (resumed after " + r.resumedAfter + ")" : "")
        + ", mass " + r.mass + " kept to the unit");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new PageRank(), args));
  }
}
