package wikihadoop;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Link graph, job three of four: each {@code pagelinks} row against its resolved target.
 *
 * <p>A reduce-side join keyed by link target id over the {@code pagelinks} dump, {@link
 * LinkTargets}' {@code targets} (the ids that name an article, or nothing) and {@link Redirects}'
 * output (the ids that name a redirect). Rows from another namespace end here, in the map; the
 * reducer gives each remaining row its target's fate, or passes it on as an edge.
 *
 * <p>Output: {@code from to direct|via}, one line per row that reaches an article; {@link
 * Adjacency} decides what becomes of it. The {@link LinkFate} counters of this job and that one
 * sum to the table's rows.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.Edges &lt;LinkTargets output&gt; &lt;Redirects output&gt; &lt;output&gt; &lt;pagelinks dumps&gt;...</pre>
 */
public final class Edges extends Configured implements Tool {

  public enum Counts { EDGE_ROWS, TARGETS_RESOLVED_TWICE }

  static final String PAGELINKS = "pagelinks";
  static final String RESOLVED = "resolved";

  public static final class TargetMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final DumpLine dump = new DumpLine();
    private final Text key = new Text();
    private final Text value = new Text();
    private String source;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (name.startsWith(LinkTargets.TARGETS + "-r-")) {
        source = LinkTargets.TARGETS;
      } else if (name.startsWith("part-r-")) {
        source = RESOLVED;
      } else if (DumpLine.table(name).equals(PAGELINKS)) {
        source = PAGELINKS;
      } else {
        throw new IOException("Edges reads pagelinks, not " + name);
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      if (source.equals(PAGELINKS)) {
        java.util.List<String> v = dump.read(line, PAGELINKS, context);  // pl_from, pl_from_namespace, pl_target_id
        if (v == null) {
          return;
        }
        if (!v.get(1).equals("0")) {
          context.getCounter(LinkFate.FROM_OTHER_NAMESPACE).increment(1);
          return;
        }
        key.set(v.get(2));
        value.set("F\t" + v.get(0));
      } else {
        String[] f = line.toString().split("\t", -1);  // lt_id kind value
        if (f[1].equals("REDIRECT")) {
          DumpLine.count(context, source, "to a redirect, resolved by Redirects");
          return;
        }
        DumpLine.count(context, source, "read");
        key.set(f[0]);
        if (f[1].equals("ARTICLE")) {
          value.set("A\t" + f[2]);
        } else if (f[1].equals("VIA")) {
          value.set("V\t" + f[2]);
        } else {
          value.set("X\t" + f[1]);
        }
      }
      context.write(key, value);
    }
  }

  public static final class EdgeReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text line = new Text();
    private long[] froms = new long[1024];

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String kind = null;  // A, V or X
      String target = null;
      int n = 0;
      for (Text t : values) {
        String s = t.toString();
        if (s.charAt(0) == 'F') {
          if (n == froms.length) {
            froms = Arrays.copyOf(froms, 2 * n);
          }
          froms[n++] = Long.parseLong(s.substring(2));
        } else {
          if (kind != null) {
            context.getCounter(Counts.TARGETS_RESOLVED_TWICE).increment(1);
          }
          kind = s.substring(0, 1);
          target = s.substring(2);
        }
      }
      if (kind == null || kind.equals("X")) {
        LinkFate fate = kind == null ? LinkFate.TARGET_ID_MISSING : LinkFate.valueOf(target);
        context.getCounter(fate).increment(n);
        return;
      }
      String how = kind.equals("A") ? "direct" : "via";
      for (int i = 0; i < n; i++) {
        line.set(froms[i] + "\t" + target + "\t" + how);
        context.write(line, NullWritable.get());
      }
      context.getCounter(Counts.EDGE_ROWS).increment(n);
    }
  }

  public static Job configure(Configuration conf, String linkTargets, String redirects, String output,
      String... pagelinks) throws IOException {
    Job job = Job.getInstance(conf, "edges");
    job.setJarByClass(Edges.class);
    job.setMapperClass(TargetMapper.class);
    job.setReducerClass(EdgeReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    FileInputFormat.addInputPath(job, new Path(linkTargets, LinkTargets.TARGETS + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(redirects, "part-r-*"));
    for (String p : pagelinks) {
      FileInputFormat.addInputPath(job, new Path(p));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  /** The {@link LinkFate} counters of one job, summed. */
  static long fates(Counters c) {
    long n = 0;
    for (LinkFate f : LinkFate.values()) {
      n += c.findCounter(f).getValue();
    }
    return n;
  }

  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    long missing = DumpLine.unaccounted(c);
    if (missing != 0) {
      s.append(missing).append(" input lines without a fate; ");
    }
    long bad = DumpLine.malformed(c, PAGELINKS);
    if (bad != 0) {
      s.append(bad).append(" malformed pagelinks lines; ");
    }
    long twice = c.findCounter(Counts.TARGETS_RESOLVED_TWICE).getValue();
    if (twice != 0) {
      s.append(twice).append(" link targets resolved twice; ");
    }
    long rows = c.findCounter(DumpLine.GROUP, PAGELINKS + " " + DumpLine.Fate.ROW).getValue();
    long accounted = fates(c) + c.findCounter(Counts.EDGE_ROWS).getValue();
    if (rows != accounted) {
      s.append(rows).append(" pagelinks rows, ").append(accounted).append(" with a fate or passed on; ");
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("usage: Edges <LinkTargets output> <Redirects output> <output> <pagelinks dumps>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    String failed = failures(job.getCounters());
    if (!failed.isEmpty()) {
      System.err.println("checks failed: " + failed);
      return 3;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Edges(), args));
  }
}
