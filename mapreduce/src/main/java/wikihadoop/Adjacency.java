package wikihadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Link graph, job four of four: each article's distinct out-links.
 *
 * <p>A reduce-side join keyed by page id over the {@code page} dump (namespace 0) and {@link Edges}'
 * output, so each reduce call holds a page and the edge rows that start at it. A row from a page
 * that is a redirect, or that the page table does not hold, ends here; an article's rows lose
 * self-links and duplicates (set semantics) and become its out-links, sorted. Every article is
 * written, with no out-links if it has none.
 *
 * <p>Three outputs: {@code adjacency} ({@code page_id out_degree targets}, comma-separated),
 * {@code titles} ({@code page_id title}, articles) and {@code redirects} ({@code page_id title},
 * redirect pages, for the join with views in phase 7).
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.Adjacency &lt;Edges output&gt; &lt;output&gt; &lt;page dumps&gt;...</pre>
 */
public final class Adjacency extends Configured implements Tool {

  public enum Counts { ARTICLES, ARTICLES_WITHOUT_LINKS, EDGES, PAGE_TWICE }

  static final String ADJACENCY = "adjacency";
  static final String TITLES = "titles";
  static final String REDIRECT_TITLES = "redirects";
  static final String EDGES_READ = "edges read";

  public static final class PageMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final DumpLine dump = new DumpLine();
    private final Text key = new Text();
    private final Text value = new Text();
    private boolean edges;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      edges = name.startsWith("part-r-");
      if (!edges && !DumpLine.table(name).equals("page")) {
        throw new IOException("Adjacency reads the page table, not " + name);
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      if (edges) {
        DumpLine.count(context, "edges", "read");
        String[] f = line.toString().split("\t", -1);  // from to how
        key.set(f[0]);
        value.set("E\t" + f[1] + "\t" + f[2]);
      } else {
        List<String> v = dump.read(line, "page", context);  // page_id, namespace, title, is_redirect, ...
        if (v == null || !v.get(1).equals("0")) {
          return;
        }
        key.set(v.get(0));
        value.set("P\t" + v.get(3) + "\t" + v.get(2));
      }
      context.write(key, value);
    }
  }

  public static final class AdjacencyReducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();
    private long[] targets = new long[256];
    private boolean[] direct = new boolean[256];

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String redirect = null;
      String title = null;
      int n = 0;
      for (Text t : values) {
        String[] f = t.toString().split("\t", 3);
        if (f[0].equals("P")) {
          if (title != null) {
            context.getCounter(Counts.PAGE_TWICE).increment(1);
          }
          redirect = f[1];
          title = f[2];
        } else {
          if (n == targets.length) {
            targets = Arrays.copyOf(targets, 2 * n);
            direct = Arrays.copyOf(direct, 2 * n);
          }
          targets[n] = Long.parseLong(f[1]);
          direct[n++] = f[2].equals("direct");
        }
      }
      String id = key.toString();
      if (title == null) {
        context.getCounter(LinkFate.FROM_MISSING).increment(n);
        return;
      }
      if (redirect.equals("1")) {
        context.getCounter(LinkFate.FROM_REDIRECT).increment(n);
        write(REDIRECT_TITLES, id + "\t" + title);
        return;
      }
      long self = Long.parseLong(id);
      Map<Long, Boolean> links = new TreeMap<>();
      for (int i = 0; i < n; i++) {
        if (targets[i] == self) {
          context.getCounter(LinkFate.SELF).increment(1);
        } else if (links.containsKey(targets[i])) {
          context.getCounter(LinkFate.DUPLICATE).increment(1);
          links.put(targets[i], links.get(targets[i]) || direct[i]);
        } else {
          links.put(targets[i], direct[i]);
        }
      }
      StringBuilder csv = new StringBuilder();
      for (Map.Entry<Long, Boolean> e : links.entrySet()) {
        csv.append(csv.length() == 0 ? "" : ",").append(e.getKey());
        context.getCounter(e.getValue() ? LinkFate.EDGE_DIRECT : LinkFate.EDGE_VIA).increment(1);
      }
      context.getCounter(Counts.ARTICLES).increment(1);
      context.getCounter(Counts.EDGES).increment(links.size());
      if (links.isEmpty()) {
        context.getCounter(Counts.ARTICLES_WITHOUT_LINKS).increment(1);
      }
      write(ADJACENCY, id + "\t" + links.size() + "\t" + csv);
      write(TITLES, id + "\t" + title);
    }

    private void write(String name, String s) throws IOException, InterruptedException {
      line.set(s);
      out.write(name, line, NullWritable.get(), name);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String edges, String output, String... pages)
      throws IOException {
    Job job = Job.getInstance(conf, "adjacency");
    job.setJarByClass(Adjacency.class);
    job.setMapperClass(PageMapper.class);
    job.setReducerClass(AdjacencyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    for (String name : new String[] {ADJACENCY, TITLES, REDIRECT_TITLES}) {
      MultipleOutputs.addNamedOutput(job, name, TextOutputFormat.class, Text.class, NullWritable.class);
    }
    FileInputFormat.addInputPath(job, new Path(edges, "part-r-*"));
    for (String p : pages) {
      FileInputFormat.addInputPath(job, new Path(p));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    long missing = DumpLine.unaccounted(c);
    if (missing != 0) {
      s.append(missing).append(" input lines without a fate; ");
    }
    long bad = DumpLine.malformed(c, "page");
    if (bad != 0) {
      s.append(bad).append(" malformed page lines; ");
    }
    long twice = c.findCounter(Counts.PAGE_TWICE).getValue();
    if (twice != 0) {
      s.append(twice).append(" page ids with two rows; ");
    }
    long read = c.findCounter(DumpLine.GROUP, EDGES_READ).getValue();
    long fated = Edges.fates(c);
    if (read != fated) {
      s.append(read).append(" edge rows read, ").append(fated).append(" with a fate; ");
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("usage: Adjacency <Edges output> <output> <page dumps>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], Arrays.copyOfRange(args, 2, args.length));
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
    System.exit(ToolRunner.run(new Adjacency(), args));
  }
}
