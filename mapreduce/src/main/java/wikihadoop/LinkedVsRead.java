package wikihadoop;

import java.io.IOException;
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
 * Linked against read, job four of four: each article's rank beside its views.
 *
 * <p>A reduce-side join keyed by page id over PageRank's {@code ranks} ({@code page_id rank}), the
 * graph's article titles ({@link Adjacency}) and {@link ViewTargets}' output, whose lines for one
 * article (its own title and each redirect to it) are summed here. Every article is written, viewed
 * or not; an id with a rank and no title, a title and no rank, or views and neither, is counted
 * and fails the run.
 *
 * <p>Output {@code linked-r-*}: {@code page_id title rank user automated user_through_redirects}.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.LinkedVsRead &lt;ranks&gt; &lt;Adjacency output&gt; &lt;ViewTargets output&gt; &lt;output&gt;</pre>
 */
public final class LinkedVsRead extends Configured implements Tool {

  public enum Counts {
    ARTICLES, ARTICLES_VIEWED, RANK_SUM, USER_VIEWS, AUTOMATED_VIEWS, USER_THROUGH_REDIRECTS,
    NO_RANK, NO_TITLE, TWICE, VIEWS_WITHOUT_ARTICLE
  }

  static final String OUTPUT = "linked";

  public static final class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private final Text value = new Text();
    private String tag;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (name.startsWith("part-m-")) {
        tag = "R";
      } else if (name.startsWith(Adjacency.TITLES + "-")) {
        tag = "T";
      } else if (name.startsWith(ViewTargets.OUTPUT + "-")) {
        tag = "V";
      } else {
        throw new IOException("LinkedVsRead reads ranks, titles and article views, not " + name);
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String s = line.toString();
      int tab = s.indexOf('\t');
      key.set(s.substring(0, tab));
      value.set(tag + "\t" + s.substring(tab + 1));
      context.write(key, value);
    }
  }

  public static final class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String rank = null;
      String title = null;
      long user = 0;
      long automated = 0;
      long through = 0;
      boolean viewed = false;
      boolean twice = false;
      for (Text t : values) {
        String[] f = t.toString().split("\t", -1);
        switch (f[0]) {
          case "R":
            twice |= rank != null;
            rank = f[1];
            break;
          case "T":
            twice |= title != null;
            title = f[1];
            break;
          default:  // V user automated via
            long u = Long.parseLong(f[1]);
            user += u;
            automated += Long.parseLong(f[2]);
            if (f[3].equals("1")) {
              through += u;
            }
            viewed = true;
        }
      }
      if (twice) {
        context.getCounter(Counts.TWICE).increment(1);
      }
      if (rank == null || title == null) {
        context.getCounter(rank == null && title == null ? Counts.VIEWS_WITHOUT_ARTICLE
            : rank == null ? Counts.NO_RANK : Counts.NO_TITLE).increment(1);
        return;
      }
      context.getCounter(Counts.ARTICLES).increment(1);
      context.getCounter(Counts.RANK_SUM).increment(Long.parseLong(rank));
      context.getCounter(Counts.USER_VIEWS).increment(user);
      context.getCounter(Counts.AUTOMATED_VIEWS).increment(automated);
      context.getCounter(Counts.USER_THROUGH_REDIRECTS).increment(through);
      if (viewed) {
        context.getCounter(Counts.ARTICLES_VIEWED).increment(1);
      }
      line.set(key + "\t" + title + "\t" + rank + "\t" + user + "\t" + automated + "\t" + through);
      out.write(OUTPUT, line, NullWritable.get(), OUTPUT);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String ranks, String graph, String viewTargets,
      String output) throws IOException {
    Job job = Job.getInstance(conf, "linked against read");
    job.setJarByClass(LinkedVsRead.class);
    job.setMapperClass(JoinMapper.class);
    job.setReducerClass(JoinReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, OUTPUT, TextOutputFormat.class, Text.class, NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(ranks, "part-m-*"));
    FileInputFormat.addInputPath(job, new Path(graph, Adjacency.TITLES + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(viewTargets, ViewTargets.OUTPUT + "-r-*"));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    for (Counts k : new Counts[] {Counts.NO_RANK, Counts.NO_TITLE, Counts.TWICE, Counts.VIEWS_WITHOUT_ARTICLE}) {
      long n = c.findCounter(k).getValue();
      if (n != 0) {
        s.append(n).append(" ").append(k.name()).append("; ");
      }
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("usage: LinkedVsRead <ranks> <Adjacency output> <ViewTargets output> <output>");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], args[2], args[3]);
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    String failed = failures(job.getCounters());
    if (!failed.isEmpty()) {
      System.err.println("check failed: " + failed);
      return 3;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new LinkedVsRead(), args));
  }
}
