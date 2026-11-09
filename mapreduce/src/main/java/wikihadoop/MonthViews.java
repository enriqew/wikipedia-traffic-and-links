package wikihadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
 * Linked against read, job one of four: each title's views over a window, by agent.
 *
 * <p>Reads the window's {@code user} and {@code automated} pageview_complete files and keeps the
 * rows of the wikis in {@code wh.views.wikis} (comma-separated codes such as {@code en.wikipedia}),
 * summing each title's views over its days and access methods; the combiner folds a title's rows
 * within a map. Every line has a fate ({@link PageviewLine.Fate}); rows of other wikis are counted
 * in {@link Counts#OTHER_WIKI_ROWS}, and the views kept in the group {@link #VIEWS} by
 * {@code <wiki> <agent>}, which the checks compare with DomainHourTotals.
 *
 * <p>Output {@code views-r-*}: {@code wiki title user automated}.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.MonthViews -D wh.views.wikis=en.wikipedia &lt;output&gt; &lt;input&gt;...</pre>
 */
public final class MonthViews extends Configured implements Tool {

  public enum Counts { OTHER_WIKI_ROWS, TITLES }

  static final String VIEWS = "month views";
  static final String OUTPUT = "views";

  static Set<String> wikis(Configuration conf) throws IOException {
    String[] names = conf.getTrimmedStrings("wh.views.wikis");
    if (names.length == 0) {
      throw new IOException("set wh.views.wikis to the wikis to keep");
    }
    return new HashSet<>(Arrays.asList(names));
  }

  public static final class ViewMapper extends Mapper<LongWritable, Text, Text, AgentViews> {
    private final PageviewLine row = new PageviewLine();
    private final Text key = new Text();
    private final AgentViews value = new AgentViews();
    private Set<String> wikis;
    private boolean automated;
    private String agent;

    @Override
    protected void setup(Context context) throws IOException {
      wikis = wikis(context.getConfiguration());
      agent = InputFiles.agent(((FileSplit) context.getInputSplit()).getPath().getName());
      automated = agent.equals("automated");
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      PageviewLine.Fate fate = row.parse(line.toString());
      context.getCounter(fate).increment(1);
      if (fate != PageviewLine.Fate.PARSED) {
        return;
      }
      if (!wikis.contains(row.wiki)) {
        context.getCounter(Counts.OTHER_WIKI_ROWS).increment(1);
        return;
      }
      context.getCounter(VIEWS, row.wiki + " " + agent).increment(row.daily);
      key.set(row.wiki + "\t" + row.title);
      value.set(automated ? 0 : row.daily, automated ? row.daily : 0);
      context.write(key, value);
    }
  }

  public static final class SumCombiner extends Reducer<Text, AgentViews, Text, AgentViews> {
    private final AgentViews sum = new AgentViews();

    @Override
    protected void reduce(Text key, Iterable<AgentViews> values, Context context)
        throws IOException, InterruptedException {
      long user = 0;
      long automated = 0;
      for (AgentViews v : values) {
        user += v.user;
        automated += v.automated;
      }
      sum.set(user, automated);
      context.write(key, sum);
    }
  }

  public static final class SumReducer extends Reducer<Text, AgentViews, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<AgentViews> values, Context context)
        throws IOException, InterruptedException {
      long user = 0;
      long automated = 0;
      for (AgentViews v : values) {
        user += v.user;
        automated += v.automated;
      }
      context.getCounter(Counts.TITLES).increment(1);
      line.set(key + "\t" + user + "\t" + automated);
      out.write(OUTPUT, line, NullWritable.get(), OUTPUT);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String output, String... inputs) throws IOException {
    wikis(conf);
    Job job = Job.getInstance(conf, "month views");
    job.setJarByClass(MonthViews.class);
    job.setMapperClass(ViewMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(SumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AgentViews.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, OUTPUT, TextOutputFormat.class, Text.class, NullWritable.class);
    for (String input : inputs) {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  /** What the counters say is wrong, or an empty string. */
  static String failures(Counters c) {
    long missing = DomainHourTotals.unaccounted(c);
    return missing == 0 ? "" : missing + " input lines without a fate";
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: MonthViews -D wh.views.wikis=<wiki,...> <output> <input>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], Arrays.copyOfRange(args, 1, args.length));
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
    System.exit(ToolRunner.run(new MonthViews(), args));
  }
}
