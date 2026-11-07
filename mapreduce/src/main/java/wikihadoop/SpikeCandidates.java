package wikihadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Pass one of three over a window: which articles could have a spike, and every view counted.
 *
 * <p>Reads the window's {@code user} and {@code automated} files. A day can be a spike only if the
 * article's views that day, summed over its three access methods, reach the wiki's floor (one in
 * ten thousand of its user views that day, from the DomainHourTotals output in the distributed
 * cache). If they do, at least one of its rows has a third of the floor; so any article with such a
 * row, by either agent, is a candidate. The test is necessary, not sufficient: pass two applies the
 * rule itself to the candidates' full data. Only the ranked wikis ({@link Wikis}) get candidates.
 *
 * <p>Every row's views also go into per {@code (date, wiki, agent, access)} totals, summed inside
 * each mapper and emitted once in cleanup (in-mapper combining, so the totals add no records to the
 * sort). The totals are what the checks compare with DomainHourTotals, and they give pass three
 * each wiki's share of each access method.
 *
 * <p>Outputs, one reducer: {@code candidates-r-00000} ({@code wiki<TAB>title<TAB>rows}) and
 * {@code totals-r-00000} ({@code date<TAB>wiki<TAB>agent<TAB>access<TAB>views}).
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.SpikeCandidates -D wh.window.start=2026-10-01 \
 *     -D wh.window.days=31 &lt;domain-hour output&gt; &lt;output&gt; &lt;input&gt;...</pre>
 */
public final class SpikeCandidates extends Configured implements Tool {

  public enum Views { USER, AUTOMATED }

  static final String DOMAIN_HOUR = "domainhour";

  public static final class CandidateMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final PageviewLine row = new PageviewLine();
    private final Map<String, Long> totals = new HashMap<>();
    private final Map<String, Double> floors = new HashMap<>();
    private final Text key = new Text();
    private final LongWritable one = new LongWritable(1);
    private Wikis wikis;
    private String date;
    private String agent;
    private Views views;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      date = InputFiles.date(name);
      agent = InputFiles.agent(name);
      views = agent.equals("user") ? Views.USER : Views.AUTOMATED;
      if (Window.of(context.getConfiguration()).index(date) < 0) {
        throw new IOException(name + " is outside the window");
      }
      wikis = Wikis.load();
      WikiTotals wikiTotals = new WikiTotals();
      CacheFiles.lines(context, DOMAIN_HOUR, wikiTotals::add);
      for (String wiki : wikis.names()) {
        floors.put(wiki, SpikeRule.floor(wikiTotals.userViews(wiki, date)));
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      PageviewLine.Fate fate = row.parse(line.toString());
      context.getCounter(fate).increment(1);
      if (fate != PageviewLine.Fate.PARSED) {
        return;
      }
      context.getCounter(views).increment(row.daily);
      totals.merge(date + "\t" + row.wiki + "\t" + agent + "\t" + row.access, row.daily, Long::sum);
      Double floor = floors.get(row.wiki);
      if (floor != null && floor > 0 && row.daily * 3.0 >= floor) {
        key.set("C\t" + row.wiki + "\t" + row.title);
        context.write(key, one);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      LongWritable value = new LongWritable();
      for (Map.Entry<String, Long> e : totals.entrySet()) {
        key.set("T\t" + e.getKey());
        value.set(e.getValue());
        context.write(key, value);
      }
    }
  }

  public static final class CandidateReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable v : values) {
        sum += v.get();
      }
      String k = key.toString();
      line.set(k.substring(2) + "\t" + sum);
      out.write(k.startsWith("C\t") ? "candidates" : "totals", line, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  /** Map input records minus the lines with a fate; zero when every line is accounted for. */
  public static long unaccounted(Counters counters) {
    long fated = 0;
    for (PageviewLine.Fate fate : PageviewLine.Fate.values()) {
      fated += counters.findCounter(fate).getValue();
    }
    return counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue() - fated;
  }

  public static Job configure(Configuration conf, String domainHour, String output, String... inputs)
      throws IOException {
    Window.of(conf);  // fails early without a window
    Job job = Job.getInstance(conf, "spike candidates");
    job.setJarByClass(SpikeCandidates.class);
    job.setMapperClass(CandidateMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(CandidateReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "candidates", TextOutputFormat.class, Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, "totals", TextOutputFormat.class, Text.class, NullWritable.class);
    CacheFiles.add(job, DOMAIN_HOUR, domainHour);
    for (String input : inputs) {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("usage: SpikeCandidates <domain-hour output> <output> <input>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], java.util.Arrays.copyOfRange(args, 2, args.length));
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    long missing = unaccounted(job.getCounters());
    if (missing != 0) {
      System.err.println("conservation failed: " + missing + " input lines have no fate");
      return 3;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SpikeCandidates(), args));
  }
}
