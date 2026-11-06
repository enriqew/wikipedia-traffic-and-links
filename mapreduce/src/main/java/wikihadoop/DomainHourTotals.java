package wikihadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Views per domain code and hour, summed from pageview_complete files.
 *
 * <p>The output is what the reconciliation compares with the hourly dumps and projectviews:
 * one line per {@code domain<TAB>hour<TAB>views}, hour 0 to 23. Every input line ends as parsed
 * or as one rejection reason, counted in {@link PageviewLine.Fate}; the job fails unless those
 * counts sum to the map input records. Rows whose wiki has no domain code are parsed but counted
 * under {@link Extra#UNMAPPED_ROWS} and their views under {@link Extra#UNMAPPED_VIEWS}.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.DomainHourTotals &lt;input&gt;... &lt;output&gt;</pre>
 */
public final class DomainHourTotals extends Configured implements Tool {

  public enum Extra { UNMAPPED_ROWS, UNMAPPED_VIEWS }

  public static final class HourMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final PageviewLine row = new PageviewLine();
    private final Text key = new Text();
    private final LongWritable value = new LongWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      PageviewLine.Fate fate = row.parse(line.toString());
      context.getCounter(fate).increment(1);
      if (fate != PageviewLine.Fate.PARSED) {
        return;
      }
      String domain = DomainCode.of(row.wiki, row.access);
      if (domain == null) {
        context.getCounter(Extra.UNMAPPED_ROWS).increment(1);
        context.getCounter(Extra.UNMAPPED_VIEWS).increment(row.daily);
        return;
      }
      for (int hour = 0; hour < 24; hour++) {
        if (row.hours[hour] > 0) {
          key.set(domain + "\t" + hour);
          value.set(row.hours[hour]);
          context.write(key, value);
        }
      }
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

  public static Job configure(Configuration conf, String[] inputs, String output) throws IOException {
    Job job = Job.getInstance(conf, "domain-hour totals");
    job.setJarByClass(DomainHourTotals.class);
    job.setMapperClass(HourMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    // One reducer: the output is one sorted file, so a rerun can be compared by its sha256.
    job.setNumReduceTasks(1);
    for (String input : inputs) {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: DomainHourTotals <input>... <output>");
      return 2;
    }
    String[] inputs = java.util.Arrays.copyOf(args, args.length - 1);
    Job job = configure(getConf(), inputs, args[args.length - 1]);
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
    System.exit(ToolRunner.run(new DomainHourTotals(), args));
  }
}
