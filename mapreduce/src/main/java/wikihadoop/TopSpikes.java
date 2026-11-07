package wikihadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The largest spikes per wiki and day, among content pages.
 *
 * <p>Reads SpikeHours' output. The mappers drop titles that are not ranked ({@link Wikis}: other
 * namespaces and the main page) and key each spike by wiki and day; the reducer keeps the
 * {@code wh.top} (default 10) largest by excess views in a heap that never holds more than that, so
 * its memory does not grow with the number of spikes. Ties go to the title that sorts first.
 *
 * <p>Output: {@code rank} followed by the spike's SpikeHours line, ordered by wiki, day and rank.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.TopSpikes &lt;spike-hours output&gt; &lt;output&gt;</pre>
 */
public final class TopSpikes extends Configured implements Tool {

  public enum Counts { SPIKES_READ, NOT_RANKED, KEPT }

  static final String TOP = "wh.top";

  public static final class RankableMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private Wikis wikis;

    @Override
    protected void setup(Context context) {
      wikis = Wikis.load();
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] f = line.toString().split("\t", 4);
      context.getCounter(Counts.SPIKES_READ).increment(1);
      if (!wikis.ranked(f[0], f[1])) {
        context.getCounter(Counts.NOT_RANKED).increment(1);
        return;
      }
      key.set(f[0] + "\t" + f[2]);
      context.write(key, line);
    }
  }

  private static double excess(String line) {
    return Double.parseDouble(line.split("\t", 7)[5]);
  }

  private static String title(String line) {
    return line.split("\t", 3)[1];
  }

  /** Larger excess first; on equal excess, the title that sorts first. */
  static final Comparator<String> BIGGEST_FIRST =
      Comparator.comparingDouble(TopSpikes::excess).reversed().thenComparing(TopSpikes::title);

  public static final class TopReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text out = new Text();
    private int top;

    @Override
    protected void setup(Context context) {
      top = context.getConfiguration().getInt(TOP, 10);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // The heap's head is the smallest spike kept, so it is the one to drop when a bigger one comes.
      PriorityQueue<String> heap = new PriorityQueue<>(BIGGEST_FIRST.reversed());
      for (Text v : values) {
        heap.add(v.toString());
        if (heap.size() > top) {
          heap.poll();
        }
      }
      List<String> kept = new ArrayList<>(heap);
      kept.sort(BIGGEST_FIRST);
      for (int i = 0; i < kept.size(); i++) {
        context.getCounter(Counts.KEPT).increment(1);
        out.set((i + 1) + "\t" + kept.get(i));
        context.write(out, NullWritable.get());
      }
    }
  }

  public static Job configure(Configuration conf, String spikeHours, String output) throws IOException {
    Job job = Job.getInstance(conf, "top spikes");
    job.setJarByClass(TopSpikes.class);
    job.setMapperClass(RankableMapper.class);
    job.setReducerClass(TopReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path(spikeHours));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: TopSpikes <spike-hours output> <output>");
      return 2;
    }
    return configure(getConf(), args[0], args[1]).waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new TopSpikes(), args));
  }
}
