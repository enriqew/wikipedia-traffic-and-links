package wikihadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The three kinds of PageRank job (plan section 14), run by {@link PageRank}.
 *
 * <ul>
 *   <li><b>init</b>, map only: Adjacency's text output to one binary record per article, rank
 *       {@link PageRankMass#UNITS};
 *   <li><b>iteration</b>, once per iteration: each article's rank plus the pool's share is passed
 *       along its out-links, what is not passed is counted into the next pool, and each article's
 *       record goes through the shuffle with its contributions, so the graph is rewritten every time;
 *   <li><b>ranks</b>, map only: the last iteration's records to text, {@code page_id rank}, with the
 *       last pool's share added.
 * </ul>
 * The share of the pool reaches the mappers through {@link #SHARE} in the job's configuration.
 */
public final class PageRankJobs {

  public enum Counts {
    NODES, EDGES, BAD_LINES, RANK_IN, POOL, DELTA, CONTRIBUTIONS, LINK_MASS, RECORDS_TWICE,
    MASS_WITHOUT_RECORD, MASS_OUT
  }

  static final String SHARE = "wh.pagerank.share";

  private PageRankJobs() {}

  static long share(Configuration conf) throws IOException {
    long share = conf.getLong(SHARE, -1);
    if (share < 0) {
      throw new IOException(SHARE + " is not set");
    }
    return share;
  }

  public static final class InitMapper extends Mapper<LongWritable, Text, LongWritable, NodeWritable> {
    private final LongWritable id = new LongWritable();
    private final NodeWritable node = new NodeWritable();
    private long[] links = new long[1024];

    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
      String[] f = line.toString().split("\t", -1);  // page_id out_degree targets
      int k = Integer.parseInt(f[1]);
      String[] t = f[2].isEmpty() ? new String[0] : f[2].split(",");
      if (t.length != k) {
        context.getCounter(Counts.BAD_LINES).increment(1);
        return;
      }
      if (links.length < k) {
        links = new long[Math.max(k, 2 * links.length)];
      }
      for (int i = 0; i < k; i++) {
        links[i] = Long.parseLong(t[i]);
      }
      id.set(Long.parseLong(f[0]));
      context.write(id, node.record(PageRankMass.UNITS, PageRankMass.UNITS, links, k));
      context.getCounter(Counts.NODES).increment(1);
      context.getCounter(Counts.EDGES).increment(k);
    }
  }

  public static final class IterationMapper extends Mapper<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    private final LongWritable target = new LongWritable();
    private final NodeWritable contribution = new NodeWritable();
    private long share;

    @Override
    protected void setup(Context context) throws IOException {
      share = share(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable id, NodeWritable node, Context context) throws IOException, InterruptedException {
      if (!node.isRecord()) {
        throw new IOException("an iteration's input holds records only; " + id + " is a contribution");
      }
      long r = node.rank() + share;
      context.getCounter(Counts.RANK_IN).increment(r);
      context.getCounter(Counts.DELTA).increment(Math.abs(r - node.previous()));
      int k = node.size();
      long per = PageRankMass.perLink(r, k);
      if (per > 0) {
        contribution.contribution(per);
        for (int i = 0; i < k; i++) {
          target.set(node.target(i));
          context.write(target, contribution);
        }
        context.getCounter(Counts.CONTRIBUTIONS).increment(k);
      }
      context.getCounter(Counts.POOL).increment(PageRankMass.toPool(r, k));
      context.write(id, node.rerank(0, r));
    }
  }

  /** Sums the contributions to a page within one map; records pass through untouched. */
  public static final class SumCombiner extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    private final NodeWritable sum = new NodeWritable();

    @Override
    protected void reduce(LongWritable id, Iterable<NodeWritable> values, Context context)
        throws IOException, InterruptedException {
      long mass = 0;
      boolean any = false;
      for (NodeWritable v : values) {
        if (v.isRecord()) {
          context.write(id, v);
        } else {
          mass += v.rank();
          any = true;
        }
      }
      if (any) {
        context.write(id, sum.contribution(mass));
      }
    }
  }

  public static final class IterationReducer extends Reducer<LongWritable, NodeWritable, LongWritable, NodeWritable> {
    private final NodeWritable held = new NodeWritable();

    @Override
    protected void reduce(LongWritable id, Iterable<NodeWritable> values, Context context)
        throws IOException, InterruptedException {
      long mass = 0;
      int records = 0;
      for (NodeWritable v : values) {
        if (v.isRecord()) {
          held.set(v);
          records++;
        } else {
          mass += v.rank();
        }
      }
      if (records == 0) {
        context.getCounter(Counts.MASS_WITHOUT_RECORD).increment(mass);
        return;
      }
      if (records > 1) {
        context.getCounter(Counts.RECORDS_TWICE).increment(1);
      }
      context.write(id, held.rerank(mass, held.previous()));
      context.getCounter(Counts.LINK_MASS).increment(mass);
      context.getCounter(Counts.NODES).increment(1);
    }
  }

  public static final class RanksMapper extends Mapper<LongWritable, NodeWritable, Text, NullWritable> {
    private final Text line = new Text();
    private long share;

    @Override
    protected void setup(Context context) throws IOException {
      share = share(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable id, NodeWritable node, Context context) throws IOException, InterruptedException {
      long r = node.rank() + share;
      line.set(id.get() + "\t" + r);
      context.write(line, NullWritable.get());
      context.getCounter(Counts.MASS_OUT).increment(r);
      context.getCounter(Counts.NODES).increment(1);
    }
  }

  private static void binaryOutput(Job job, String output) {
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NodeWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(output));
  }

  public static Job init(Configuration conf, String adjacency, String output) throws IOException {
    Job job = Job.getInstance(conf, "pagerank init");
    job.setJarByClass(PageRankJobs.class);
    job.setMapperClass(InitMapper.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job, new Path(adjacency));
    binaryOutput(job, output);
    return job;
  }

  public static Job iteration(Configuration conf, String input, String output, long share, int number)
      throws IOException {
    Job job = Job.getInstance(conf, "pagerank iteration " + number);
    job.getConfiguration().setLong(SHARE, share);
    job.setJarByClass(PageRankJobs.class);
    job.setMapperClass(IterationMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setReducerClass(IterationReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NodeWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(input));
    binaryOutput(job, output);
    return job;
  }

  public static Job ranks(Configuration conf, String input, String output, long share) throws IOException {
    Job job = Job.getInstance(conf, "pagerank ranks");
    job.getConfiguration().setLong(SHARE, share);
    job.setJarByClass(PageRankJobs.class);
    job.setMapperClass(RanksMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }
}
