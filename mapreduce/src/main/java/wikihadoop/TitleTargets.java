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
 * Linked against read, job two of four: the article each title of namespace 0 names.
 *
 * <p>A reduce-side join keyed by page id over the graph's article titles and redirect titles
 * ({@link Adjacency}) and the redirect hops ({@link LinkTargets}). An article's title names it; a
 * redirect's title names the article its hop reaches, one hop as in the graph. A redirect whose hop
 * goes anywhere else keeps the hop's fate: {@code DOUBLE_REDIRECT}, {@code REDIRECT_OUT_OF_NS0},
 * {@code REDIRECT_INTERWIKI}, {@code REDIRECT_TO_MISSING}, or {@code REDIRECT_WITHOUT_ROW} when the
 * redirect table has no row for it. Hops from redirect pages outside namespace 0 are not titles
 * here and are only counted.
 *
 * <p>Output {@code targets-r-*}: {@code title article_id article|redirect}, or {@code title - fate}.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.TitleTargets &lt;Adjacency output&gt; &lt;LinkTargets output&gt; &lt;output&gt;</pre>
 */
public final class TitleTargets extends Configured implements Tool {

  public enum Counts { ARTICLES, REDIRECTS_TO_ARTICLES, REDIRECTS_ELSEWHERE, HOPS_OUTSIDE_NS0, PAGE_TWICE }

  static final String OUTPUT = "targets";

  public static final class TitleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private final Text value = new Text();
    private String tag;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (name.startsWith(Adjacency.TITLES + "-")) {
        tag = "A";
      } else if (name.startsWith(Adjacency.REDIRECT_TITLES + "-")) {
        tag = "R";
      } else if (name.startsWith(LinkTargets.HOPS + "-")) {
        tag = "H";
      } else {
        throw new IOException("TitleTargets reads titles, redirects and hops, not " + name);
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

  public static final class TargetReducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String article = null;
      String redirect = null;
      String hop = null;
      int pages = 0;
      for (Text t : values) {
        String[] f = t.toString().split("\t", 2);
        switch (f[0]) {
          case "A": article = f[1]; pages++; break;
          case "R": redirect = f[1]; pages++; break;
          default: hop = f[1];
        }
      }
      if (pages > 1) {
        context.getCounter(Counts.PAGE_TWICE).increment(1);
        return;
      }
      if (article != null) {
        context.getCounter(Counts.ARTICLES).increment(1);
        write(article + "\t" + key + "\tarticle");
      } else if (redirect != null) {
        String[] h = hop == null ? null : hop.split("\t", -1);  // kind, target
        if (h != null && h[0].equals("ARTICLE")) {
          context.getCounter(Counts.REDIRECTS_TO_ARTICLES).increment(1);
          write(redirect + "\t" + h[1] + "\tredirect");
        } else {
          context.getCounter(Counts.REDIRECTS_ELSEWHERE).increment(1);
          String fate = h == null ? "REDIRECT_WITHOUT_ROW" : h[0].equals("REDIRECT") ? "DOUBLE_REDIRECT" : h[0];
          write(redirect + "\t-\t" + fate);
        }
      } else {
        context.getCounter(Counts.HOPS_OUTSIDE_NS0).increment(1);
      }
    }

    private void write(String s) throws IOException, InterruptedException {
      line.set(s);
      out.write(OUTPUT, line, NullWritable.get(), OUTPUT);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String graph, String linkTargets, String output)
      throws IOException {
    Job job = Job.getInstance(conf, "title targets");
    job.setJarByClass(TitleTargets.class);
    job.setMapperClass(TitleMapper.class);
    job.setReducerClass(TargetReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, OUTPUT, TextOutputFormat.class, Text.class, NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(graph, Adjacency.TITLES + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(graph, Adjacency.REDIRECT_TITLES + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(linkTargets, LinkTargets.HOPS + "-r-*"));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  static String failures(Counters c) {
    long twice = c.findCounter(Counts.PAGE_TWICE).getValue();
    return twice == 0 ? "" : twice + " page ids that are both an article and a redirect";
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("usage: TitleTargets <Adjacency output> <LinkTargets output> <output>");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], args[2]);
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
    System.exit(ToolRunner.run(new TitleTargets(), args));
  }
}
