package wikihadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
 * Link graph, job two of four: link targets that name a redirect take the redirect's target.
 *
 * <p>A reduce-side join keyed by page id over {@link LinkTargets}' two outputs: the link targets
 * whose title is a redirect page, keyed by that page, and the redirect hops, keyed by the redirect
 * page. One hop: a redirect whose target is itself a redirect gives {@code DOUBLE_REDIRECT}.
 *
 * <p>Output, per link target id: {@code lt_id VIA article_id}, or {@code lt_id <fate> -} with the
 * fate one of {@code DOUBLE_REDIRECT}, {@code REDIRECT_TO_MISSING}, {@code REDIRECT_OUT_OF_NS0},
 * {@code REDIRECT_INTERWIKI} or {@code REDIRECT_WITHOUT_ROW}. Link targets that name an article or
 * nothing are left for {@link Edges}, which reads them from LinkTargets directly.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.Redirects &lt;LinkTargets output&gt; &lt;output&gt;</pre>
 */
public final class Redirects extends Configured implements Tool {

  public enum Counts { RESOLVED, REDIRECT_ROWS_TWICE }

  public static final class PageMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private final Text value = new Text();
    private String source;

    @Override
    protected void setup(Context context) {
      source = ((FileSplit) context.getInputSplit()).getPath().getName().startsWith(LinkTargets.HOPS)
          ? LinkTargets.HOPS : LinkTargets.TARGETS;
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] f = line.toString().split("\t", -1);  // id kind value
      if (source.equals(LinkTargets.HOPS)) {
        DumpLine.count(context, source, "read");
        key.set(f[0]);
        value.set("H\t" + f[1] + "\t" + f[2]);
      } else if (f[1].equals("REDIRECT")) {
        DumpLine.count(context, source, "to a redirect");
        key.set(f[2]);
        value.set("L\t" + f[0]);
      } else {
        DumpLine.count(context, source, "left for Edges");
        return;
      }
      context.write(key, value);
    }
  }

  public static final class HopReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final Text line = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String kind = null;
      String target = null;
      List<String> targets = new ArrayList<>();
      for (Text t : values) {
        String[] f = t.toString().split("\t", -1);
        if (f[0].equals("H")) {
          if (kind != null) {
            context.getCounter(Counts.REDIRECT_ROWS_TWICE).increment(1);
          }
          kind = f[1];
          target = f[2];
        } else {
          targets.add(f[1]);
        }
      }
      String resolved;
      if (kind == null) {
        resolved = LinkFate.REDIRECT_WITHOUT_ROW.name() + "\t-";
      } else if (kind.equals("ARTICLE")) {
        resolved = "VIA\t" + target;
      } else if (kind.equals("REDIRECT")) {
        resolved = LinkFate.DOUBLE_REDIRECT.name() + "\t-";
      } else {
        resolved = kind + "\t-";  // already a redirect fate from LinkTargets
      }
      for (String lt : targets) {
        line.set(lt + "\t" + resolved);
        context.write(line, NullWritable.get());
        context.getCounter(Counts.RESOLVED).increment(1);
      }
    }
  }

  public static Job configure(Configuration conf, String linkTargets, String output) throws IOException {
    Job job = Job.getInstance(conf, "redirects");
    job.setJarByClass(Redirects.class);
    job.setMapperClass(PageMapper.class);
    job.setReducerClass(HopReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    FileInputFormat.addInputPath(job, new Path(linkTargets, LinkTargets.TARGETS + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(linkTargets, LinkTargets.HOPS + "-r-*"));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    long missing = DumpLine.unaccounted(c);
    if (missing != 0) {
      s.append(missing).append(" input lines without a fate; ");
    }
    long twice = c.findCounter(Counts.REDIRECT_ROWS_TWICE).getValue();
    if (twice != 0) {
      s.append(twice).append(" redirect pages with two redirect rows; ");
    }
    long toRedirect = c.findCounter(DumpLine.GROUP, LinkTargets.TARGETS + " to a redirect").getValue();
    long resolved = c.findCounter(Counts.RESOLVED).getValue();
    if (toRedirect != resolved) {
      s.append(toRedirect).append(" link targets to a redirect, ").append(resolved).append(" resolved; ");
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("usage: Redirects <LinkTargets output> <output>");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1]);
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
    System.exit(ToolRunner.run(new Redirects(), args));
  }
}
