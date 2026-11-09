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
 * Linked against read, job three of four: each viewed title's views go to an article or a fate.
 *
 * <p>A reduce-side join keyed by title over {@link MonthViews}' output for the wiki in {@code
 * wh.views.wiki} and {@link TitleTargets}' output for that wiki's graph. Every viewed title ends in
 * one {@link Fate}: the title of an article, a redirect to one, a redirect that reaches no article,
 * a title in one of the wiki's other namespaces (known for the wikis in {@link Wikis}), a title
 * that names no page of namespace 0, or {@code -}, which the pageview files give a view whose title
 * is not known (English Wikipedia has a redirect titled {@code -}, which this keeps the views from). The group {@link #USER} counts user views per fate, {@link
 * #AUTOMATED} automated views, and {@link #TITLES} titles, so the user fates sum to the wiki's user
 * views.
 *
 * <p>Output {@code articleviews-r-*}: {@code article_id user automated via}, one line per title that
 * reached an article, {@code via} 0 for its own title and 1 for a redirect's.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.ViewTargets -D wh.views.wiki=en.wikipedia \
 *     &lt;MonthViews output&gt; &lt;TitleTargets output&gt; &lt;output&gt;</pre>
 */
public final class ViewTargets extends Configured implements Tool {

  public enum Fate { ARTICLE, REDIRECT, REDIRECT_ELSEWHERE, OTHER_NAMESPACE, NO_ARTICLE, UNKNOWN_TITLE }

  public enum Counts { OTHER_WIKI_TITLES, VIEWS_TWICE, TARGET_TWICE }

  static final String USER = "user views";
  static final String AUTOMATED = "automated views";
  static final String TITLES = "viewed titles";
  static final String OUTPUT = "articleviews";

  static String wiki(Configuration conf) throws IOException {
    String wiki = conf.getTrimmed("wh.views.wiki");
    if (wiki == null || wiki.isEmpty()) {
      throw new IOException("set wh.views.wiki to the wiki whose views go to the graph");
    }
    return wiki;
  }

  public static final class TitleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text key = new Text();
    private final Text value = new Text();
    private String wiki;
    private boolean views;

    @Override
    protected void setup(Context context) throws IOException {
      wiki = wiki(context.getConfiguration());
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (name.startsWith(MonthViews.OUTPUT + "-")) {
        views = true;
      } else if (!name.startsWith(TitleTargets.OUTPUT + "-")) {
        throw new IOException("ViewTargets reads MonthViews and TitleTargets output, not " + name);
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      String[] f = line.toString().split("\t", -1);
      if (views) {  // wiki title user automated
        if (!f[0].equals(wiki)) {
          context.getCounter(Counts.OTHER_WIKI_TITLES).increment(1);
          return;
        }
        key.set(f[1]);
        value.set("V\t" + f[2] + "\t" + f[3]);
      } else {  // title article_id kind
        key.set(f[0]);
        value.set("T\t" + f[1] + "\t" + f[2]);
      }
      context.write(key, value);
    }
  }

  public static final class FateReducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();
    private final Wikis wikis = Wikis.load();
    private String wiki;

    @Override
    protected void setup(Context context) throws IOException {
      out = new MultipleOutputs<>(context);
      wiki = wiki(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String[] views = null;
      String[] target = null;
      for (Text t : values) {
        String[] f = t.toString().split("\t", -1);
        if (f[0].equals("V")) {
          if (views != null) {
            context.getCounter(Counts.VIEWS_TWICE).increment(1);
          }
          views = f;
        } else {
          if (target != null) {
            context.getCounter(Counts.TARGET_TWICE).increment(1);
          }
          target = f;
        }
      }
      if (views == null) {
        return;  // a title nobody viewed
      }
      Fate fate;
      if (key.toString().equals("-")) {
        fate = Fate.UNKNOWN_TITLE;
      } else if (target == null) {
        fate = wikis.otherNamespace(wiki, key.toString()) ? Fate.OTHER_NAMESPACE : Fate.NO_ARTICLE;
      } else if (target[2].equals("article")) {
        fate = Fate.ARTICLE;
      } else if (target[2].equals("redirect")) {
        fate = Fate.REDIRECT;
      } else {
        fate = Fate.REDIRECT_ELSEWHERE;
      }
      long user = Long.parseLong(views[1]);
      long automated = Long.parseLong(views[2]);
      context.getCounter(USER, fate.name()).increment(user);
      context.getCounter(AUTOMATED, fate.name()).increment(automated);
      context.getCounter(TITLES, fate.name()).increment(1);
      if (fate == Fate.ARTICLE || fate == Fate.REDIRECT) {
        line.set(target[1] + "\t" + user + "\t" + automated + "\t" + (fate == Fate.ARTICLE ? 0 : 1));
        out.write(OUTPUT, line, NullWritable.get(), OUTPUT);
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String monthViews, String titleTargets, String output)
      throws IOException {
    wiki(conf);
    Job job = Job.getInstance(conf, "view targets");
    job.setJarByClass(ViewTargets.class);
    job.setMapperClass(TitleMapper.class);
    job.setReducerClass(FateReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(LinkTargets.reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, OUTPUT, TextOutputFormat.class, Text.class, NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(monthViews, MonthViews.OUTPUT + "-r-*"));
    FileInputFormat.addInputPath(job, new Path(titleTargets, TitleTargets.OUTPUT + "-r-*"));
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  /** The sum of a group's counters over the fates. */
  static long sum(Counters c, String group) {
    long n = 0;
    for (Fate f : Fate.values()) {
      n += c.findCounter(group, f.name()).getValue();
    }
    return n;
  }

  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    long views = c.findCounter(Counts.VIEWS_TWICE).getValue();
    if (views != 0) {
      s.append(views).append(" titles with two lines of views; ");
    }
    long targets = c.findCounter(Counts.TARGET_TWICE).getValue();
    if (targets != 0) {
      s.append(targets).append(" titles with two targets; ");
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("usage: ViewTargets -D wh.views.wiki=<wiki> <MonthViews output> <TitleTargets output> <output>");
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
    System.exit(ToolRunner.run(new ViewTargets(), args));
  }
}
