package wikihadoop;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Pass two of three: each candidate article's days, and its spikes.
 *
 * <p>Rereads the window and keeps the rows of the candidates from pass one (in the distributed
 * cache). The shuffle is a secondary sort ({@link ArticleDayKey}): one reduce call per article, its
 * days in order. The reducer builds the article's daily user and automated views, with days it has
 * no row for at zero, and applies {@link SpikeRule} to every day, the wiki's floor coming from the
 * DomainHourTotals output.
 *
 * <p>Outputs: {@code spikes-r-*}, one line per spike: {@code wiki title date user baseline excess
 * automated desktop mobile-web mobile-app} (the last three are that day's user views by access
 * method); and {@code articles-r-*}, one line per candidate: {@code wiki title user automated},
 * its totals over the window.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.ArticleDays -D wh.window.start=2026-10-01 \
 *     -D wh.window.days=31 &lt;domain-hour output&gt; &lt;candidates output&gt; &lt;output&gt; &lt;input&gt;...</pre>
 */
public final class ArticleDays extends Configured implements Tool {

  public enum Counts { CANDIDATE_ROWS, ARTICLES, SPIKES }

  static final String CANDIDATES = "candidates";

  public static final class DayMapper extends Mapper<LongWritable, Text, ArticleDayKey, DayViews> {
    private final PageviewLine row = new PageviewLine();
    private final Set<String> candidates = new HashSet<>();
    private final ArticleDayKey key = new ArticleDayKey();
    private final DayViews value = new DayViews();
    private int day;
    private boolean automated;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      day = Window.of(context.getConfiguration()).index(InputFiles.date(name));
      if (day < 0) {
        throw new IOException(name + " is outside the window");
      }
      automated = InputFiles.agent(name).equals("automated");
      CacheFiles.lines(context, CANDIDATES, line -> {
        String[] f = line.split("\t", -1);
        candidates.add(f[0] + "\t" + f[1]);
      });
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      PageviewLine.Fate fate = row.parse(line.toString());
      context.getCounter(fate).increment(1);
      if (fate != PageviewLine.Fate.PARSED || !candidates.contains(row.wiki + "\t" + row.title)) {
        return;
      }
      context.getCounter(Counts.CANDIDATE_ROWS).increment(1);
      context.write(key.set(row.wiki, row.title, day),
          value.set(day, automated, DayViews.access(row.access), row.daily));
    }
  }

  public static final class SpikeReducer extends Reducer<ArticleDayKey, DayViews, Text, NullWritable> {
    private final WikiTotals wikiTotals = new WikiTotals();
    private final Text line = new Text();
    private MultipleOutputs<Text, NullWritable> out;
    private Window window;

    @Override
    protected void setup(Context context) throws IOException {
      window = Window.of(context.getConfiguration());
      CacheFiles.lines(context, SpikeCandidates.DOMAIN_HOUR, wikiTotals::add);
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(ArticleDayKey key, Iterable<DayViews> values, Context context)
        throws IOException, InterruptedException {
      String wiki = key.wiki.toString();
      String title = key.title.toString();
      long[] user = new long[window.days];
      long[] automated = new long[window.days];
      long[][] byAccess = new long[window.days][DayViews.ACCESS.length];
      int previous = -1;
      for (DayViews v : values) {
        if (v.day < previous) {
          throw new IllegalStateException("days out of order for " + wiki + " " + title);
        }
        previous = v.day;
        if (v.automated) {
          automated[v.day] += v.views;
        } else {
          user[v.day] += v.views;
          byAccess[v.day][v.access] += v.views;
        }
      }
      context.getCounter(Counts.ARTICLES).increment(1);
      long userTotal = 0;
      long automatedTotal = 0;
      for (int d = 0; d < window.days; d++) {
        userTotal += user[d];
        automatedTotal += automated[d];
        String date = window.date(d);
        double baseline = SpikeRule.baseline(user, d);
        if (SpikeRule.isSpike(user[d], baseline, wikiTotals.userViews(wiki, date))) {
          context.getCounter(Counts.SPIKES).increment(1);
          line.set(wiki + "\t" + title + "\t" + date + "\t" + user[d] + "\t" + baseline + "\t"
              + (user[d] - baseline) + "\t" + automated[d] + "\t" + byAccess[d][0] + "\t"
              + byAccess[d][1] + "\t" + byAccess[d][2]);
          out.write("spikes", line, NullWritable.get());
        }
      }
      line.set(wiki + "\t" + title + "\t" + userTotal + "\t" + automatedTotal);
      out.write("articles", line, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  public static Job configure(Configuration conf, String domainHour, String candidates, String output,
      String... inputs) throws IOException {
    Window.of(conf);
    Job job = Job.getInstance(conf, "article days");
    job.setJarByClass(ArticleDays.class);
    job.setMapperClass(DayMapper.class);
    job.setReducerClass(SpikeReducer.class);
    job.setMapOutputKeyClass(ArticleDayKey.class);
    job.setMapOutputValueClass(DayViews.class);
    job.setPartitionerClass(ArticleDayKey.ByArticle.class);
    job.setGroupingComparatorClass(ArticleDayKey.GroupByArticle.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(conf.getInt("wh.reducers", 3));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "spikes", TextOutputFormat.class, Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, "articles", TextOutputFormat.class, Text.class, NullWritable.class);
    CacheFiles.add(job, SpikeCandidates.DOMAIN_HOUR, domainHour);
    CacheFiles.addNamed(job, CANDIDATES, candidates, "candidates-r-");
    for (String input : inputs) {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 4) {
      System.err.println("usage: ArticleDays <domain-hour output> <candidates output> <output> <input>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], args[2], java.util.Arrays.copyOfRange(args, 3, args.length));
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    long missing = SpikeCandidates.unaccounted(job.getCounters());
    if (missing != 0) {
      System.err.println("conservation failed: " + missing + " input lines have no fate");
      return 3;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ArticleDays(), args));
  }
}
