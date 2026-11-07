package wikihadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Pass three of three: each spike's 24 hours, and whether it looks automated.
 *
 * <p>A map-side join. The spikes from pass two go to every mapper through the distributed cache,
 * and the mappers reread the window's {@code user} files keeping only the rows of a listed spike,
 * each with its 24 hourly counts. The reducer sums an article-day's rows and applies
 * {@link LooksAutomated}, with the wiki's hourly views from the DomainHourTotals output and its
 * access-method shares from pass one's totals, both also in the cache.
 *
 * <p>Output: each spike's line from pass two followed by {@code flatness dominant_access
 * dominant_share wiki_share_of_it flags peak_share burst hourly}. {@code flags} is
 * {@link LooksAutomated}'s verdict; {@code burst} is {@code burst} or {@code -} by {@link Burst},
 * which is not part of that verdict; {@code hourly} is 24 comma-separated counts. A spike
 * whose hours do not sum to the views pass two gave it is counted in {@link Counts#HOURS_MISMATCH},
 * which the run requires to be zero.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.SpikeHours -D wh.window.start=2026-10-01 -D wh.window.days=31 \
 *     &lt;domain-hour output&gt; &lt;candidates output&gt; &lt;article-days output&gt; &lt;output&gt; &lt;user input&gt;...</pre>
 */
public final class SpikeHours extends Configured implements Tool {

  public enum Counts { SPIKE_ROWS, SPIKES, FLAGGED, BURSTS, HOURS_MISMATCH }

  static final String SPIKES = "spikes";
  static final String TOTALS = "totals";

  public static final class HourMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final PageviewLine row = new PageviewLine();
    private final Set<String> spikes = new HashSet<>();
    private final Text key = new Text();
    private final Text value = new Text();
    private String date;

    @Override
    protected void setup(Context context) throws IOException {
      String name = ((FileSplit) context.getInputSplit()).getPath().getName();
      if (!InputFiles.agent(name).equals("user")) {
        throw new IOException("SpikeHours reads user files only, not " + name);
      }
      date = InputFiles.date(name);
      CacheFiles.lines(context, SPIKES, line -> {
        String[] f = line.split("\t", -1);
        if (f[2].equals(date)) {
          spikes.add(f[0] + "\t" + f[1]);
        }
      });
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      PageviewLine.Fate fate = row.parse(line.toString());
      context.getCounter(fate).increment(1);
      if (fate != PageviewLine.Fate.PARSED || !spikes.contains(row.wiki + "\t" + row.title)) {
        return;
      }
      context.getCounter(Counts.SPIKE_ROWS).increment(1);
      StringBuilder hours = new StringBuilder(row.access);
      for (long h : row.hours) {
        hours.append(',').append(h);
      }
      key.set(row.wiki + "\t" + row.title + "\t" + date);
      value.set(hours.toString());
      context.write(key, value);
    }
  }

  public static final class FlagReducer extends Reducer<Text, Text, Text, NullWritable> {
    private final WikiTotals wikiTotals = new WikiTotals();
    private final Map<String, Long> accessTotals = new HashMap<>();
    private final Map<String, String> spikeLines = new HashMap<>();
    private final Text line = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      CacheFiles.lines(context, SpikeCandidates.DOMAIN_HOUR, wikiTotals::add);
      CacheFiles.lines(context, TOTALS, l -> {
        String[] f = l.split("\t", -1);  // date wiki agent access views
        if (f[2].equals("user")) {
          accessTotals.merge(f[0] + "\t" + f[1] + "\t" + f[3], Long.parseLong(f[4]), Long::sum);
        }
      });
      CacheFiles.lines(context, SPIKES, l -> {
        String[] f = l.split("\t", -1);
        spikeLines.put(f[0] + "\t" + f[1] + "\t" + f[2], l);
      });
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      long[] hourly = new long[24];
      Map<String, Long> byAccess = new HashMap<>();
      for (Text v : values) {
        String[] f = v.toString().split(",", -1);
        long sum = 0;
        for (int h = 0; h < 24; h++) {
          long c = Long.parseLong(f[h + 1]);
          hourly[h] += c;
          sum += c;
        }
        byAccess.merge(f[0], sum, Long::sum);
      }
      String[] k = key.toString().split("\t", -1);
      String wiki = k[0];
      String date = k[2];
      String spike = spikeLines.get(key.toString());
      long total = 0;
      for (long h : hourly) {
        total += h;
      }
      if (spike == null || total != Long.parseLong(spike.split("\t", -1)[3])) {
        context.getCounter(Counts.HOURS_MISMATCH).increment(1);
        return;
      }
      String dominant = null;
      long dominantViews = -1;
      for (String access : DayViews.ACCESS) {
        long v = byAccess.getOrDefault(access, 0L);
        if (v > dominantViews) {
          dominant = access;
          dominantViews = v;
        }
      }
      long wikiAll = 0;
      for (String access : DayViews.ACCESS) {
        wikiAll += accessTotals.getOrDefault(date + "\t" + wiki + "\t" + access, 0L);
      }
      long wikiDominant = accessTotals.getOrDefault(date + "\t" + wiki + "\t" + dominant, 0L);
      double share = (double) dominantViews / total;
      double wikiShare = wikiAll == 0 ? 0 : (double) wikiDominant / wikiAll;
      double flatness = LooksAutomated.flatness(hourly, wikiTotals.hourly(wiki, date));
      String flags = LooksAutomated.flags(flatness, share, wikiShare);
      double peakShare = Burst.peakShare(hourly);
      boolean burst = Burst.is(peakShare);
      context.getCounter(Counts.SPIKES).increment(1);
      if (!flags.equals("-")) {
        context.getCounter(Counts.FLAGGED).increment(1);
      }
      if (burst) {
        context.getCounter(Counts.BURSTS).increment(1);
      }
      StringBuilder csv = new StringBuilder();
      for (int h = 0; h < 24; h++) {
        csv.append(h == 0 ? "" : ",").append(hourly[h]);
      }
      line.set(spike + "\t" + String.format(java.util.Locale.ROOT,
          "%.4f\t%s\t%.4f\t%.4f\t%s\t%.4f\t%s\t%s",
          flatness, dominant, share, wikiShare, flags, peakShare, burst ? "burst" : "-", csv));
      context.write(line, NullWritable.get());
    }
  }

  public static Job configure(Configuration conf, String domainHour, String candidatesOutput,
      String articleDaysOutput, String output, String... userInputs) throws IOException {
    Window.of(conf);
    Job job = Job.getInstance(conf, "spike hours");
    job.setJarByClass(SpikeHours.class);
    job.setMapperClass(HourMapper.class);
    job.setReducerClass(FlagReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(1);
    CacheFiles.add(job, SpikeCandidates.DOMAIN_HOUR, domainHour);
    CacheFiles.addNamed(job, TOTALS, candidatesOutput, "totals-r-");
    CacheFiles.addNamed(job, SPIKES, articleDaysOutput, "spikes-r-");
    for (String input : userInputs) {
      FileInputFormat.addInputPath(job, new Path(input));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println("usage: SpikeHours <domain-hour> <candidates output> <article-days output> <output> <user input>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], args[1], args[2], args[3], java.util.Arrays.copyOfRange(args, 4, args.length));
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    long mismatched = job.getCounters().findCounter(Counts.HOURS_MISMATCH).getValue();
    long missing = SpikeCandidates.unaccounted(job.getCounters());
    if (mismatched != 0 || missing != 0) {
      System.err.println("checks failed: " + mismatched + " spikes whose hours do not add up, "
          + missing + " input lines without a fate");
      return 3;
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new SpikeHours(), args));
  }
}
