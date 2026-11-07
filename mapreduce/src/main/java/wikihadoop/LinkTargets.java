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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Link graph, job one of four: what each title names (plan section 13).
 *
 * <p>A reduce-side join keyed by namespace and title over three dump tables: {@code page} (namespace
 * 0 only), {@code linktarget}, and {@code redirect} keyed by the title it points at, so each reduce
 * call holds a title's page, the link target that names it and the redirects that point at it.
 *
 * <p>Two outputs, each line {@code id kind value}:
 * <ul>
 *   <li>{@code targets}, per link target id: {@code ARTICLE} or {@code REDIRECT} with the page id,
 *       or a final {@link LinkFate} ({@code RED_LINK}, {@code TARGET_OTHER_NAMESPACE}) and {@code -};
 *   <li>{@code hops}, per redirect page id: {@code ARTICLE} or {@code REDIRECT} with the target page
 *       id, or {@code REDIRECT_TO_MISSING}, {@code REDIRECT_OUT_OF_NS0}, {@code REDIRECT_INTERWIKI}.
 * </ul>
 * A title with two pages is counted in {@link Counts#DUPLICATE_TITLES}, which must be zero.
 *
 * <pre>hadoop jar wiki-hadoop.jar wikihadoop.LinkTargets &lt;output&gt; &lt;page, linktarget and redirect dumps&gt;...</pre>
 */
public final class LinkTargets extends Configured implements Tool {

  public enum Counts { TARGETS, HOPS, DUPLICATE_TITLES }

  static final String TARGETS = "targets";
  static final String HOPS = "hops";
  /** The key of every interwiki redirect: they point outside this wiki, so at no title here. */
  static final String INTERWIKI = "iw";

  public static final class TitleMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final DumpLine dump = new DumpLine();
    private final Text key = new Text();
    private final Text value = new Text();
    private String table;

    @Override
    protected void setup(Context context) throws IOException {
      table = DumpLine.table(((FileSplit) context.getInputSplit()).getPath().getName());
      if (table.equals("pagelinks")) {
        throw new IOException("LinkTargets reads page, linktarget and redirect, not pagelinks");
      }
    }

    @Override
    protected void map(LongWritable offset, Text line, Context context)
        throws IOException, InterruptedException {
      List<String> v = dump.read(line, table, context);
      if (v == null) {
        return;
      }
      switch (table) {
        case "page":  // page_id, page_namespace, page_title, page_is_redirect, ...
          if (!v.get(1).equals("0")) {
            return;
          }
          key.set("0\t" + v.get(2));
          value.set("P\t" + v.get(0) + "\t" + v.get(3));
          break;
        case "linktarget":  // lt_id, lt_namespace, lt_title
          key.set(v.get(1) + "\t" + v.get(2));
          value.set("L\t" + v.get(0));
          break;
        default:  // redirect: rd_from, rd_namespace, rd_title, rd_interwiki, rd_fragment
          String interwiki = v.get(3);
          key.set(interwiki != null && !interwiki.isEmpty() ? INTERWIKI : v.get(1) + "\t" + v.get(2));
          value.set("R\t" + v.get(0));
      }
      context.write(key, value);
    }
  }

  public static final class TitleReducer extends Reducer<Text, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> out;
    private final Text line = new Text();

    @Override
    protected void setup(Context context) {
      out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String k = key.toString();
      String page = null;
      String pageKind = null;
      List<String> targets = new ArrayList<>();
      List<String> redirects = new ArrayList<>();
      for (Text t : values) {
        String[] f = t.toString().split("\t", -1);
        switch (f[0]) {
          case "P":
            if (page != null) {
              context.getCounter(Counts.DUPLICATE_TITLES).increment(1);
            }
            page = f[1];
            pageKind = f[2].equals("1") ? "REDIRECT" : "ARTICLE";
            break;
          case "L":
            targets.add(f[1]);
            break;
          default:
            redirects.add(f[1]);
        }
      }
      String targetKind;
      String hopKind;
      String id;
      if (k.equals(INTERWIKI)) {
        targetKind = LinkFate.TARGET_OTHER_NAMESPACE.name();  // no link target has this key
        hopKind = LinkFate.REDIRECT_INTERWIKI.name();
        id = "-";
      } else if (!k.startsWith("0\t")) {
        targetKind = LinkFate.TARGET_OTHER_NAMESPACE.name();
        hopKind = LinkFate.REDIRECT_OUT_OF_NS0.name();
        id = "-";
      } else if (page == null) {
        targetKind = LinkFate.RED_LINK.name();
        hopKind = LinkFate.REDIRECT_TO_MISSING.name();
        id = "-";
      } else {
        targetKind = pageKind;
        hopKind = pageKind;
        id = page;
      }
      for (String lt : targets) {
        write(TARGETS, lt + "\t" + targetKind + "\t" + id);
        context.getCounter(Counts.TARGETS).increment(1);
      }
      for (String from : redirects) {
        write(HOPS, from + "\t" + hopKind + "\t" + id);
        context.getCounter(Counts.HOPS).increment(1);
      }
    }

    private void write(String name, String s) throws IOException, InterruptedException {
      line.set(s);
      out.write(name, line, NullWritable.get(), name);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      out.close();
    }
  }

  static int reducers(Configuration conf) {
    return conf.getInt("wh.graph.reducers", 3);
  }

  public static Job configure(Configuration conf, String output, String... dumps) throws IOException {
    Job job = Job.getInstance(conf, "link targets");
    job.setJarByClass(LinkTargets.class);
    job.setMapperClass(TitleMapper.class);
    job.setReducerClass(TitleReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setNumReduceTasks(reducers(conf));
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, TARGETS, TextOutputFormat.class, Text.class, NullWritable.class);
    MultipleOutputs.addNamedOutput(job, HOPS, TextOutputFormat.class, Text.class, NullWritable.class);
    for (String dump : dumps) {
      FileInputFormat.addInputPath(job, new Path(dump));
    }
    FileOutputFormat.setOutputPath(job, new Path(output));
    return job;
  }

  /** What the run requires: every line accounted for, no malformed row, no title with two pages. */
  static String failures(Counters c) {
    StringBuilder s = new StringBuilder();
    long missing = DumpLine.unaccounted(c);
    if (missing != 0) {
      s.append(missing).append(" input lines without a fate; ");
    }
    for (String table : new String[] {"page", "linktarget", "redirect"}) {
      long bad = DumpLine.malformed(c, table);
      if (bad != 0) {
        s.append(bad).append(" malformed ").append(table).append(" lines; ");
      }
    }
    long dup = c.findCounter(Counts.DUPLICATE_TITLES).getValue();
    if (dup != 0) {
      s.append(dup).append(" titles with two pages; ");
    }
    return s.toString();
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("usage: LinkTargets <output> <page, linktarget and redirect dumps>...");
      return 2;
    }
    Job job = configure(getConf(), args[0], java.util.Arrays.copyOfRange(args, 1, args.length));
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
    System.exit(ToolRunner.run(new LinkTargets(), args));
  }
}
