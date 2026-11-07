package wikihadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Small side inputs shipped to every task through the distributed cache.
 *
 * <p>{@link #add} registers every {@code part-*} file of a job output directory under a name, as
 * {@code name.0}, {@code name.1}, and so on. On a cluster YARN copies each one to the task's node
 * and links it into the task's working directory under that name, so {@link #lines} reads a local
 * file. In the local job runner, where no link may exist, it reads the file where it lies.
 */
public final class CacheFiles {

  private CacheFiles() {}

  public static void add(Job job, String name, String outputDir) throws IOException {
    Path dir = new Path(outputDir);
    FileSystem fs = dir.getFileSystem(job.getConfiguration());
    List<FileStatus> parts = new ArrayList<>();
    for (FileStatus s : fs.listStatus(dir)) {
      if (s.getPath().getName().startsWith("part-") || s.getPath().getName().contains("-r-")) {
        parts.add(s);
      }
    }
    if (parts.isEmpty()) {
      throw new IOException("no part files in " + outputDir);
    }
    parts.sort((a, b) -> a.getPath().getName().compareTo(b.getPath().getName()));
    for (int i = 0; i < parts.size(); i++) {
      URI uri = fs.makeQualified(parts.get(i).getPath()).toUri();
      try {
        job.addCacheFile(new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, name + "." + i));
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
  }

  /** Feeds every line of every cache file registered under {@code name} to {@code sink}. */
  public static void lines(JobContext context, String name, Consumer<String> sink) throws IOException {
    URI[] files = context.getCacheFiles();
    int found = 0;
    for (URI uri : files == null ? new URI[0] : files) {
      String fragment = uri.getFragment();
      if (fragment == null || !fragment.startsWith(name + ".")) {
        continue;
      }
      found++;
      File local = new File(fragment);
      if (local.exists()) {
        try (BufferedReader r = Files.newBufferedReader(local.toPath(), StandardCharsets.UTF_8)) {
          r.lines().forEach(sink);
        }
      } else {
        Configuration conf = context.getConfiguration();
        Path p = new Path(uri.getScheme(), uri.getAuthority(), uri.getPath());
        try (BufferedReader r = new BufferedReader(
            new InputStreamReader(p.getFileSystem(conf).open(p), StandardCharsets.UTF_8))) {
          r.lines().forEach(sink);
        }
      }
    }
    if (found == 0) {
      throw new IOException("no cache files named " + name);
    }
  }
}
