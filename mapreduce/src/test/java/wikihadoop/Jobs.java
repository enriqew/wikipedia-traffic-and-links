package wikihadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;

/** Helpers for running jobs in Hadoop's local job runner. */
final class Jobs {

  private Jobs() {}

  static Configuration localConf() {
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    return conf;
  }

  /** Writes {@code text} as a bzip2 file, like the real input. */
  static void bzip2(Configuration conf, Path file, String text) throws IOException {
    BZip2Codec codec = new BZip2Codec();
    codec.setConf(conf);
    try (OutputStream out = codec.createOutputStream(Files.newOutputStream(file))) {
      out.write(text.getBytes(StandardCharsets.UTF_8));
    }
  }
}
