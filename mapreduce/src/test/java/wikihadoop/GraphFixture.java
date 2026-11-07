package wikihadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * A tiny wiki whose fifteen links meet each {@link LinkFate} once, except EDGE_DIRECT twice, written
 * as the four dump tables the link-graph jobs read.
 *
 * <pre>
 * pages: 1 Alpha, 2 Beta, 3 Gamma, 11 Lonely (articles); 4 B -&gt; Beta, 6 G3 -&gt; Gamma,
 *        5 G2 -&gt; G3 (a double redirect), 7 Out -&gt; Category:X, 8 Iw -&gt; another wiki,
 *        9 Lost -&gt; Nowhere (no such page) (redirects); 10 Talk:Alpha (namespace 1)
 * </pre>
 * Expected graph: 1 -&gt; 2, 3; 2 -&gt; 3; 3 and 11 link nowhere.
 */
final class GraphFixture {

  static final String WIKI = "testwiki-20261101-";

  private GraphFixture() {}

  static Path page(Path dir) throws IOException {
    return dump(dir, "page", new String[] {
        page(1, 0, "Alpha", 0), page(2, 0, "Beta", 0), page(3, 0, "Gamma", 0), page(4, 0, "B", 1),
        page(5, 0, "G2", 1), page(6, 0, "G3", 1), page(7, 0, "Out", 1), page(8, 0, "Iw", 1),
        page(9, 0, "Lost", 1), page(10, 1, "Alpha", 0), page(11, 0, "Lonely", 0)});
  }

  static Path linktarget(Path dir) throws IOException {
    return dump(dir, "linktarget", new String[] {
        "(100,0,'Beta')", "(101,0,'B')", "(102,0,'Gamma')", "(103,0,'Nowhere')", "(104,14,'X')",
        "(105,0,'G2')", "(106,0,'Out')", "(107,0,'Iw')", "(108,0,'Lost')", "(109,0,'Alpha')",
        "(110,0,'G3')"});
  }

  static Path redirect(Path dir) throws IOException {
    return dump(dir, "redirect", new String[] {
        "(4,0,'Beta','','')", "(5,0,'G3','','')", "(6,0,'Gamma','','')", "(7,14,'X','','')",
        "(8,0,'Foo','en','')", "(9,0,'Nowhere','',NULL)"});
  }

  static Path pagelinks(Path dir) throws IOException {
    return dump(dir, "pagelinks", new String[] {
        "(1,0,100)",    // Alpha -> Beta: EDGE_DIRECT
        "(1,0,101)",    // Alpha -> B -> Beta: DUPLICATE (the edge stays direct)
        "(1,0,102)",    // Alpha -> Gamma: EDGE_DIRECT
        "(1,0,103)",    // RED_LINK
        "(1,0,104)",    // TARGET_OTHER_NAMESPACE
        "(1,0,105)",    // DOUBLE_REDIRECT
        "(1,0,106)",    // REDIRECT_OUT_OF_NS0
        "(1,0,107)",    // REDIRECT_INTERWIKI
        "(1,0,108)",    // REDIRECT_TO_MISSING
        "(1,0,109)",    // SELF
        "(1,0,999)",    // TARGET_ID_MISSING
        "(2,0,110)",    // Beta -> G3 -> Gamma: EDGE_VIA
        "(4,0,100)",    // FROM_REDIRECT
        "(10,1,109)",   // FROM_OTHER_NAMESPACE
        "(12,0,100)"}); // FROM_MISSING
  }

  /** The four dumps in one directory. */
  static Path all(Path dir) throws IOException {
    page(dir);
    linktarget(dir);
    redirect(dir);
    pagelinks(dir);
    return dir;
  }

  private static String page(int id, int ns, String title, int redirect) {
    return "(" + id + "," + ns + ",'" + title + "'," + redirect
        + ",0,0.5,'20261101000000','20261101000000',1,10,'wikitext',NULL)";
  }

  /** A dump file as mysqldump writes it: a header, then the tuples one per line in one INSERT. */
  private static Path dump(Path dir, String table, String[] tuples) throws IOException {
    Files.createDirectories(dir);
    List<String> lines = new ArrayList<>(List.of(
        "-- MySQL dump 10.19", "DROP TABLE IF EXISTS `" + table + "`;", "CREATE TABLE `" + table + "` (",
        "  `x` int(8) unsigned NOT NULL,", ") ENGINE=InnoDB;", "INSERT INTO `" + table + "` VALUES"));
    for (int i = 0; i < tuples.length; i++) {
      lines.add(tuples[i] + (i == tuples.length - 1 ? ";" : ","));
    }
    lines.add("UNLOCK TABLES;");
    Path file = dir.resolve(WIKI + table + ".sql.gz");
    try (OutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
      out.write((String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
    }
    return file;
  }

  /** Lines in a dump file of {@code n} tuples: six header lines, the tuples, one trailer. */
  static long lines(int tuples) {
    return 6 + tuples + 1;
  }
}
