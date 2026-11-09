package wikihadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The wikis whose spikes are ranked, with what makes a title rankable there.
 *
 * <p>Read from {@code wikis.tsv} in the jar (written by scripts/fetch_siteinfo.py): the ten largest
 * wikis by user views, each with its main page title and every name and alias of its non-article
 * namespaces. A title is ranked when it is not the main page, not {@code -}, and not prefixed by one
 * of those names before its first colon (MediaWiki namespace names are case-insensitive). Titles
 * that are not ranked still count in every total.
 */
public final class Wikis {

  private static final class Wiki {
    String mainPage;
    Set<String> prefixes;
  }

  private final Map<String, Wiki> wikis = new HashMap<>();

  public static Wikis load() {
    try (InputStream in = Wikis.class.getResourceAsStream("/wikis.tsv")) {
      if (in == null) {
        throw new IllegalStateException("wikis.tsv is not on the classpath");
      }
      return parse(new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Wikis parse(BufferedReader reader) throws IOException {
    Wikis w = new Wikis();
    for (String line; (line = reader.readLine()) != null; ) {
      if (line.isEmpty()) {
        continue;
      }
      String[] f = line.split("\t", -1);
      Wiki wiki = new Wiki();
      wiki.mainPage = f[1];
      wiki.prefixes = new HashSet<>(java.util.Arrays.asList(f[2].split("\\|")));
      w.wikis.put(f[0], wiki);
    }
    return w;
  }

  public boolean contains(String wiki) {
    return wikis.containsKey(wiki);
  }

  public Set<String> names() {
    return wikis.keySet();
  }

  /** Whether the title starts with the name of one of this wiki's non-article namespaces. */
  public boolean otherNamespace(String wiki, String title) {
    Wiki w = wikis.get(wiki);
    int colon = title.indexOf(':');
    return w != null && colon > 0 && w.prefixes.contains(title.substring(0, colon).toLowerCase(Locale.ROOT));
  }

  /** Whether the title takes part in this wiki's spike rankings. */
  public boolean ranked(String wiki, String title) {
    Wiki w = wikis.get(wiki);
    if (w == null || title.equals("-") || title.equals(w.mainPage)) {
      return false;
    }
    int colon = title.indexOf(':');
    return colon <= 0 || !w.prefixes.contains(title.substring(0, colon).toLowerCase(Locale.ROOT));
  }
}
