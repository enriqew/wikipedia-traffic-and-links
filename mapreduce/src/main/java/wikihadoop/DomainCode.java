package wikihadoop;

import java.util.Map;

/**
 * The domain code under which the hourly pageview dumps count a wiki's views.
 *
 * <p>Domain codes are {@code <lang>[.m][.<project suffix>]}. Found in phase 0 (docs/PHASE0.md):
 * mobile-app views are counted under the mobile code with mobile-web, the {@code X.wikimedia}
 * projects take {@code X.m} and {@code X.m.m}, and Wikifunctions sits under the empty code
 * {@code ""}. With this mapping the user totals of pageview_complete equal the hourly dumps for
 * every domain and hour of the day checked.
 */
public final class DomainCode {

  private static final Map<String, String> SUFFIX = Map.of(
      "wikipedia", "", "wikibooks", "b", "wiktionary", "d", "wikinews", "n", "wikiquote", "q",
      "wikisource", "s", "wikiversity", "v", "wikivoyage", "voy");

  /** Projects without a language prefix: {desktop, mobile}. */
  private static final Map<String, String[]> GLOBAL = Map.of(
      "wikidata", new String[] {"www.wd", "m.wd"},
      "mediawiki", new String[] {"www.w", "m.w"},
      "wikisource", new String[] {"www.s", "m.s"},
      "wikifunctions", new String[] {"\"\"", "\"\""});

  private DomainCode() {}

  /** The domain code for a wiki and access method, or null when the wiki has no known code. */
  public static String of(String wiki, String access) {
    boolean mobile = "mobile-web".equals(access) || "mobile-app".equals(access);
    String[] global = GLOBAL.get(wiki);
    if (global != null) {
      return global[mobile ? 1 : 0];
    }
    int dot = wiki.indexOf('.');
    if (dot < 0) {
      return null;
    }
    String lang = wiki.substring(0, dot);
    String project = wiki.substring(dot + 1);
    if ("wikimedia".equals(project)) {
      return lang + (mobile ? ".m.m" : ".m");
    }
    String suffix = SUFFIX.get(project);
    if (suffix == null) {
      return null;
    }
    return lang + (mobile ? ".m" : "") + (suffix.isEmpty() ? "" : "." + suffix);
  }
}
