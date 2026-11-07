package wikihadoop;

import java.util.ArrayList;
import java.util.List;

/**
 * One line of a MediaWiki SQL dump table (page, pagelinks, linktarget, redirect).
 *
 * <p>The dumps write each statement as a line {@code INSERT INTO `table` VALUES} followed by one
 * tuple per line, {@code (v1,v2,...),}, the statement's last tuple ending in {@code ;}
 * (docs/PHASE0.md). So a tuple line is one row, and every line gets a fate: a row, a line that
 * carries no data (the schema, comments, the INSERT line), or malformed. A line that starts as a
 * tuple or as an INSERT and does not parse is malformed, never skipped, so no row can go missing
 * without a counter saying so.
 *
 * <p>Values come back as strings: numbers as written, quoted strings with mysqldump's backslash
 * escapes undone, and NULL as null. An escape mysqldump does not write is malformed.
 */
public final class SqlRow {

  public enum Fate { ROW, NOT_DATA, MALFORMED }

  /** The row's values, after a parse that returned {@link Fate#ROW}. */
  public final List<String> values = new ArrayList<>();

  private final StringBuilder buf = new StringBuilder();

  public Fate parse(String line) {
    values.clear();
    if (line.startsWith("(")) {
      return tuple(line) ? Fate.ROW : Fate.MALFORMED;
    }
    if (line.startsWith("INSERT")) {
      return line.matches("INSERT INTO `[a-z_]+` VALUES") ? Fate.NOT_DATA : Fate.MALFORMED;
    }
    return Fate.NOT_DATA;
  }

  private boolean tuple(String line) {
    int i = 1;
    int n = line.length();
    while (true) {
      if (i >= n) {
        return false;
      }
      if (line.charAt(i) == '\'') {
        i = string(line, i + 1);
        if (i < 0) {
          return false;
        }
        values.add(buf.toString());
      } else {
        int start = i;
        while (i < n && line.charAt(i) != ',' && line.charAt(i) != ')') {
          i++;
        }
        String token = line.substring(start, i);
        if (token.equals("NULL")) {
          values.add(null);
        } else if (token.matches("-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?")) {
          values.add(token);
        } else {
          return false;
        }
      }
      if (i >= n) {
        return false;
      }
      char c = line.charAt(i);
      if (c == ')') {
        String rest = line.substring(i + 1);
        return rest.equals(",") || rest.equals(";");
      }
      if (c != ',') {
        return false;
      }
      i++;
    }
  }

  /** Reads a quoted string's body from {@code i} into {@link #buf}; the index after its closing quote, or -1. */
  private int string(String line, int i) {
    buf.setLength(0);
    int n = line.length();
    while (i < n) {
      char c = line.charAt(i);
      if (c == '\'') {
        return i + 1;
      }
      if (c == '\\') {
        if (i + 1 >= n) {
          return -1;
        }
        char e = line.charAt(i + 1);
        switch (e) {
          case '0': buf.append('\0'); break;
          case 'n': buf.append('\n'); break;
          case 'r': buf.append('\r'); break;
          case 'Z': buf.append('\u001a'); break;
          case '\\': case '\'': case '"': buf.append(e); break;
          default: return -1;
        }
        i += 2;
      } else {
        buf.append(c);
        i++;
      }
    }
    return -1;
  }
}
