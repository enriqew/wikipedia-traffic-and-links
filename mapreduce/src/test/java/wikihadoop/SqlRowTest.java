package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class SqlRowTest {

  private final SqlRow row = new SqlRow();

  @Test
  void aPagelinksTuple() {
    assertEquals(SqlRow.Fate.ROW, row.parse("(128281,0,4),"));
    assertEquals(Arrays.asList("128281", "0", "4"), row.values);
  }

  @Test
  void theStatementsLastTupleEndsInASemicolon() {
    assertEquals(SqlRow.Fate.ROW, row.parse("(866814,0,30);"));
    assertEquals(Arrays.asList("866814", "0", "30"), row.values);
  }

  @Test
  void aPageTupleWithADecimalAndANull() {
    assertEquals(SqlRow.Fate.ROW, row.parse(
        "(1,0,'April',0,0,0.778582929065,'20260928162419','20260928174648',10861257,22079,'wikitext',NULL),"));
    assertEquals(Arrays.asList("1", "0", "April", "0", "0", "0.778582929065", "20260928162419",
        "20260928174648", "10861257", "22079", "wikitext", null), row.values);
  }

  @Test
  void quotedStringsWithEscapesCommasAndParentheses() {
    assertEquals(SqlRow.Fate.ROW, row.parse("(7,0,'Rock_\\'n\\'_roll_(music),_a\\\\b','',''),"));
    assertEquals(Arrays.asList("7", "0", "Rock_'n'_roll_(music),_a\\b", "", ""), row.values);
  }

  @Test
  void utf8TitlesComeThroughAsTheyAre() {
    assertEquals(SqlRow.Fate.ROW, row.parse("(70680,0,'1._FC_Nürnberg'),"));
    assertEquals("1._FC_Nürnberg", row.values.get(2));
  }

  @Test
  void schemaCommentsAndTheInsertLineCarryNoData() {
    for (String line : new String[] {
        "-- MySQL dump 10.19", "/*!40101 SET NAMES binary*/;", "CREATE TABLE `pagelinks` (",
        "  `pl_from` int(8) unsigned NOT NULL DEFAULT 0,", ") ENGINE=InnoDB;", "",
        "INSERT INTO `pagelinks` VALUES", "UNLOCK TABLES;"}) {
      assertEquals(SqlRow.Fate.NOT_DATA, row.parse(line), line);
    }
  }

  @Test
  void anythingThatStartsAsDataAndDoesNotParseIsMalformed() {
    for (String line : new String[] {
        "INSERT INTO `pagelinks` VALUES (1,0,4),",  // a tuple on the INSERT line would be lost
        "(1,0,4)",           // no trailer
        "(1,0,4),(2,0,5),",  // two tuples on a line
        "(1,0,'open),",      // unterminated string
        "(1,0,'a\\qb'),",    // an escape mysqldump does not write
        "(1,0,abc),",        // a bare word
        "(1,0,),",           // an empty value
        "("}) {
      assertEquals(SqlRow.Fate.MALFORMED, row.parse(line), line);
    }
  }
}
