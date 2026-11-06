package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PageviewLineTest {

  private final PageviewLine row = new PageviewLine();

  @Test
  void parsesARowAndItsHours() {
    assertEquals(PageviewLine.Fate.PARSED, row.parse("en.wikipedia Mohs_scale 7188 desktop 5 C2T3"));
    assertEquals("en.wikipedia", row.wiki);
    assertEquals("Mohs_scale", row.title);
    assertEquals("7188", row.pageId);
    assertEquals("desktop", row.access);
    assertEquals(5, row.daily);
    long[] expected = new long[24];
    expected[2] = 2;
    expected[19] = 3;
    assertArrayEquals(expected, row.hours);
  }

  @Test
  void keepsANullPageIdAndADashTitle() {
    assertEquals(PageviewLine.Fate.PARSED, row.parse("aa.wikibooks - null desktop 1 Q1"));
    assertEquals("null", row.pageId);
    assertEquals("-", row.title);
  }

  @Test
  void clearsTheHoursOfThePreviousRow() {
    row.parse("en.wikipedia A 1 desktop 7 X7");
    row.parse("en.wikipedia B 2 desktop 1 A1");
    assertEquals(0, row.hours[23]);
    assertEquals(1, row.hours[0]);
  }

  @Test
  void rejectsEachMalformedLineWithItsReason() {
    assertEquals(PageviewLine.Fate.FIELDS, row.parse("broken line"));
    assertEquals(PageviewLine.Fate.FIELDS, row.parse("en.wikipedia A B 1 desktop 1 A1"));
    assertEquals(PageviewLine.Fate.BAD_COUNTS, row.parse("en.wikipedia A 1 desktop x A1"));
    assertEquals(PageviewLine.Fate.BAD_COUNTS, row.parse("en.wikipedia A 1 desktop 1 Y1"));
    assertEquals(PageviewLine.Fate.BAD_COUNTS, row.parse("en.wikipedia A 1 desktop 1 AB1"));
    assertEquals(PageviewLine.Fate.BAD_COUNTS, row.parse("en.wikipedia A 1 desktop 1 "));
    assertEquals(PageviewLine.Fate.HOURS_OUT_OF_ORDER, row.parse("en.wikipedia A 1 desktop 3 C1A2"));
    assertEquals(PageviewLine.Fate.HOURS_OUT_OF_ORDER, row.parse("en.wikipedia A 1 desktop 2 A1A1"));
    assertEquals(PageviewLine.Fate.DAILY_NOT_SUM, row.parse("en.wikipedia A 1 desktop 4 A2C3"));
  }
}
