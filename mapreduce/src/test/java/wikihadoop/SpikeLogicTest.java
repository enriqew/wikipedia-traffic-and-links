package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class SpikeLogicTest {

  @Test
  void baselineLeavesOutTheDayAndItsNeighbours() {
    long[] daily = {10, 10, 10, 500, 900, 10, 12};
    // Day 4 (900): left out are days 3, 4, 5; the rest are 10, 10, 10, 12.
    assertEquals(10.0, SpikeRule.baseline(daily, 4));
    // Day 0: left out are days 0 and 1; the rest are 10, 500, 900, 10, 12, median 12.
    assertEquals(12.0, SpikeRule.baseline(daily, 0));
  }

  @Test
  void baselineOfAnEvenCountIsTheMeanOfTheMiddleTwo() {
    // Day 1: left out are days 0, 1, 2; the rest are 10, 20, 30, 40.
    assertEquals(25.0, SpikeRule.baseline(new long[] {0, 0, 0, 10, 20, 30, 40}, 1));
  }

  @Test
  void aSpikeNeedsTheFactorAndTheFloor() {
    long wiki = 190_000_000L;  // floor 19,000
    assertTrue(SpikeRule.isSpike(20_000, 1_000, wiki));
    assertFalse(SpikeRule.isSpike(9_000, 0, wiki));  // ten times nothing, but under the floor
    assertFalse(SpikeRule.isSpike(25_000, 3_000, wiki));  // over the floor, under ten times
    assertTrue(SpikeRule.isSpike(19_000, 0.4, wiki));  // a baseline under one counts as one
  }

  @Test
  void windowIndexesDays() {
    Window w = new Window(LocalDate.parse("2026-10-01"), 31);
    assertEquals(0, w.index("2026-10-01"));
    assertEquals(30, w.index("2026-10-31"));
    assertEquals(-1, w.index("2026-11-01"));
    assertEquals(-1, w.index("2026-09-30"));
    assertEquals("2026-10-28", w.date(27));
  }

  @Test
  void rankingKeepsArticlesOnly() throws Exception {
    Wikis w = Wikis.parse(new BufferedReader(new StringReader(
        "es.wikipedia\tWikipedia:Portada\tanexo|archivo|categoría|file|wikipedia\n")));
    assertTrue(w.ranked("es.wikipedia", "Lionel_Messi"));
    assertTrue(w.ranked("es.wikipedia", "Star_Wars:_Episodio_IV"));  // a colon, not a namespace
    assertFalse(w.ranked("es.wikipedia", "Wikipedia:Portada"));
    assertFalse(w.ranked("es.wikipedia", "Categoría:Futbolistas"));
    assertFalse(w.ranked("es.wikipedia", "ARCHIVO:Foto.jpg"));  // namespace names ignore case
    assertFalse(w.ranked("es.wikipedia", "-"));
    assertFalse(w.ranked("xx.wikipedia", "Anything"));  // not one of the ranked wikis
  }

  @Test
  void theShippedTableHasTheTenWikis() {
    Wikis w = Wikis.load();
    assertEquals(10, w.names().size());
    assertFalse(w.ranked("en.wikipedia", "Main_Page"));
    assertFalse(w.ranked("en.wikipedia", "Special:Search"));
    assertFalse(w.ranked("de.wikipedia", "Wikipedia:Hauptseite"));
    assertTrue(w.ranked("en.wikipedia", "Lamprey"));
  }
}
