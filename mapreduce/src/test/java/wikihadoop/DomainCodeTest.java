package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class DomainCodeTest {

  @Test
  void wikipediaDesktopAndMobile() {
    assertEquals("en", DomainCode.of("en.wikipedia", "desktop"));
    assertEquals("en.m", DomainCode.of("en.wikipedia", "mobile-web"));
  }

  @Test
  void mobileAppViewsCountUnderTheMobileCode() {
    assertEquals("en.m", DomainCode.of("en.wikipedia", "mobile-app"));
  }

  @Test
  void sisterProjectsTakeTheirSuffixAfterTheMobileMark() {
    assertEquals("fr.b", DomainCode.of("fr.wikibooks", "desktop"));
    assertEquals("fr.m.b", DomainCode.of("fr.wikibooks", "mobile-web"));
    assertEquals("de.voy", DomainCode.of("de.wikivoyage", "desktop"));
    assertEquals("ja.m.d", DomainCode.of("ja.wiktionary", "mobile-app"));
  }

  @Test
  void wikimediaProjects() {
    assertEquals("commons.m", DomainCode.of("commons.wikimedia", "desktop"));
    assertEquals("commons.m.m", DomainCode.of("commons.wikimedia", "mobile-web"));
  }

  @Test
  void projectsWithoutALanguage() {
    assertEquals("www.wd", DomainCode.of("wikidata", "desktop"));
    assertEquals("m.wd", DomainCode.of("wikidata", "mobile-web"));
    assertEquals("www.w", DomainCode.of("mediawiki", "desktop"));
    assertEquals("m.s", DomainCode.of("wikisource", "mobile-app"));
    assertEquals("www.m", DomainCode.of("wikimedia", "desktop"));
    assertEquals("m.m", DomainCode.of("wikimedia", "mobile-web"));
    assertEquals("\"\"", DomainCode.of("wikifunctions", "desktop"));
    assertEquals("\"\"", DomainCode.of("wikifunctions", "mobile-web"));
  }

  @Test
  void unknownWikisHaveNoCode() {
    assertNull(DomainCode.of("en.somethingelse", "desktop"));
    assertNull(DomainCode.of("nodot", "desktop"));
  }
}
