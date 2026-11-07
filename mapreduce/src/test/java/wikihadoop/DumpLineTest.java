package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class DumpLineTest {

  @Test
  void theTableComesFromTheFileName() throws IOException {
    assertEquals("page", DumpLine.table("simplewiki-20261101-page.sql.gz"));
    assertEquals("pagelinks", DumpLine.table("enwiki-20261101-pagelinks-00003.sql.gz"));
    assertEquals("linktarget", DumpLine.table("enwiki-20261101-linktarget-00000.sql.gz"));
    assertEquals("redirect", DumpLine.table("simplewiki-20261101-redirect.sql.gz"));
  }

  @Test
  void otherFilesAreRefused() {
    assertThrows(IOException.class, () -> DumpLine.table("simplewiki-20261101-categorylinks.sql.gz"));
    assertThrows(IOException.class, () -> DumpLine.table("simplewiki-latest-page.sql.gz"));
    assertThrows(IOException.class, () -> DumpLine.table("pageviews-20261005-user.bz2"));
  }
}
