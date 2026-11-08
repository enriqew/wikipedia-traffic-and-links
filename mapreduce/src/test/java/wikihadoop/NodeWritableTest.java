package wikihadoop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class NodeWritableTest {

  private static byte[] bytes(NodeWritable n) throws IOException {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    n.write(new DataOutputStream(b));
    return b.toByteArray();
  }

  private static NodeWritable read(byte[] b, NodeWritable into) throws IOException {
    into.readFields(new DataInputStream(new ByteArrayInputStream(b)));
    return into;
  }

  @Test
  void aRecordComesBackWithItsRanksAndOutLinks() throws IOException {
    long[] links = {3, 17, 18, 70_000_000, 70_000_001};
    byte[] b = bytes(new NodeWritable().record(1_234_567_890L, 999_999_999L, links, links.length));
    NodeWritable n = read(b, new NodeWritable());
    assertTrue(n.isRecord());
    assertEquals(1_234_567_890L, n.rank());
    assertEquals(999_999_999L, n.previous());
    assertArrayEquals(links, n.copyOfTargets());
  }

  @Test
  void aContributionIsJustItsMassAndClearsAReusedRecord() throws IOException {
    NodeWritable reused = new NodeWritable().record(5, 6, new long[] {1, 2}, 2);
    NodeWritable n = read(bytes(new NodeWritable().contribution(42)), reused);
    assertFalse(n.isRecord());
    assertEquals(42, n.rank());
    assertEquals(0, n.size());
  }

  @Test
  void outLinksAreDeltasSoNeighbouringIdsTakeAByteEach() throws IOException {
    long[] links = new long[1000];
    for (int i = 0; i < links.length; i++) {
      links[i] = 50_000_000L + i;
    }
    int size = bytes(new NodeWritable().record(1, 1, links, links.length)).length;
    assertTrue(size < 1020, "record of 1,000 neighbouring links took " + size + " bytes");
  }

  @Test
  void outLinksMustBeAscending() {
    assertThrows(IllegalArgumentException.class, () -> new NodeWritable().record(1, 1, new long[] {5, 5}, 2));
    assertThrows(IllegalArgumentException.class, () -> new NodeWritable().record(1, 1, new long[] {5, 4}, 2));
  }
}
