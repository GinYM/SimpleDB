package edu.berkeley.cs186.database.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestRecordId {
  @Test
  public void testSizeInBytes() {
    assertEquals(6, RecordId.getSizeInBytes());
  }

  @Test
  public void testToAndFromBytes() {
    for (int i = 0; i < 10; ++i) {
      for (short j = 0; j < 10; ++j) {
        RecordId rid = new RecordId(i, j);
        assertEquals(rid, RecordId.fromBytes(ByteBuffer.wrap(rid.toBytes())));
      }
    }
  }

  @Test
  public void testEquals() {
    RecordId a = new RecordId(0, (short) 0);
    RecordId b = new RecordId(1, (short) 0);
    RecordId c = new RecordId(0, (short) 1);

    assertEquals(a, a);
    assertNotEquals(a, b);
    assertNotEquals(a, c);
    assertNotEquals(b, a);
    assertEquals(b, b);
    assertNotEquals(b, c);
    assertNotEquals(c, a);
    assertNotEquals(c, b);
    assertEquals(c, c);
  }

  @Test
  public void testCompareTo() {
    RecordId a = new RecordId(0, (short) 0);
    RecordId b = new RecordId(0, (short) 1);
    RecordId c = new RecordId(1, (short) 0);
    RecordId d = new RecordId(1, (short) 1);

    assertTrue(a.compareTo(a) == 0);
    assertTrue(b.compareTo(b) == 0);
    assertTrue(c.compareTo(c) == 0);
    assertTrue(d.compareTo(d) == 0);

    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(c) < 0);
    assertTrue(c.compareTo(d) < 0);

    assertTrue(d.compareTo(c) > 0);
    assertTrue(c.compareTo(b) > 0);
    assertTrue(b.compareTo(a) > 0);
  }
}
