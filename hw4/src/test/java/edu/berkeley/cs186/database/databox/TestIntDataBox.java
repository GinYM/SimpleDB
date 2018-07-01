package edu.berkeley.cs186.database.databox;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestIntDataBox {
  @Test
  public void testType() {
    assertEquals(Type.intType(), new IntDataBox(0).type());
  }

  @Test(expected = DataBoxException.class)
  public void testGetBool() {
    new IntDataBox(0).getBool();
  }

  @Test
  public void testGetInt() {
    assertEquals(0, new IntDataBox(0).getInt());
  }

  @Test(expected = DataBoxException.class)
  public void testGetFloat() {
    new IntDataBox(0).getFloat();
  }

  @Test(expected = DataBoxException.class)
  public void testGetString() {
    new IntDataBox(0).getString();
  }

  @Test
  public void testToAndFromBytes() {
    for (int i = -10; i < 10; ++i) {
      IntDataBox d = new IntDataBox(i);
      byte[] bytes = d.toBytes();
      assertEquals(d, DataBox.fromBytes(ByteBuffer.wrap(bytes), Type.intType()));
    }
  }

  @Test
  public void testEquals() {
    IntDataBox zero = new IntDataBox(0);
    IntDataBox one = new IntDataBox(1);
    assertEquals(zero, zero);
    assertEquals(one, one);
    assertNotEquals(zero, one);
    assertNotEquals(one, zero);
  }

  @Test
  public void testCompareTo() {
    IntDataBox zero = new IntDataBox(0);
    IntDataBox one = new IntDataBox(1);
    assertTrue(zero.compareTo(zero) == 0);
    assertTrue(zero.compareTo(one) < 0);
    assertTrue(one.compareTo(one) == 0);
    assertTrue(one.compareTo(zero) > 0);
  }
}
