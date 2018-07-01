package edu.berkeley.cs186.database.databox;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * There are four primitive types:
 *
 *   1. 1-byte booleans (Type.boolType()),
 *   2. 4-byte integers (Type.intType()),
 *   3. 4-byte floats (Type.floatType()), and
 *   4. n-byte strings (Type.stringType(n)) where n > 0.
 *
 * Note that n-byte strings and m-byte strings are considered different types
 * when n != m.
 */
public class Type {
  // The type of this type.
  private TypeId typeId;

  // The size (in bytes) of an element of this type.
  private int sizeInBytes;

  private Type(TypeId typeId, int sizeInBytes) {
      this.typeId = typeId;
      this.sizeInBytes = sizeInBytes;
  }

  public static Type boolType() {
    // Unlike all the other primitive type boxes (e.g. Integer, Float), Boolean
    // does not have a BYTES field, so we hand code the fact that Java booleans
    // are 1 byte.
    return new Type(TypeId.BOOL, 1);
  }

  public static Type intType() {
    return new Type(TypeId.INT, Integer.BYTES);
  }

  public static Type floatType() {
    return new Type(TypeId.FLOAT, Float.BYTES);
  }

  public static Type stringType(int n) {
    if (n < 0) {
      String msg = String.format("The provided string length %d is negative.", n);
      throw new DataBoxException(msg);
    }
    if (n == 0) {
      String msg = String.format("Empty strings are not supported.");
      throw new DataBoxException(msg);
    }
    return new Type(TypeId.STRING, n);
  }

  public TypeId getTypeId() {
      return typeId;
  }

  public int getSizeInBytes() {
      return sizeInBytes;
  }

  public byte[] toBytes() {
    // A Type is uniquely identified by its typeId `t` and the size (in bytes)
    // of an element of the type `s`. A Type is serialized as two integers. The
    // first is the ordinal corresponding to `t`. The second is `s`.
    //
    // For example, the type "42-byte string" would serialized as the bytes [3,
    // 42] because 3 is the ordinal of the STRING TypeId and 42 is the number
    // of bytes in a 42-byte string (duh).
    ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2);
    buf.putInt(typeId.ordinal());
    buf.putInt(sizeInBytes);
    return buf.array();
  }

  public static Type fromBytes(ByteBuffer buf) {
    int ordinal = buf.getInt();
    int sizeInBytes = buf.getInt();
    if (ordinal == TypeId.BOOL.ordinal()) {
      assert(sizeInBytes == 1);
      return Type.boolType();
    } else if (ordinal == TypeId.INT.ordinal()) {
      assert(sizeInBytes == Integer.BYTES);
      return Type.intType();
    } else if (ordinal == TypeId.FLOAT.ordinal()) {
      assert(sizeInBytes == Integer.BYTES);
      return Type.floatType();
    } else if (ordinal == TypeId.STRING.ordinal()) {
      return Type.stringType(sizeInBytes);
    } else {
      String err = String.format("Unknown TypeId ordinal %d.", ordinal);
      throw new IllegalArgumentException(err);
    }
  }

  @Override
  public String toString() {
    return String.format("(%s, %d)", typeId.toString(), sizeInBytes);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Type)) {
      return false;
    }
    Type t = (Type) o;
    return typeId.equals(t.typeId) && sizeInBytes == t.sizeInBytes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(typeId, sizeInBytes);
  }
}
