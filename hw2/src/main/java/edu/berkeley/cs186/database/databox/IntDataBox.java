package edu.berkeley.cs186.database.databox;
import java.nio.ByteBuffer;

public class IntDataBox extends DataBox {
  private int i;

  public IntDataBox(int i) {
    this.i = i;
  }

  @Override
  public Type type() {
    return Type.intType();
  }

  @Override
  public int getInt() {
    return this.i;
  }

  @Override
  public byte[] toBytes() {
    return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
  }

  @Override
  public String toString() {
    return new Integer(i).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof IntDataBox)) {
      return false;
    }
    IntDataBox i = (IntDataBox) o;
    return this.i == i.i;
  }

  @Override
  public int hashCode() {
    return new Integer(i).hashCode();
  }

  @Override
  public int compareTo(DataBox d) {
    if (!(d instanceof IntDataBox)) {
      String err = String.format("Invalid comparison between %s and %s.",
                                 toString(), d.toString());
      throw new DataBoxException(err);
    }
    IntDataBox i = (IntDataBox) d;
    return Integer.compare(this.i, i.i);
  }
}
