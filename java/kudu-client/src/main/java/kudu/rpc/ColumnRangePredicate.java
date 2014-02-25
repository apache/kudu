// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.ColumnSchema;
import kudu.Type;
import kudu.tserver.Tserver;

/**
 * A range predicate on one of the columns in the underlying data
 * The both boundaries are inclusive
 */
public class ColumnRangePredicate {

  final Tserver.ColumnRangePredicatePB.Builder pb = Tserver.ColumnRangePredicatePB
      .newBuilder();
  private final ColumnSchema column;
  private byte[] lowerBound = null;
  private byte[] upperBound = null;

  /**
   * Create the predicate on the specified column
   * @param column
   */
  public ColumnRangePredicate(ColumnSchema column) {
    this.column = column;
    this.pb.setColumn(ProtobufHelper.columnToPb(column));
  }

  private void setLowerBound(byte[] value) {
    this.lowerBound = value;
    pb.setLowerBound(ZeroCopyLiteralByteString.wrap(this.lowerBound));
  }

  private void setUpperBound(byte[] value) {
    this.upperBound = value;
    pb.setUpperBound(ZeroCopyLiteralByteString.wrap(this.upperBound));
  }

  /**
   * Set a byte for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(byte lowerBound) {
    checkColumn(Type.INT8);
    setLowerBound(new byte[] { lowerBound });
  }

  /**
   * Set a short for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(short lowerBound) {
    checkColumn(Type.INT16);
    setLowerBound(Bytes.fromShort(lowerBound));
  }

  /**
   * Set an int for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(int lowerBound) {
    checkColumn(Type.INT32);
    setLowerBound(Bytes.fromInt(lowerBound));
  }

  /**
   * Set a long for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(long lowerBound) {
    checkColumn(Type.INT64);
    setLowerBound(Bytes.fromLong(lowerBound));
  }

  /**
   * Set a string for the lower bound
   * @param lowerBound value for the lower bound
   */
  public void setLowerBound(String lowerBound) {
    checkColumn(Type.STRING);
    setLowerBound(lowerBound.getBytes());
  }

  public void setUnsignedLowerBound(byte lowerBound) {
    checkColumn(Type.UINT8);
    // TODO
  }

  public void setUnsignedLowerBound(short lowerBound) {
    checkColumn(Type.UINT16);
    // TODO
  }

  public void setUnsignedLowerBound(int lowerBound) {
    checkColumn(Type.UINT32);
    // TODO
  }

  public void setUnsignedLowerBound(long lowerBound) {
    checkColumn(Type.UINT64);
    // TODO
  }

  /**
   * Set a byte for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(byte upperBound) {
    checkColumn(Type.INT8);
    setUpperBound(new byte[] { upperBound });
  }

  /**
   * Set a short for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(short upperBound) {
    checkColumn(Type.INT16);
    setUpperBound(Bytes.fromShort(upperBound));
  }

  /**
   * Set an int for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(int upperBound) {
    checkColumn(Type.INT32);
    setUpperBound(Bytes.fromInt(upperBound));
  }

  /**
   * Set a long for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(long upperBound) {
    checkColumn(Type.INT64);
    setUpperBound(Bytes.fromLong(upperBound));
  }

  /**
   * Set a string for the upper bound
   * @param upperBound value for the upper bound
   */
  public void setUpperBound(String upperBound) {
    checkColumn(Type.STRING);
    setUpperBound(upperBound.getBytes());
  }

  public void setUnsignedUpperBound(byte upperBound) {
    checkColumn(Type.UINT8);
    // TODO
  }

  public void setUnsignedUpperBound(short upperBound) {
    checkColumn(Type.UINT16);
    // TODO
  }

  public void setUnsignedUpperBound(int upperBound) {
    checkColumn(Type.UINT32);
    // TODO
  }

  public void setUnsignedUpperBound(long upperBound) {
    checkColumn(Type.UINT64);
    // TODO
  }

  /**
   * Get the column used by this predicate
   * @return the column
   */
  public ColumnSchema getColumn() {
    return column;
  }

  /**
   * Get the lower bound in its raw representation
   * @return lower bound as a byte array
   */
  public byte[] getLowerBound() {
    return lowerBound;
  }

  /**
   * Get the upper bound in its raw representation
   * @return upper bound as a byte array
   */
  public byte[] getUpperBound() {
    return upperBound;
  }

  private void checkColumn(Type passedType) {
    if (!this.column.getType().equals(passedType)) {
      throw new IllegalArgumentException(column.getName() +
          "'s type isn't " + passedType.getName() + ", it's " + column.getType().getName());
    }
  }
}
