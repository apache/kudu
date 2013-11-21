// Copyright (c) 2013, Cloudera, inc.
package kudu;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import static kudu.Common.DataType;

/**
 * Describes all the types available to build table schemas.
 */
public enum Type {

  UINT8 (DataType.UINT8, "uint8"),
  INT8 (DataType.INT8, "int8"),
  UINT16 (DataType.UINT16, "uint16"),
  INT16 (DataType.INT16, "int16"),
  UINT32 (DataType.UINT32, "uint32"),
  INT32 (DataType.INT32, "int32"),
  UINT64 (DataType.UINT64, "uint64"),
  INT64 (DataType.INT64, "int64"),
  STRING (DataType.STRING, "string");

  private final DataType dataType;
  private final String name;
  private final int size;

  /**
   * Private constructor used to pre-create the types
   * @param dataType DataType from the common's pb
   * @param name string representation of the type
   */
  private Type(DataType dataType, String name) {
    this.dataType = dataType;
    this.name = name;
    this.size = getTypeSize(this.dataType);
  }

  /**
   * Get the data type from the common's pb
   * @return A DataType
   */
  public DataType getDataType() {
    return this.dataType;
  }

  /**
   * Get the string representation of this type
   * @return The type's name
   */
  public String getName() {
    return this.name;
  }

  /**
   * The size of this type on the wire
   * @return A size
   */
  public int getSize() {
    return this.size;
  }

  @Override
  public String toString() {
    return "Type: " + this.name + ", size: " + this.size;
  }

  /**
   * Gives the size in bytes for a given DataType, as per the pb specification
   * @param type pb type
   * @return size in bytes
   */
  static int getTypeSize(DataType type) {

    if (type == DataType.STRING) {
      return 8 + 8; // offset then string length
    } else if (type == DataType.UINT8 || type == DataType.INT8) {
      return 1;
    } else if (type == DataType.UINT16 || type == DataType.INT16) {
      return Shorts.BYTES;
    } else if (type == DataType.UINT32 || type == DataType.INT32) {
      return Ints.BYTES;
    } else if (type == DataType.UINT64 || type == DataType.INT64) {
      return Longs.BYTES;
    } else {
      throw new IllegalArgumentException("The provided data type doesn't map" +
          " to know any known one.");
    }
  }

}
