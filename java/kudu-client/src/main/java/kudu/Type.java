// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import static kudu.Common.DataType;

/**
 * Describes all the types available to build table schemas.
 */
public enum Type {

  INT8 (DataType.INT8, "int8"),
  INT16 (DataType.INT16, "int16"),
  INT32 (DataType.INT32, "int32"),
  INT64 (DataType.INT64, "int64"),
  STRING (DataType.STRING, "string"),
  BOOL (DataType.BOOL, "bool"),
  FLOAT (DataType.FLOAT, "float"),
  DOUBLE (DataType.DOUBLE, "double");


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
    } else if (type == DataType.BOOL) {
      return 1;
    } else if (type == DataType.INT8) {
      return 1;
    } else if (type == DataType.INT16) {
      return Shorts.BYTES;
    } else if (type == DataType.INT32 || type == DataType.FLOAT) {
      return Ints.BYTES;
    } else if (type == DataType.INT64 || type == DataType.DOUBLE) {
      return Longs.BYTES;
    } else {
      throw new IllegalArgumentException("The provided data type doesn't map" +
          " to know any known one.");
    }
  }

  /**
   * Convert the pb DataType to a Type
   * @param type DataType to convert
   * @return a matching Type
   */
  public static Type getTypeForDataType(DataType type) {
    switch (type) {
      case STRING: return STRING;
      case BOOL: return BOOL;
      case INT8: return INT8;
      case INT16: return INT16;
      case INT32: return INT32;
      case INT64: return INT64;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      default:
        throw new IllegalArgumentException("The provided data type doesn't map" +
            " to know any known one: " + type.getDescriptorForType().getFullName());

    }
  }

}
