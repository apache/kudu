// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu;

import static org.apache.kudu.Common.DataType;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.util.DecimalUtil;

/**
 * Describes all the types available to build table schemas.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum Type {

  INT8(DataType.INT8, "int8"),
  INT16(DataType.INT16, "int16"),
  INT32(DataType.INT32, "int32"),
  INT64(DataType.INT64, "int64"),
  BINARY(DataType.BINARY, "binary"),
  STRING(DataType.STRING, "string"),
  BOOL(DataType.BOOL, "bool"),
  FLOAT(DataType.FLOAT, "float"),
  DOUBLE(DataType.DOUBLE, "double"),
  UNIXTIME_MICROS(DataType.UNIXTIME_MICROS, "unixtime_micros"),
  DECIMAL(Arrays.asList(DataType.DECIMAL32, DataType.DECIMAL64, DataType.DECIMAL128), "decimal"),
  VARCHAR(DataType.VARCHAR, "varchar"),
  DATE(DataType.DATE, "date");

  private final ImmutableList<DataType> dataTypes;
  private final String name;
  private final int size;

  /**
   * Private constructor used to pre-create the types
   * @param dataType DataType from the common's pb
   * @param name string representation of the type
   */
  Type(DataType dataType, String name) {
    this.dataTypes = ImmutableList.of(dataType);
    this.name = name;
    this.size = getTypeSize(dataType);
  }

  Type(List<DataType> dataTypes, String name) {
    this.dataTypes = ImmutableList.copyOf(dataTypes);
    this.name = name;
    this.size = -1;
  }

  /**
   * Get the data type from the common's pb
   * @return A DataType
   * @deprecated use {@link #getDataType(ColumnTypeAttributes)}
   */
  @Deprecated
  public DataType getDataType() {
    if (this == DECIMAL) {
      throw new IllegalStateException("Please use the newer getDataType(ColumnTypeAttributes) " +
          "to support the Decimal data type");
    }
    return this.dataTypes.get(0);
  }

  /**
   * Get the data type from the common's pb
   * @param typeAttributes the additional attributes of the type.
   * @return A DataType
   */
  public DataType getDataType(ColumnTypeAttributes typeAttributes) {
    if (this == DECIMAL) {
      return DecimalUtil.precisionToDataType(typeAttributes.getPrecision());
    }
    return this.dataTypes.get(0);
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
   * @deprecated use {@link #getSize(ColumnTypeAttributes)}
   */
  @Deprecated
  public int getSize() {
    if (this == DECIMAL) {
      throw new IllegalStateException("Please use the newer getSize(ColumnTypeAttributes) " +
          "to support the Decimal data type");
    }
    return this.size;
  }

  /**
   * The size of this type on the wire
   * @param typeAttributes the additional attributes of the type.
   * @return A size
   */
  public int getSize(ColumnTypeAttributes typeAttributes) {
    if (this == DECIMAL) {
      return DecimalUtil.precisionToSize(typeAttributes.getPrecision());
    }
    return this.size;
  }

  @Override
  public String toString() {
    return "Type: " + this.name;
  }

  /**
   * Gives the size in bytes for a given DataType, as per the pb specification
   * @param type pb type
   * @return size in bytes
   */
  private static int getTypeSize(DataType type) {
    switch (type) {
      case STRING:
      case BINARY:
      case VARCHAR:
        return 8 + 8; // offset then string length
      case BOOL:
      case INT8:
      case IS_DELETED:
        return 1;
      case INT16:
        return Shorts.BYTES;
      case INT32:
      case DATE:
      case FLOAT:
        return Ints.BYTES;
      case INT64:
      case DOUBLE:
      case UNIXTIME_MICROS:
        return Longs.BYTES;
      default: throw new IllegalArgumentException(
          "the provided data type doesn't map to any known one");
    }
  }

  /**
   * Convert the pb DataType to a Type
   * @param type DataType to convert
   * @return a matching Type
   */
  public static Type getTypeForDataType(DataType type) {
    switch (type) {
      case STRING:
        return STRING;
      case BINARY:
        return BINARY;
      case VARCHAR:
        return VARCHAR;
      case BOOL:
      case IS_DELETED:
        return BOOL;
      case INT8:
        return INT8;
      case INT16:
        return INT16;
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case UNIXTIME_MICROS:
        return UNIXTIME_MICROS;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case DATE:
        return DATE;
      case DECIMAL32:
      case DECIMAL64:
      case DECIMAL128:
        return DECIMAL;
      default:
        throw new IllegalArgumentException("the provided data type doesn't map " +
            "to any known one: " + type.getDescriptorForType().getFullName());
    }
  }

  /**
   * Create a Type from its name
   * @param name The DataType name. It accepts Type name (from the getName()
   * method) and ENUM name (from the name() method).
   * @throws IllegalArgumentException if the provided name doesn't map to any
   * known type.
   * @return a matching Type.
   */
  public static Type getTypeForName(String name) {
    for (Type t : values()) {
      if (t.name().equals(name) || t.getName().equals(name)) {
        return t;
      }
    }
    throw new IllegalArgumentException("The provided name doesn't map to any known type: " + name);
  }

  /**
   * @return true if this type has a pre-determined fixed size, false otherwise
   */
  public boolean isFixedSize() {
    return this != BINARY && this != STRING && this != VARCHAR;
  }
}