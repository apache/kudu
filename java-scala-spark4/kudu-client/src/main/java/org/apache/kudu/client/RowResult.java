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

package org.apache.kudu.client;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.TimestampUtil;

/**
 * RowResult represents one row from a scanner.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class RowResult {

  protected static final int INDEX_RESET_LOCATION = -1;
  protected int index = INDEX_RESET_LOCATION;

  protected final Schema schema;

  /**
   * Prepares the row representation using the provided data. Doesn't copy data
   * out of the byte arrays. Package private.
   * @param schema Schema used to build the rowData
   * @param rowIndex The index of the row in the rowData that this RowResult represents
   */
  RowResult(Schema schema, int rowIndex) {
    this.schema = schema;
    this.index = rowIndex;
  }

  void resetPointer() {
    advancePointerTo(INDEX_RESET_LOCATION);
  }

  /**
   * Package-protected, only meant to be used by the RowResultIterator
   */
  void advancePointerTo(int rowIndex) {
    this.index = rowIndex;
  }

  /**
   * Get the specified column's integer
   * @param columnName name of the column to get data for
   * @return an integer
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final int getInt(String columnName) {
    return getInt(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's integer
   * @param columnIndex Column index in the schema
   * @return an integer
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract int getInt(int columnIndex);

  /**
   * Get the specified column's short
   * @param columnName name of the column to get data for
   * @return a short
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final short getShort(String columnName) {
    return getShort(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's short
   * @param columnIndex Column index in the schema
   * @return a short
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract short getShort(int columnIndex);

  /**
   * Get the specified column's boolean
   * @param columnName name of the column to get data for
   * @return a boolean
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final boolean getBoolean(String columnName) {
    return getBoolean(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's boolean
   * @param columnIndex Column index in the schema
   * @return a boolean
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract boolean getBoolean(int columnIndex);

  /**
   * Get the specified column's byte
   * @param columnName name of the column to get data for
   * @return a byte
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final byte getByte(String columnName) {
    return getByte(this.schema.getColumnIndex(columnName));

  }

  /**
   * Get the specified column's byte
   * @param columnIndex Column index in the schema
   * @return a byte
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract byte getByte(int columnIndex);

  /**
   * Get the specified column's long
   *
   * If this is a UNIXTIME_MICROS column, the long value corresponds to a number of microseconds
   * since midnight, January 1, 1970 UTC.
   *
   * @param columnName name of the column to get data for
   * @return a positive long
   * @throws IllegalArgumentException if the column doesn't exist or is null
   */
  public final long getLong(String columnName) {
    return getLong(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's long
   *
   * If this is a UNIXTIME_MICROS column, the long value corresponds to a number of microseconds
   * since midnight, January 1, 1970 UTC.
   *
   * @param columnIndex Column index in the schema
   * @return a positive long
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract long getLong(int columnIndex);

  /**
   * Get the specified column's float
   * @param columnName name of the column to get data for
   * @return a float
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final float getFloat(String columnName) {
    return getFloat(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's float
   * @param columnIndex Column index in the schema
   * @return a float
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract float getFloat(int columnIndex);

  /**
   * Get the specified column's double
   * @param columnName name of the column to get data for
   * @return a double
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final double getDouble(String columnName) {
    return getDouble(this.schema.getColumnIndex(columnName));

  }

  /**
   * Get the specified column's double
   * @param columnIndex Column index in the schema
   * @return a double
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract double getDouble(int columnIndex);

  /**
   * Get the specified column's Decimal.
   *
   * @param columnName name of the column to get data for
   * @return a BigDecimal
   * @throws IllegalArgumentException if the column doesn't exist or is null
   */
  public final BigDecimal getDecimal(String columnName) {
    return getDecimal(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Decimal.
   *
   * @param columnIndex Column index in the schema
   * @return a BigDecimal.
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract BigDecimal getDecimal(int columnIndex);

  /**
   * Get the specified column's Timestamp.
   *
   * @param columnName name of the column to get data for
   * @return a Timestamp
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public final Timestamp getTimestamp(String columnName) {
    return getTimestamp(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Timestamp.
   *
   * @param columnIndex Column index in the schema
   * @return a Timestamp
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract Timestamp getTimestamp(int columnIndex);

  /**
   * Get the specified column's Date.
   *
   * @param columnName name of the column to get data for
   * @return a Date
   * @throws IllegalArgumentException if the column doesn't exist,
   * is null, is unset, or the type doesn't match the column's type
   */
  public final Date getDate(String columnName) {
    return getDate(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's Date.
   *
   * @param columnIndex Column index in the schema
   * @return a Date
   * @throws IllegalArgumentException if the column is null, is unset,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final Date getDate(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.DATE);
    int days = getInt(columnIndex);
    return DateUtil.epochDaysToSqlDate(days);
  }

  /**
   * Get the schema used for this scanner's column projection.
   * @return a column projection as a schema.
   */
  public final Schema getColumnProjection() {
    return this.schema;
  }

  /**
   * Get the specified column's string.
   * @param columnName name of the column to get data for
   * @return a string
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final String getString(String columnName) {
    return getString(this.schema.getColumnIndex(columnName));

  }

  /**
   * Get the specified column's string.
   * @param columnIndex Column index in the schema
   * @return a string
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final String getString(int columnIndex) {
    checkType(columnIndex, Type.STRING);
    return getVarLengthData(columnIndex);
  }

  protected abstract String getVarLengthData(int columnIndex);

  /**
   * Get the specified column's varchar.
   * @param columnIndex Column index in the schema
   * @return a string
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final String getVarchar(int columnIndex) {
    checkType(columnIndex, Type.VARCHAR);
    return getVarLengthData(columnIndex);
  }

  /**
   * Get the specified column's varchar.
   * @param columnName name of the column to get data for
   * @return a string
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   */
  public final String getVarchar(String columnName) {
    return getVarchar(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get a copy of the specified column's binary data.
   * @param columnName name of the column to get data for
   * @return a byte[] with the binary data.
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final byte[] getBinaryCopy(String columnName) {
    return getBinaryCopy(this.schema.getColumnIndex(columnName));

  }

  /**
   * Get a copy of the specified column's binary data.
   * @param columnIndex Column index in the schema
   * @return a byte[] with the binary data.
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract byte[] getBinaryCopy(int columnIndex);

  /**
   * Get the specified column's binary data.
   *
   * This doesn't copy the data and instead returns a ByteBuffer that wraps it.
   *
   * @param columnName name of the column to get data for
   * @return a ByteBuffer with the binary data.
   * @throws IllegalArgumentException if the column doesn't exist, is null,
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final ByteBuffer getBinary(String columnName) {
    return getBinary(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's binary data.
   *
   * This doesn't copy the data and instead returns a ByteBuffer that wraps it.
   *
   * @param columnIndex Column index in the schema
   * @return a ByteBuffer with the binary data.
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract ByteBuffer getBinary(int columnIndex);

  /**
   * Get if the specified column is NULL
   * @param columnName name of the column in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final boolean isNull(String columnName) {
    return isNull(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get if the specified column is NULL
   * @param columnIndex Column index in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public abstract boolean isNull(int columnIndex);

  /**
   * Get the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.VARCHAR -> java.lang.String
   *  Type.STRING -> java.lang.String
   *  Type.BINARY -> byte[]
   *  Type.DECIMAL -> java.math.BigDecimal
   *
   * @param columnName name of the column in the schema
   * @return the column's value as an Object, null if the value is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final Object getObject(String columnName) {
    return getObject(this.schema.getColumnIndex(columnName));
  }

  /**
   * Get the specified column's value as an Object.
   *
   * This method is useful when you don't care about autoboxing
   * and your existing type handling logic is based on Java types.
   *
   * The Object type is based on the column's {@link Type}:
   *  Type.BOOL -> java.lang.Boolean
   *  Type.INT8 -> java.lang.Byte
   *  Type.INT16 -> java.lang.Short
   *  Type.INT32 -> java.lang.Integer
   *  Type.INT64 -> java.lang.Long
   *  Type.UNIXTIME_MICROS -> java.sql.Timestamp
   *  Type.FLOAT -> java.lang.Float
   *  Type.DOUBLE -> java.lang.Double
   *  Type.VARCHAR -> java.lang.String
   *  Type.STRING -> java.lang.String
   *  Type.BINARY -> byte[]
   *  Type.DECIMAL -> java.math.BigDecimal
   *  Type.Date -> java.sql.Date
   *
   * @param columnIndex Column index in the schema
   * @return the column's value as an Object, null if the value is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final Object getObject(int columnIndex) {
    checkValidColumn(columnIndex);
    if (isNull(columnIndex)) {
      return null;
    }
    Type type = schema.getColumnByIndex(columnIndex).getType();
    switch (type) {
      case BOOL: return getBoolean(columnIndex);
      case INT8: return getByte(columnIndex);
      case INT16: return getShort(columnIndex);
      case INT32: return getInt(columnIndex);
      case INT64: return getLong(columnIndex);
      case DATE: return getDate(columnIndex);
      case UNIXTIME_MICROS: return getTimestamp(columnIndex);
      case FLOAT: return getFloat(columnIndex);
      case DOUBLE: return getDouble(columnIndex);
      case VARCHAR: return getVarchar(columnIndex);
      case STRING: return getString(columnIndex);
      case BINARY: return getBinaryCopy(columnIndex);
      case DECIMAL: return getDecimal(columnIndex);
      default: throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  /**
   * @return true if the RowResult has the IS_DELETED virtual column
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final boolean hasIsDeleted() {
    return schema.hasIsDeleted();
  }

  /**
   * @return the value of the IS_DELETED virtual column
   * @throws IllegalStateException if no IS_DELETED virtual column exists
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public final boolean isDeleted() {
    return getBoolean(schema.getIsDeletedIndex());
  }

  /**
   * Get the type of a column in this result.
   * @param columnName name of the column
   * @return a type
   */
  public final Type getColumnType(String columnName) {
    return this.schema.getColumn(columnName).getType();
  }

  /**
   * Get the type of a column in this result.
   * @param columnIndex column index in the schema
   * @return a type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public final Type getColumnType(int columnIndex) {
    return this.schema.getColumnByIndex(columnIndex).getType();
  }

  /**
   * Get the schema associated with this result.
   * @return a schema
   */
  public final Schema getSchema() {
    return schema;
  }

  /**
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  protected final void checkValidColumn(int columnIndex) {
    if (columnIndex >= schema.getColumnCount()) {
      throw new IndexOutOfBoundsException("Requested column is out of range, " +
          columnIndex + " out of " + schema.getColumnCount());
    }
  }

  /**
   * @throws IllegalArgumentException if the column is null
   */
  protected final void checkNull(int columnIndex) {
    if (!schema.hasNullableColumns()) {
      return;
    }
    if (isNull(columnIndex)) {
      ColumnSchema columnSchema = schema.getColumnByIndex(columnIndex);
      throw new IllegalArgumentException("The requested column (name: " + columnSchema.getName() +
          ", index: " + columnIndex + ") is null");
    }
  }

  protected final void checkType(int columnIndex, Type... types) {
    ColumnSchema columnSchema = schema.getColumnByIndex(columnIndex);
    Type columnType = columnSchema.getType();
    for (Type type : types) {
      if (columnType.equals(type)) {
        return;
      }
    }
    throw new IllegalArgumentException("Column (name: " + columnSchema.getName() +
        ", index: " + columnIndex + ") is of type " +
        columnType.getName() + " but was requested as a type " + Arrays.toString(types));
  }

  /**
   * Return the actual data from this row in a stringified key=value
   * form.
   */
  public String rowToString() {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);
      if (i != 0) {
        buf.append(", ");
      }
      Type type = col.getType();
      buf.append(type.name());
      buf.append(" ").append(col.getName());
      if (col.getTypeAttributes() != null) {
        buf.append(col.getTypeAttributes().toStringForType(type));
      }
      buf.append("=");
      if (isNull(i)) {
        buf.append("NULL");
      } else {
        switch (col.getType()) {
          case INT8:
            buf.append(getByte(i));
            break;
          case INT16:
            buf.append(getShort(i));
            break;
          case INT32:
            buf.append(getInt(i));
            break;
          case INT64:
            buf.append(getLong(i));
            break;
          case DATE:
            buf.append(DateUtil.epochDaysToDateString(getInt(i)));
            break;
          case UNIXTIME_MICROS: {
            buf.append(TimestampUtil.timestampToString(getTimestamp(i)));
          } break;
          case VARCHAR:
            buf.append(getVarchar(i));
            break;
          case STRING:
            buf.append(getString(i));
            break;
          case BINARY:
            buf.append(Bytes.pretty(getBinaryCopy(i)));
            break;
          case FLOAT:
            buf.append(getFloat(i));
            break;
          case DOUBLE:
            buf.append(getDouble(i));
            break;
          case DECIMAL:
            buf.append(getDecimal(i));
            break;
          case BOOL:
            buf.append(getBoolean(i));
            break;
          default:
            buf.append("<unknown type!>");
            break;
        }
      }
    }
    return buf.toString();
  }

  /**
   * @return a string describing the location of this row result within
   * the iterator as well as its data.
   */
  public String toStringLongFormat() {
    StringBuilder buf = new StringBuilder();
    buf.append(this.toString());
    buf.append("{");
    buf.append(rowToString());
    buf.append("}");
    return buf.toString();
  }
}
