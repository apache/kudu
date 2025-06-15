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
import java.sql.Timestamp;
import java.util.BitSet;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.Slice;
import org.apache.kudu.util.TimestampUtil;

/**
 * RowResult represents one row from a scanner, in row-wise layout.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class RowwiseRowResult extends RowResult {

  private final Slice rowData;
  private final Slice indirectData;

  private final int rowSize;
  private final int[] columnOffsets;

  private int offset;
  private BitSet nullsBitSet;

  /**
   * Prepares the row representation using the provided data. Doesn't copy data
   * out of the byte arrays. Package private.
   * @param schema Schema used to build the rowData
   * @param rowData The Slice of data returned by the tablet server
   * @param indirectData The full indirect data that contains the strings
   * @param rowIndex The index of the row in the rowData that this RowResult represents
   */
  RowwiseRowResult(Schema schema, Slice rowData, Slice indirectData, int rowIndex) {
    super(schema, rowIndex);
    this.rowData = rowData;
    this.indirectData = indirectData;
    this.rowSize = this.schema.getRowSize();

    int columnOffsetsSize = schema.getColumnCount();
    if (schema.hasNullableColumns()) {
      columnOffsetsSize++;
    }
    columnOffsets = new int[columnOffsetsSize];
    // Empty projection, usually used for quick row counting.
    if (columnOffsetsSize == 0) {
      return;
    }
    int currentOffset = 0;
    columnOffsets[0] = currentOffset;
    // Pre-compute the columns offsets in rowData for easier lookups later.
    // If the schema has nullables, we also add the offset for the null bitmap at the end.
    for (int i = 1; i < columnOffsetsSize; i++) {
      org.apache.kudu.ColumnSchema column = schema.getColumnByIndex(i - 1);
      int previousSize = column.getTypeSize();
      columnOffsets[i] = previousSize + currentOffset;
      currentOffset += previousSize;
    }
    advancePointerTo(rowIndex);
  }

  /**
   * Package-protected, only meant to be used by the RowResultIterator
   */
  @Override
  void advancePointerTo(int rowIndex) {
    super.advancePointerTo(rowIndex);

    this.offset = this.rowSize * this.index;
    if (schema.hasNullableColumns() && this.index != INDEX_RESET_LOCATION) {
      this.nullsBitSet = Bytes.toBitSet(
          this.rowData.getRawArray(),
          this.rowData.getRawOffset() +
              getCurrentRowDataOffsetForColumn(schema.getColumnCount()),
          schema.getColumnCount());
    }
  }

  int getCurrentRowDataOffsetForColumn(int columnIndex) {
    return this.offset + this.columnOffsets[columnIndex];
  }

  /**
   * Get the specified column's integer
   * @param columnIndex Column index in the schema
   * @return an integer
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public int getInt(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.INT32, Type.DATE);
    return Bytes.getInt(this.rowData.getRawArray(),
        this.rowData.getRawOffset() + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's short
   * @param columnIndex Column index in the schema
   * @return a short
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public short getShort(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.INT16);
    return Bytes.getShort(this.rowData.getRawArray(),
        this.rowData.getRawOffset() + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's boolean
   * @param columnIndex Column index in the schema
   * @return a boolean
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public boolean getBoolean(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.BOOL);
    byte b = Bytes.getByte(this.rowData.getRawArray(),
                         this.rowData.getRawOffset() +
                             getCurrentRowDataOffsetForColumn(columnIndex));
    return b == 1;
  }

  /**
   * Get the specified column's byte
   * @param columnIndex Column index in the schema
   * @return a byte
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public byte getByte(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.INT8);
    return Bytes.getByte(this.rowData.getRawArray(),
        this.rowData.getRawOffset() + getCurrentRowDataOffsetForColumn(columnIndex));
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
  @Override
  public long getLong(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.INT64, Type.UNIXTIME_MICROS);
    return Bytes.getLong(this.rowData.getRawArray(),
            this.rowData.getRawOffset() + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's float
   * @param columnIndex Column index in the schema
   * @return a float
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public float getFloat(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.FLOAT);
    return Bytes.getFloat(this.rowData.getRawArray(),
                          this.rowData.getRawOffset() +
                              getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's double
   * @param columnIndex Column index in the schema
   * @return a double
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public double getDouble(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.DOUBLE);
    return Bytes.getDouble(this.rowData.getRawArray(),
                           this.rowData.getRawOffset() +
                               getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's Decimal.
   *
   * @param columnIndex Column index in the schema
   * @return a BigDecimal.
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public BigDecimal getDecimal(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.DECIMAL);
    ColumnSchema column = schema.getColumnByIndex(columnIndex);
    ColumnTypeAttributes typeAttributes = column.getTypeAttributes();
    return Bytes.getDecimal(this.rowData.getRawArray(),
        this.rowData.getRawOffset() + getCurrentRowDataOffsetForColumn(columnIndex),
            typeAttributes.getPrecision(), typeAttributes.getScale());
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
  @Override
  public Timestamp getTimestamp(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.UNIXTIME_MICROS);
    long micros = getLong(columnIndex);
    return TimestampUtil.microsToTimestamp(micros);
  }

  @Override
  public String getVarLengthData(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.STRING, Type.VARCHAR);
    // C++ puts a Slice in rowData which is 16 bytes long for simplicity, but we only support ints.
    long offset = getOffset(columnIndex);
    long length = rowData.getLong(getCurrentRowDataOffsetForColumn(columnIndex) + 8);
    assert offset < Integer.MAX_VALUE;
    assert length < Integer.MAX_VALUE;
    return Bytes.getString(indirectData.getRawArray(),
            indirectData.getRawOffset() + (int)offset,
            (int)length);
  }

  /**
   * Get a copy of the specified column's binary data.
   * @param columnIndex Column index in the schema
   * @return a byte[] with the binary data.
   * @throws IllegalArgumentException if the column is null
   * or if the type doesn't match the column's type
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public byte[] getBinaryCopy(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    // C++ puts a Slice in rowData which is 16 bytes long for simplicity,
    // but we only support ints.
    long offset = getOffset(columnIndex);
    long length = rowData.getLong(getCurrentRowDataOffsetForColumn(columnIndex) + 8);
    assert offset < Integer.MAX_VALUE;
    assert length < Integer.MAX_VALUE;
    byte[] ret = new byte[(int)length];
    System.arraycopy(indirectData.getRawArray(), indirectData.getRawOffset() + (int) offset,
                     ret, 0, (int) length);
    return ret;
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
  @Override
  public ByteBuffer getBinary(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    checkType(columnIndex, Type.BINARY);
    // C++ puts a Slice in rowData which is 16 bytes long for simplicity,
    // but we only support ints.
    long offset = getOffset(columnIndex);
    long length = rowData.getLong(getCurrentRowDataOffsetForColumn(columnIndex) + 8);
    assert offset < Integer.MAX_VALUE;
    assert length < Integer.MAX_VALUE;
    return ByteBuffer.wrap(indirectData.getRawArray(), indirectData.getRawOffset() + (int) offset,
        (int) length);
  }

  /**
   * Returns the long column value if the column type is INT64 or UNIXTIME_MICROS.
   * Returns the column's offset into the indirectData if the column type is BINARY or STRING.
   * @param columnIndex Column index in the schema
   * @return a long value for the column
   */
  long getOffset(int columnIndex) {
    return Bytes.getLong(this.rowData.getRawArray(),
            this.rowData.getRawOffset() +
                    getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get if the specified column is NULL
   * @param columnIndex Column index in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  @Override
  public boolean isNull(int columnIndex) {
    checkValidColumn(columnIndex);
    if (nullsBitSet == null) {
      return false;
    }
    return schema.getColumnByIndex(columnIndex).isNullable() &&
        nullsBitSet.get(columnIndex);
  }


  @Override
  public String toString() {
    return "RowResult(Rowwise) index: " + this.index + ", size: " + this.rowSize;
  }

}
