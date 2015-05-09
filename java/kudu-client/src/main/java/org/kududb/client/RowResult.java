// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.util.Slice;

import java.math.BigInteger;
import java.util.BitSet;

/**
 * RowResult represents one row from a scanner. Do not reuse or store the objects.
 */
public class RowResult {

  private static final int INDEX_RESET_LOCATION = -1;
  private int index = INDEX_RESET_LOCATION;
  private int offset;
  private BitSet nullsBitSet;
  private final int rowSize;
  private final int[] columnOffsets;
  private final Schema schema;
  private final Slice rowData;
  private final Slice indirectData;

  /**
   * Prepares the row representation using the provided data. Doesn't copy data
   * out of the byte arrays. Package private.
   * @param schema Schema used to build the rowData
   * @param rowData The Slice of data returned by the tablet server
   * @param indirectData The full indirect data that contains the strings
   */
  RowResult(Schema schema, Slice rowData, Slice indirectData) {
    this.schema = schema;
    this.rowData = rowData;
    this.indirectData = indirectData;
    int columnOffsetsSize = schema.getColumnCount();
    if (schema.hasNullableColumns()) {
      columnOffsetsSize++;
    }
    this.rowSize = this.schema.getRowSize();
    columnOffsets = new int[columnOffsetsSize];
    // Empty projection, usually used for quick row counting
    if (columnOffsetsSize == 0) {
      return;
    }
    int currentOffset = 0;
    columnOffsets[0] = currentOffset;
    // Pre-compute the columns offsets in rowData for easier lookups later
    // If the schema has nullables, we also add the offset for the null bitmap at the end
    for (int i = 1; i < columnOffsetsSize; i++) {
      int previousSize = schema.getColumn(i-1).getType().getSize();
      columnOffsets[i] = previousSize + currentOffset;
      currentOffset += previousSize;
    }
  }

  /**
   * Package-protected, only meant to be used by the RowResultIterator
   */
  void advancePointer() {
    advancePointerTo(this.index + 1);
  }

  void resetPointer() {
    advancePointerTo(INDEX_RESET_LOCATION);
  }

  void advancePointerTo(int rowIndex) {
    this.index = rowIndex;
    this.offset = this.rowSize * this.index;
    if (schema.hasNullableColumns() && this.index != INDEX_RESET_LOCATION) {
      this.nullsBitSet = Bytes.toBitSet(
          this.rowData.getRawArray(),
          this.rowData.getRawOffset()
          + getCurrentRowDataOffsetForColumn(schema.getColumnCount()),
          schema.getColumnCount());
    }
  }

  int getCurrentRowDataOffsetForColumn(int columnIndex) {
    return this.offset + this.columnOffsets[columnIndex];
  }

  /**
   * Get the specified column's positive integer
   * @param columnIndex Column index in the schema
   * @return A positive integer
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public long getUnsignedInt(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getUnsignedInt(this.rowData.getRawArray(),
                                this.rowData.getRawOffset()
                                + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's integer
   * @param columnIndex Column index in the schema
   * @return An integer
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public int getInt(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getInt(this.rowData.getRawArray(),
                        this.rowData.getRawOffset()
                        + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's positive short
   * @param columnIndex Column index in the schema
   * @return A positive short
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public int getUnsignedShort(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getUnsignedShort(this.rowData.getRawArray(),
                                  this.rowData.getRawOffset()
                                  + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's short
   * @param columnIndex Column index in the schema
   * @return A short
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public short getShort(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getShort(this.rowData.getRawArray(),
                          this.rowData.getRawOffset()
                          + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's positive byte
   * @param columnIndex Column index in the schema
   * @return A positive byte
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public short getUnsignedByte(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getUnsignedByte(this.rowData.getRawArray(),
                                 this.rowData.getRawOffset()
                                 + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's boolean
   * @param columnIndex Column index in the schema
   * @return A boolean
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public boolean getBoolean(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    byte b = Bytes.getByte(this.rowData.getRawArray(),
                         this.rowData.getRawOffset()
                         + getCurrentRowDataOffsetForColumn(columnIndex));
    return b == 1;
  }

  /**
   * Get the specified column's byte
   * @param columnIndex Column index in the schema
   * @return A byte
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public byte getByte(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getByte(this.rowData.getRawArray(),
                         this.rowData.getRawOffset()
                         + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's long
   * @param columnIndex Column index in the schema
   * @return A positive long
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public long getLong(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getLong(this.rowData.getRawArray(),
                         this.rowData.getRawOffset()
                         + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's long
   * @param columnIndex Column index in the schema
   * @return A positive long
   */
  public BigInteger getUnsignedLong(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getUnsignedLong(this.rowData.getRawArray(),
                                 this.rowData.getRawOffset()
                                 + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's float
   * @param columnIndex Column index in the schema
   * @return A float
   */
  public float getFloat(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getFloat(this.rowData.getRawArray(),
                          this.rowData.getRawOffset()
                          + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the specified column's double
   * @param columnIndex Column index in the schema
   * @return A double
   */
  public double getDouble(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    return Bytes.getDouble(this.rowData.getRawArray(),
                           this.rowData.getRawOffset()
                           + getCurrentRowDataOffsetForColumn(columnIndex));
  }

  /**
   * Get the schema used for this scanner's column projection.
   * @return A column projection as a schema.
   */
  public Schema getColumnProjection() {
    return this.schema;
  }

  /**
   * Get the specified column's string. Read from the indirect data
   * @param columnIndex Column index in the schema
   * @return A string
   * @throws IllegalArgumentException if the column is null
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public String getString(int columnIndex) {
    checkValidColumn(columnIndex);
    checkNull(columnIndex);
    // C++ puts a Slice in rowData which is 16 bytes long for simplity, but we only support ints
    long offset = getLong(columnIndex);
    long length = rowData.getLong(getCurrentRowDataOffsetForColumn(columnIndex) + 8);
    assert offset < Integer.MAX_VALUE;
    assert length < Integer.MAX_VALUE;
    return Bytes.getString(indirectData.getRawArray(),
                           indirectData.getRawOffset() + (int)offset,
                           (int)length);
  }

  /**
   * Get if the specified column is NULL
   * @param columnIndex Column index in the schema
   * @return true if the column cell is null and the column is nullable,
   * false otherwise
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  public boolean isNull(int columnIndex) {
    checkValidColumn(columnIndex);
    if (nullsBitSet == null) {
      return false;
    }
    return schema.getColumn(columnIndex).isNullable()
        && nullsBitSet.get(columnIndex);
  }

  /**
   * @throws IndexOutOfBoundsException if the column doesn't exist
   */
  private void checkValidColumn(int columnIndex) {
    if (columnIndex >= schema.getColumnCount()) {
      throw new IndexOutOfBoundsException("Requested column is out of range, " +
          columnIndex + " out of " + schema.getColumnCount());
    }
  }

  /**
   * @throws IllegalArgumentException if the column is null
   */
  private void checkNull(int columnIndex) {
    if (!schema.hasNullableColumns()) {
      return;
    }
    if (isNull(columnIndex)) {
      throw new IllegalArgumentException("The requested column (" + columnIndex + ")  is null");
    }
  }

  @Override
  public String toString() {
    return "RowResult index: " + this.index + ", size: " + this.rowSize + ", " +
        "schema: " + this.schema;
  }

  /**
   * Return the actual data from this row in a stringified key=value
   * form.
   */
  public String rowToString() {
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumn(i);
      if (i != 0) {
        buf.append(", ");
      }
      buf.append(col.getType().name());
      buf.append(" ").append(col.getName()).append("=");
      if (isNull(i)) {
        buf.append("NULL");
      } else if (col.getType().equals(Type.INT8)) {
        buf.append(getByte(i));
      } else if (col.getType().equals(Type.INT16)) {
        buf.append(getShort(i));
      } else if (col.getType().equals(Type.INT32)) {
        buf.append(getInt(i));
      } else if (col.getType().equals(Type.INT64)) {
        buf.append(getLong(i));
      } else if (col.getType().equals(Type.STRING)) {
        buf.append(getString(i));
      } else if (col.getType().equals(Type.FLOAT)) {
        buf.append(getFloat(i));
      } else if (col.getType().equals(Type.DOUBLE)) {
        buf.append(getDouble(i));
      } else {
        buf.append("<unknown type!>");
      }
    }
    return buf.toString();
  }

  /**
   * @return a string describing the location of this row result within
   * the iterator as well as its data.
   */
  public String toStringLongFormat() {
    StringBuffer buf = new StringBuffer(this.rowSize); // super rough estimation
    buf.append(this.toString());
    buf.append("{");
    buf.append(rowToString());
    buf.append("}");
    return buf.toString();
  }

}
