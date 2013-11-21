// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;

/**
 * RowResult represents one row from a scanner. Do not reuse or store the objects.
 */
public class RowResult {

  private int index = -1;
  private int offset;
  private final int rowSize;
  private final int[] columnOffsets;
  private final Schema schema;
  private final byte[] rowData;
  private final byte[] indirectData;

  /**
   * Prepares the row representation using the provided data. Doesn't copy data
   * out of the byte arrays. Package private.
   * @param schema Schema used to build the rowData
   * @param rowData The full data returned by the tabletSlice server
   * @param indirectData The full indirect data that contains the strings
   */
  RowResult(Schema schema, byte[] rowData, byte[] indirectData) {
    this.schema = schema;
    this.rowSize = this.schema.getRowSize();
    //this.offset = this.rowSize * this.index;
    this.rowData = rowData;
    this.indirectData = indirectData;
    columnOffsets = new int[schema.getColumnCount()];
    int currentOffset = 0;
    columnOffsets[0] = currentOffset;
    // Pre-compute the columns offsets in rowData for easier lookups later
    for (int i = 1; i < schema.getColumnCount(); i++) {
      int previousSize = schema.getColumn(i-1).getType().getSize();
      columnOffsets[i] = previousSize + currentOffset;
      currentOffset += previousSize;
    }
  }

  /**
   * Package-protected, only meant to be used by the RowResultIterator
   */
  void advancePointer() {
    this.index++;
    this.offset = this.rowSize * this.index;
  }

  /**
   * Get the specified column's positive integer
   * @param columnIndex Column index in the schema
   * @return A positive integer
   */
  public long getUnsignedInt(int columnIndex) {
    return Bytes.getUnsignedInt(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's integer
   * @param columnIndex Column index in the schema
   * @return An integer
   */
  public int getInt(int columnIndex) {
    return Bytes.getInt(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's positive short
   * @param columnIndex Column index in the schema
   * @return A positive short
   */
  public int getUnsignedShort(int columnIndex) {
    return Bytes.getUnsignedShort(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's short
   * @param columnIndex Column index in the schema
   * @return A short
   */
  public short getShort(int columnIndex) {
    return Bytes.getShort(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's positive byte
   * @param columnIndex Column index in the schema
   * @return A positive byte
   */
  public int getUnsignedByte(int columnIndex) {
    return Bytes.getUnsignedByte(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's byte
   * @param columnIndex Column index in the schema
   * @return A byte
   */
  public byte getByte(int columnIndex) {
    return Bytes.getByte(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's long
   * @param columnIndex Column index in the schema
   * @return A positive long
   */
  public long getLong(int columnIndex) {
    return Bytes.getLong(this.rowData, this.offset + this.columnOffsets[columnIndex]);
  }

  /**
   * Get the specified column's string. Read from the indirect data
   * @param columnIndex Column index in the schema
   * @return A string
   */
  public String getString(int columnIndex) {
    // TODO figure the long/int mess
    int offset = (int)getLong(columnIndex);
    int length = (int)Bytes.getLong(rowData, this.offset + this.columnOffsets[columnIndex] + 8);
    return new String(indirectData, offset, length);
  }

  @Override
  public String toString() {
    return "RowResult index: " + this.index + ", size: " + this.rowSize + ", " +
        "schema: " + this.schema;
  }

  /**
   *
   * @return
   */
  public String toStringLongFormat() {
    StringBuffer buf = new StringBuffer(this.rowSize); // super rough estimation
    buf.append(this.toString());
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumn(i);
      buf.append(", ");
      buf.append(col.getName());
      buf.append(": {");
      if (col.getType().equals(Type.UINT8)) {
        buf.append(getUnsignedByte(i));
      } else if (col.getType().equals(Type.INT8)) {
        buf.append(getByte(i));
      } else if (col.getType().equals(Type.UINT16)) {
        buf.append(getUnsignedShort(i));
      } else if (col.getType().equals(Type.INT16)) {
        buf.append(getShort(i));
      } else if (col.getType().equals(Type.UINT32)) {
        buf.append(getUnsignedInt(i));
      } else if (col.getType().equals(Type.INT32)) {
        buf.append(getInt(i));
      } else if (col.getType().equals(Type.INT64)) {
        buf.append(getLong(i));
      } else if (col.getType().equals(Type.STRING)) {
        buf.append(getString(i));
      }
      buf.append("}");
    }
    return buf.toString();
  }

}
