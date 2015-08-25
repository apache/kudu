// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

/**
 * Class used to represent parts of row along with its schema.
 *
 * Each PartialRow is backed by an byte array where all the cells (except strings) are written. The
 * strings are kept in a List.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartialRow {

  private final Schema schema;
  // Variable length data. If string, will be UTF-8 encoded.
  private final List<byte[]> varLengthData;
  private final byte[] rowAlloc;
  private final BitSet columnsBitSet;
  private final BitSet nullsBitSet;

  /**
   * This is not a stable API, prefer using {@link Schema#newPartialRow()}
   * to create a new partial row.
   * @param schema the schema to use for this row
   */
  public PartialRow(Schema schema) {
    this.schema = schema;
    this.columnsBitSet = new BitSet(this.schema.getColumnCount());
    this.nullsBitSet = schema.hasNullableColumns() ?
        new BitSet(this.schema.getColumnCount()) : null;
    this.rowAlloc = new byte[schema.getRowSize()];
    this.varLengthData = Lists.newArrayListWithCapacity(schema.getVarLengthColumnCount());
  }

  /**
   * Creates a new partial row by deep-copying the data-fields of the provided partial row.
   * @param row the partial row to copy
   */
  PartialRow(PartialRow row) {
    this.schema = row.schema;

    this.varLengthData = Lists.newArrayListWithCapacity(row.varLengthData.size());
    for (byte[] data: row.varLengthData) {
      this.varLengthData.add(data.clone());
    }

    this.rowAlloc = row.rowAlloc.clone();
    this.columnsBitSet = (BitSet) row.columnsBitSet.clone();
    this.nullsBitSet = row.nullsBitSet == null ? null : (BitSet) row.nullsBitSet.clone();
  }

  /**
   * Add a boolean for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addBoolean(int columnIndex, boolean val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BOOL);
    rowAlloc[getPositionInRowAllocAndSetBitSet(columnIndex)] = (byte) (val ? 1 : 0);
  }

  /**
   * Add a boolean for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addBoolean(String columnName, boolean val) {
    addBoolean(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add a byte for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addByte(int columnIndex, byte val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT8);
    rowAlloc[getPositionInRowAllocAndSetBitSet(columnIndex)] = val;
  }

  /**
   * Add a byte for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addByte(String columnName, byte val) {
    addByte(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add a short for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addShort(int columnIndex, short val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT16);
    Bytes.setShort(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add a short for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addShort(String columnName, short val) {
    addShort(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add an int for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addInt(int columnIndex, int val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT32);
    Bytes.setInt(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an int for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addInt(String columnName, int val) {
    addInt(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add an long for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addLong(int columnIndex, long val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.INT64, Type.TIMESTAMP);
    Bytes.setLong(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an long for the specified column.
   *
   * If this is a TIMESTAMP column, the long value provided should be the number of microseconds
   * between a given time and January 1, 1970 UTC.
   * For example, to encode the current time, use setLong(System.currentTimeMillis() * 1000);
   *
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addLong(String columnName, long val) {
    addLong(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add an float for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addFloat(int columnIndex, float val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.FLOAT);
    Bytes.setFloat(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an float for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addFloat(String columnName, float val) {
    addFloat(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add an double for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addDouble(int columnIndex, double val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.DOUBLE);
    Bytes.setDouble(rowAlloc, val, getPositionInRowAllocAndSetBitSet(columnIndex));
  }

  /**
   * Add an double for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addDouble(String columnName, double val) {
    addDouble(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add a String for the specified column.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addString(int columnIndex, String val) {
    addStringUtf8(columnIndex, Bytes.fromString(val));
  }

  /**
   * Add a String for the specified column.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addString(String columnName, String val) {
    addStringUtf8(columnName, Bytes.fromString(val));
  }

  /**
   * Add a String for the specified value, encoded as UTF8.
   * Note that the provided value must not be mutated after this.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addStringUtf8(int columnIndex, byte[] val) {
    // TODO: use Utf8.isWellFormed from Guava 16 to verify that
    // the user isn't putting in any garbage data.
    checkColumn(schema.getColumnByIndex(columnIndex), Type.STRING);
    addVarLengthData(columnIndex, val);
  }

  /**
   * Add a String for the specified value, encoded as UTF8.
   * Note that the provided value must not be mutated after this.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist or the value doesn't match
   * the column's type
   */
  public void addStringUtf8(String columnName, byte[] val) {
    addStringUtf8(schema.getColumnIndex(columnName), val);
  }

  /**
   * Add binary data with the specified value.
   * Note that the provided value must not be mutated after this.
   * @param columnIndex the column's index in the schema
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   */
  public void addBinary(int columnIndex, byte[] val) {
    checkColumn(schema.getColumnByIndex(columnIndex), Type.BINARY);
    addVarLengthData(columnIndex, val);
  }

  /**
   * Add binary data with the specified value.
   * Note that the provided value must not be mutated after this.
   * @param columnName Name of the column
   * @param val value to add
   * @throws IllegalArgumentException if the column doesn't exist
   */
  public void addBinary(String columnName, byte[] val) {
    addBinary(schema.getColumnIndex(columnName), val);
  }

  private void addVarLengthData(int columnIndex, byte[] val) {
    int index = varLengthData.size();
    varLengthData.add(val);
    // Set the bit and set the usage bit
    int pos = getPositionInRowAllocAndSetBitSet(columnIndex);

    // For now, just store the index of the string.
    // Later, we'll replace this with the offset within the wire buffer
    // before we send it.
    // TODO We don't need to write the length, we could do one of the following:
    // - Remove the string lengths from rowAlloc, or
    // - Modify Operation to single copy rowAlloc instead of copying column by column.
    Bytes.setLong(rowAlloc, index, pos);
    Bytes.setLong(rowAlloc, val.length, pos + Longs.BYTES);
  }

  /**
   * Set the specified column to null
   * @param columnIndex the column's index in the schema
   * @throws IllegalArgumentException if the column doesn't exist or cannot be set to null
   */
  public void setNull(int columnIndex) {
    setNull(this.schema.getColumnByIndex(columnIndex));
  }

  /**
   * Set the specified column to null
   * @param columnName Name of the column
   * @throws IllegalArgumentException if the column doesn't exist or cannot be set to null
   */
  public void setNull(String columnName) {
    setNull(this.schema.getColumn(columnName));
  }

  /**
   * Removes all column values from the row.
   */
  public void reset() {
    this.varLengthData.clear();
    this.columnsBitSet.clear();
    this.nullsBitSet.clear();
  }

  private void setNull(ColumnSchema column) {
    assert nullsBitSet != null;
    checkColumnExists(column);
    if (!column.isNullable()) {
      throw new IllegalArgumentException(column.getName() + " cannot be set to null");
    }
    int idx = schema.getColumns().indexOf(column);
    columnsBitSet.set(idx);
    nullsBitSet.set(idx);
  }

  /**
   * Verifies if the column exists and belongs to one of the specified types
   * It also does some internal accounting
   * @param column column the user wants to set
   * @param types types we expect
   * @throws IllegalArgumentException if the column or type was invalid
   */
  private static void checkColumn(ColumnSchema column, Type... types) {
    checkColumnExists(column);
    for(Type type : types) {
      if (column.getType().equals(type)) return;
    }
    throw new IllegalArgumentException(String.format("%s isn't %s, it's %s", column.getName(),
        Arrays.toString(types), column.getType().getName()));
  }

  /**
   * @param column column the user wants to set
   * @throws IllegalArgumentException if the column doesn't exist
   */
  private static void checkColumnExists(ColumnSchema column) {
    if (column == null)
      throw new IllegalArgumentException("Column name isn't present in the table's schema");
  }

  /**
   * Sets the column bit set for the column index, and returns the column's offset.
   * @param columnIndex the index of the column to get the position for and mark as set
   * @return the offset in rowAlloc for the column
   */
  private int getPositionInRowAllocAndSetBitSet(int columnIndex) {
    columnsBitSet.set(columnIndex);
    return schema.getColumnOffset(columnIndex);
  }

  /**
   * Tells if the specified column was set by the user
   * @param column column's index in the schema
   * @return true if it was set, else false
   */
  boolean isSet(int column) {
    return this.columnsBitSet.get(column);
  }

  /**
   * Tells if the specified column was set to null by the user
   * @param column column's index in the schema
   * @return true if it was set, else false
   */
  boolean isSetToNull(int column) {
    if (this.nullsBitSet == null) {
      return false;
    }
    return this.nullsBitSet.get(column);
  }

  /**
   * Returns the encoded primary key of the row.
   * @return a byte array containing an encoded primary key
   */
  public byte[] encodePrimaryKey() {
    return new KeyEncoder().encodePrimaryKey(this);
  }

  /**
   * Get the schema used for this row.
   * @return a schema that came from KuduTable
   */
  Schema getSchema() {
    return schema;
  }

  /**
   * Get the list variable length data cells that were added to this row.
   * @return a list of binary data, may be empty
   */
  List<byte[]> getVarLengthData() {
    return varLengthData;
  }

  /**
   * Get the byte array that contains all the data added to this partial row. Variable length data
   * is contained separately, see {@link #getVarLengthData()}. In their place you'll find their
   * index in that list and their size.
   * @return a byte array containing the data for this row, except strings
   */
  byte[] getRowAlloc() {
    return rowAlloc;
  }

  /**
   * Get the bit set that indicates which columns were set.
   * @return a bit set for columns with data
   */
  BitSet getColumnsBitSet() {
    return columnsBitSet;
  }

  /**
   * Get the bit set for the columns that were specifically set to null
   * @return a bit set for null columns
   */
  BitSet getNullsBitSet() {
    return nullsBitSet;
  }
}
