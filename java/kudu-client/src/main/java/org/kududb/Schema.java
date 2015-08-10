// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb;

import org.kududb.client.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents table's schema which is essentially a list of columns.
 * This class offers a few utility methods for querying it.
 */
public class Schema {

  private final List<ColumnSchema> columns;
  private final Map<String, ColumnSchema> columnsMap;
  private final int[] columnOffsets;
  private final int varLengthColumnCount;
  private final int rowSize;
  private final int keysCount;
  private final boolean hasNullableColumns;

  /**
   * Constructs a schema using the specified columns and does some internal accounting
   * @param columns
   * @throws IllegalArgumentException If the key columns aren't specified first
   * @see src/kudu/common/wire_protocol.cc in ColumnPBsToSchema()
   */
  public Schema(List<ColumnSchema> columns) {
    this.columns = columns;
    int varlencnt = 0;
    int keycnt = 0;
    this.columnOffsets = new int[columns.size()];
    this.columnsMap = new HashMap<String, ColumnSchema>(columns.size());
    int pos = 0;
    int i = 0;
    boolean hasNulls = false;
    boolean isHandlingKeys = true;
    // pre-compute a few counts and offsets
    for (ColumnSchema col : this.columns) {
      if (col.isKey()) {
        if (!isHandlingKeys) {
          throw new IllegalArgumentException("Got out-of-order key column " + col);
        }
        keycnt++;
      } else {
        isHandlingKeys = false;
      }
      if (col.isNullable()) {
        hasNulls = true;
      }
      this.columnsMap.put(col.getName(), col);
      columnOffsets[i] = pos;
      pos += col.getType().getSize();
      if (col.getType() == Type.STRING || col.getType() == Type.BINARY) {
        varlencnt++;
      }
      i++;
    }
    this.hasNullableColumns = hasNulls;
    this.varLengthColumnCount = varlencnt;
    this.keysCount = keycnt;
    this.rowSize = getRowSize(this);
  }

  /**
   * Get the list of columns used to create this schema
   * @return list of columns
   */
  public List<ColumnSchema> getColumns() {
    return this.columns;
  }

  /**
   * Get the count of columns with variable length (BINARY/STRING) in
   * this schema.
   * @return strings count
   */
  public int getVarLengthColumnCount() {
    return this.varLengthColumnCount;
  }

  /**
   * Get the size a row built using this schema would be
   * @return size in bytes
   */
  public int getRowSize() {
    return this.rowSize;
  }

  /**
   * Get the index at which this column can be found in the backing byte array
   * @param idx column's index
   * @return column's offset
   */
  public int getColumnOffset(int idx) {
    return this.columnOffsets[idx];
  }

  /**
   * Get the index for the provided column based
   * @param column column to search for
   * @return an index in the schema
   */
  public int getColumnIndex(ColumnSchema column) {
    return this.columns.indexOf(column);
  }

  /**
   * Get the column at the specified index in the original list
   * @param idx column's index
   * @return the column
   */
  public ColumnSchema getColumn(int idx) {
    return this.columns.get(idx);
  }

  /**
   * Get the column associated with the specified name
   * @param columnName column's name
   * @return the column
   */
  public ColumnSchema getColumn(String columnName) {
    return this.columnsMap.get(columnName);
  }

  /**
   * Get the count of columns in this schema
   * @return count of columns
   */
  public int getColumnCount() {
    return this.columns.size();
  }

  /**
   * Get the count of columns that are part of the keys
   * @return count of keys
   */
  public int getKeysCount() {
    return this.keysCount;
  }

  /**
   * Get a schema that only contains the columns which are part of the key
   * @return new schema with only the keys
   */
  public Schema getRowKeyProjection() {
    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(this.keysCount);
    for (ColumnSchema col : this.columns) {
      if (col.isKey()) {
        columns.add(col);
      }
    }
    return new Schema(columns);
  }


  /**
   * Tells if there's at least one nullable column
   * @return true if at least one column is nullable, else false.
   */
  public boolean hasNullableColumns() {
    return this.hasNullableColumns;
  }

  /**
   * Gives the size in bytes for a single row given the specified schema
   * @param schema row's schema
   * @return row size in bytes
   */
  public static int getRowSize(Schema schema) {
    int totalSize = 0;
    for (ColumnSchema column : schema.getColumns()) {
      totalSize += column.getType().getSize();
    }
    if (schema.hasNullableColumns()) {
      totalSize += Bytes.getBitSetSize(schema.getColumnCount());
    }
    return totalSize;
  }
}
