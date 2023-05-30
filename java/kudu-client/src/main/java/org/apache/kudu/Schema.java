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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Common.DataType;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.PartialRow;

/**
 * Represents table's schema which is essentially a list of columns.
 * This class offers a few utility methods for querying it.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Schema {

  /*
   * Column name and type of auto_incrementing_id column, which is added by Kudu engine
   * automatically if the primary key is not unique.
   */
  private static final String AUTO_INCREMENTING_ID_COL_NAME = "auto_incrementing_id";
  private static final Type AUTO_INCREMENTING_ID_COL_TYPE = Type.INT64;

  /**
   * Mapping of column index to column.
   */
  private final List<ColumnSchema> columnsByIndex;

  /**
   * The primary key columns.
   */
  private final List<ColumnSchema> primaryKeyColumns = new ArrayList<>();

  /**
   * Mapping of column name to index.
   */
  private final Map<String, Integer> columnsByName;

  /**
   * Mapping of column ID to index, or null if the schema does not have assigned column IDs.
   */
  private final Map<Integer, Integer> columnsById;

  /**
   * Mapping of column name to column ID, or null if the schema does not have assigned column IDs.
   */
  private final Map<String, Integer> columnIdByName;

  /**
   * Mapping of column index to backing byte array offset.
   */
  private final int[] columnOffsets;

  private final int varLengthColumnCount;
  private final int rowSize;
  private final boolean isKeyUnique;
  private final boolean hasNullableColumns;
  private final boolean hasImmutableColumns;
  private final boolean hasAutoIncrementingColumn;

  private final int isDeletedIndex;
  private static final int NO_IS_DELETED_INDEX = -1;

  /**
   * Constructs a schema using the specified columns and does some internal accounting
   *
   * @param columns the columns in index order
   *
   * See {@code ColumnPBsToSchema()} in {@code src/kudu/common/wire_protocol.cc}
   */
  public Schema(List<ColumnSchema> columns) {
    this(columns, null);
  }

  /**
   * Constructs a schema using the specified columns and IDs.
   *
   * This is not a stable API, prefer using {@link Schema#Schema(List)} to create a new schema.
   *
   * @param columns the columns in index order
   * @param columnIds the column ids of the provided columns, or null
   * @throws IllegalArgumentException If the column ids length does not match the columns length
   *
   * See {@code ColumnPBsToSchema()} in {@code src/kudu/common/wire_protocol.cc}
   */
  public Schema(List<ColumnSchema> columns, List<Integer> columnIds) {
    boolean hasColumnIds = columnIds != null;
    if (hasColumnIds && columns.size() != columnIds.size()) {
      throw new IllegalArgumentException(
          "Schema must be constructed with all column IDs, or none.");
    }

    boolean isKeyFound = false;
    boolean isKeyUnique = false;
    boolean hasAutoIncrementing = false;
    int keyColumnCount = 0;
    int maxColumnId = Integer.MIN_VALUE;
    // Check if auto-incrementing column should be added into the input columns list
    for (int index = 0; index < columns.size(); index++) {
      final ColumnSchema column = columns.get(index);
      if (column.isKey()) {
        keyColumnCount++;
        if (!isKeyFound) {
          isKeyFound = true;
          isKeyUnique = column.isKeyUnique();
        } else if (isKeyUnique != column.isKeyUnique()) {
          throw new IllegalArgumentException(
              "Mixture of unique key and non unique key in a table");
        }
      }
      if (column.isAutoIncrementing()) {
        if (!hasAutoIncrementing) {
          hasAutoIncrementing = true;
        } else {
          throw new IllegalArgumentException(
              "More than one columns are set as auto-incrementing columns");
        }
      }
      if (hasColumnIds && maxColumnId < columnIds.get(index).intValue()) {
        maxColumnId = columnIds.get(index).intValue();
      }
    }
    // Add auto-incrementing column into input columns list if the primary key is not
    // unique and auto-incrementing column has not been created.
    if (keyColumnCount > 0 && !isKeyUnique && !hasAutoIncrementing) {
      // Build auto-incrementing column
      ColumnSchema autoIncrementingColumn =
          new ColumnSchema.AutoIncrementingColumnSchemaBuilder().build();
      // Make a copy of mutable list of columns, then add an auto-incrementing
      // column after the columns marked as key columns.
      columns = new ArrayList<>(columns);
      Preconditions.checkNotNull(columns);
      columns.add(keyColumnCount, autoIncrementingColumn);
      if (hasColumnIds) {
        columnIds = new ArrayList<>(columnIds);
        columnIds.add(keyColumnCount, maxColumnId + 1);
      }
      hasAutoIncrementing = true;
    }

    this.columnsByIndex = ImmutableList.copyOf(columns);
    int varLenCnt = 0;
    this.columnOffsets = new int[columns.size()];
    this.columnsByName = new HashMap<>(columns.size());
    this.columnsById = hasColumnIds ? new HashMap<>(columnIds.size()) : null;
    this.columnIdByName = hasColumnIds ? new HashMap<>(columnIds.size()) : null;
    int offset = 0;
    boolean hasNulls = false;
    boolean hasImmutables = false;
    int isDeletedIndex = NO_IS_DELETED_INDEX;
    // pre-compute a few counts and offsets
    for (int index = 0; index < columns.size(); index++) {
      final ColumnSchema column = columns.get(index);
      if (column.isKey()) {
        primaryKeyColumns.add(column);
      }

      hasNulls |= column.isNullable();
      hasImmutables |= column.isImmutable();
      columnOffsets[index] = offset;
      offset += column.getTypeSize();
      if (this.columnsByName.put(column.getName(), index) != null) {
        throw new IllegalArgumentException(
            String.format("Column names must be unique: %s", columns));
      }
      if (column.getType() == Type.STRING || column.getType() == Type.BINARY) {
        varLenCnt++;
      }

      if (hasColumnIds) {
        if (this.columnsById.put(columnIds.get(index), index) != null) {
          throw new IllegalArgumentException(
              String.format("Column IDs must be unique: %s", columnIds));
        }
        if (this.columnIdByName.put(column.getName(), columnIds.get(index)) != null) {
          throw new IllegalArgumentException(
              String.format("Column names must be unique: %s", columnIds));
        }
      }

      // If this is the IS_DELETED virtual column, set `hasIsDeleted` and `isDeletedIndex`.
      if (column.getWireType() == DataType.IS_DELETED) {
        isDeletedIndex = index;
      }
    }

    this.varLengthColumnCount = varLenCnt;
    this.rowSize = getRowSize(this.columnsByIndex);
    this.isKeyUnique = isKeyUnique;
    this.hasNullableColumns = hasNulls;
    this.hasImmutableColumns = hasImmutables;
    this.hasAutoIncrementingColumn = hasAutoIncrementing;
    this.isDeletedIndex = isDeletedIndex;
  }

  /**
   * Get the list of columns used to create this schema
   * @return list of columns
   */
  public List<ColumnSchema> getColumns() {
    return this.columnsByIndex;
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
   * Gives the size in bytes for a single row given the specified schema
   * @param columns the row's columns
   * @return row size in bytes
   */
  private static int getRowSize(List<ColumnSchema> columns) {
    int totalSize = 0;
    boolean hasNullables = false;
    for (ColumnSchema column : columns) {
      totalSize += column.getTypeSize();
      hasNullables |= column.isNullable();
    }
    if (hasNullables) {
      totalSize += Bytes.getBitSetSize(columns.size());
    }
    return totalSize;
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
   * Returns true if the column exists.
   * @param columnName column to search for
   * @return true if the column exists
   */
  public boolean hasColumn(String columnName) {
    return this.columnsByName.containsKey(columnName);
  }

  /**
   * Get the index for the provided column name.
   * @param columnName column to search for
   * @return an index in the schema
   */
  public int getColumnIndex(String columnName) {
    Integer index = this.columnsByName.get(columnName);
    if (index == null) {
      throw new IllegalArgumentException(
          String.format("Unknown column: %s", columnName));
    }
    return index;
  }

  /**
   * Get the column index of the column with the provided ID.
   * This method is not part of the stable API.
   * @param columnId the column id of the column
   * @return the column index of the column.
   */
  public int getColumnIndex(int columnId) {
    if (!hasColumnIds()) {
      throw new IllegalStateException("Schema does not have Column IDs");
    }
    Integer index = this.columnsById.get(columnId);
    if (index == null) {
      throw new IllegalArgumentException(
          String.format("Unknown column id: %s", columnId));
    }
    return index;
  }

  /**
   * Get the column at the specified index in the original list
   * @param idx column's index
   * @return the column
   */
  public ColumnSchema getColumnByIndex(int idx) {
    return this.columnsByIndex.get(idx);
  }

  /**
   * Get the column associated with the specified name
   * @param columnName column's name
   * @return the column
   */
  public ColumnSchema getColumn(String columnName) {
    return columnsByIndex.get(getColumnIndex(columnName));
  }

  /**
   * Get the count of columns in this schema
   * @return count of columns
   */
  public int getColumnCount() {
    return this.columnsByIndex.size();
  }

  /**
   * Get the count of columns that are part of the primary key.
   * @return count of primary key columns.
   */
  public int getPrimaryKeyColumnCount() {
    return this.primaryKeyColumns.size();
  }

  /**
   * Get the primary key columns.
   * @return the primary key columns.
   */
  public List<ColumnSchema> getPrimaryKeyColumns() {
    return primaryKeyColumns;
  }

  /**
   * Answers if the primary key is unique for the table
   * @return true if the key is unique
   */
  public boolean isPrimaryKeyUnique() {
    return this.isKeyUnique;
  }

  /**
   * Tells if there's auto-incrementing column
   * @return true if there's auto-incrementing column, else false.
   */
  public boolean hasAutoIncrementingColumn() {
    return this.hasAutoIncrementingColumn;
  }

  /**
   * Get the name of the auto-incrementing column
   * @return column name of the auto-incrementing column.
   */
  public static String getAutoIncrementingColumnName() {
    return AUTO_INCREMENTING_ID_COL_NAME;
  }

  /**
   * Get the type of the auto-incrementing column
   * @return type of the auto-incrementing column.
   */
  public static Type getAutoIncrementingColumnType() {
    return AUTO_INCREMENTING_ID_COL_TYPE;
  }

  /**
   * Get a schema that only contains the columns which are part of the key
   * @return new schema with only the keys
   */
  public Schema getRowKeyProjection() {
    return new Schema(primaryKeyColumns);
  }

  /**
   * Tells if there's at least one nullable column
   * @return true if at least one column is nullable, else false.
   */
  public boolean hasNullableColumns() {
    return this.hasNullableColumns;
  }

  /**
   * Tells if there's at least one immutable column
   * @return true if at least one column is immutable, else false.
   */
  public boolean hasImmutableColumns() {
    return this.hasImmutableColumns;
  }

  /**
   * Tells whether this schema includes IDs for columns. A schema created by a client as part of
   * table creation will not include IDs, but schemas for open tables will include IDs.
   * This method is not part of the stable API.
   *
   * @return whether this schema includes column IDs.
   */
  public boolean hasColumnIds() {
    return columnsById != null;
  }

  /**
   * Get the internal column ID for a column name.
   * @param columnName column's name
   * @return the column ID
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public int getColumnId(String columnName) {
    return columnIdByName.get(columnName);
  }

  /**
   * Creates a new partial row for the schema.
   * @return a new partial row
   */
  public PartialRow newPartialRow() {
    return new PartialRow(this);
  }

  /**
   * @return true if the schema has the IS_DELETED virtual column
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public boolean hasIsDeleted() {
    return isDeletedIndex != NO_IS_DELETED_INDEX;
  }

  /**
   * @return the index of the IS_DELETED virtual column
   * @throws IllegalStateException if no IS_DELETED virtual column exists
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public int getIsDeletedIndex() {
    Preconditions.checkState(hasIsDeleted(), "Schema doesn't have an IS_DELETED columns");
    return isDeletedIndex;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof Schema)) {
      return false;
    }
    Schema that = (Schema) obj;
    if (this.getPrimaryKeyColumnCount() != that.getPrimaryKeyColumnCount()) {
      return false;
    }
    if (this.getColumns().size() != that.getColumns().size()) {
      return false;
    }
    for (int i = 0; i < this.getColumns().size(); i++) {
      if (!this.getColumnByIndex(i).equals(that.getColumnByIndex(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(varLengthColumnCount, rowSize, isKeyUnique, hasNullableColumns,
                        hasImmutableColumns, hasAutoIncrementingColumn);
  }
}
