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

import static org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import static org.apache.kudu.ColumnSchema.Encoding;
import static org.apache.kudu.master.Master.AlterTableRequestPB;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Type;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;

/**
 * This builder must be used to alter a table. At least one change must be specified.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class AlterTableOptions {

  private final AlterTableRequestPB.Builder pb = AlterTableRequestPB.newBuilder();

  /**
   * Change a table's name.
   * @param newName new table's name, must be used to check progress
   * @return this instance
   */
  public AlterTableOptions renameTable(String newName) {
    pb.setNewTableName(newName);
    return this;
  }

  /**
   * Add a new column.
   * @param colSchema the schema of the new column
   * @return this instance
   */
  public AlterTableOptions addColumn(ColumnSchema colSchema) {
    if (!colSchema.isNullable() && colSchema.getDefaultValue() == null) {
      throw new IllegalArgumentException("A new non-null column must have a default value");
    }
    if (colSchema.isKey()) {
      throw new IllegalArgumentException("Key columns cannot be added");
    }
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ADD_COLUMN);
    step.setAddColumn(AlterTableRequestPB.AddColumn.newBuilder()
        .setSchema(ProtobufHelper.columnToPb(colSchema)));
    return this;
  }

  /**
   * Add a new column that's not nullable.
   * @param name name of the new column
   * @param type type of the new column
   * @param defaultVal default value used for the currently existing rows
   * @return this instance
   */
  public AlterTableOptions addColumn(String name, Type type, Object defaultVal) {
    return addColumn(new ColumnSchema.ColumnSchemaBuilder(name, type)
        .defaultValue(defaultVal)
        .build());
  }

  /**
   * Add a new column that's nullable and has no default value.
   * @param name name of the new column
   * @param type type of the new column
   * @return this instance
   */
  public AlterTableOptions addNullableColumn(String name, Type type) {
    return addNullableColumn(name, type, null);
  }

  /**
   * Add a new column that's nullable.
   * @param name name of the new column
   * @param type type of the new column
   * @param defaultVal the default value of the new column
   * @return this instance
   */
  public AlterTableOptions addNullableColumn(String name, Type type, Object defaultVal) {
    return addColumn(new ColumnSchema.ColumnSchemaBuilder(name, type)
        .nullable(true)
        .defaultValue(defaultVal)
        .build());
  }

  /**
   * Drop a column.
   * @param name name of the column
   * @return this instance
   */
  public AlterTableOptions dropColumn(String name) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.DROP_COLUMN);
    step.setDropColumn(AlterTableRequestPB.DropColumn.newBuilder().setName(name));
    return this;
  }

  /**
   * Change the name of a column.
   * @param oldName old column's name, must exist
   * @param newName new name to use
   * @return this instance
   */
  public AlterTableOptions renameColumn(String oldName, String newName) {
    // For backwards compatibility, this uses the RENAME_COLUMN step type.
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.RENAME_COLUMN);
    step.setRenameColumn(AlterTableRequestPB.RenameColumn.newBuilder().setOldName(oldName)
        .setNewName(newName));
    return this;
  }

  /**
   * Remove the default value for a column.
   * @param name name of the column
   * @return this instance
   */
  public AlterTableOptions removeDefault(String name) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ALTER_COLUMN);
    AlterTableRequestPB.AlterColumn.Builder alterBuilder =
        AlterTableRequestPB.AlterColumn.newBuilder();
    alterBuilder.setDelta(
        Common.ColumnSchemaDeltaPB.newBuilder().setName(name).setRemoveDefault(true));
    step.setAlterColumn(alterBuilder);
    return this;
  }

  /**
   * Change the default value for a column. `newDefault` must not be null or
   * else throws {@link IllegalArgumentException}.
   * @param name name of the column
   * @param newDefault the new default value
   * @return this instance
   */
  public AlterTableOptions changeDefault(String name, Object newDefault) {
    if (newDefault == null) {
      throw new IllegalArgumentException("newDefault cannot be null: " +
          "use removeDefault to clear a default value");
    }

    ByteString defaultByteString = ProtobufHelper.objectToByteStringNoType(name, newDefault);
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ALTER_COLUMN);
    AlterTableRequestPB.AlterColumn.Builder alterBuilder =
        AlterTableRequestPB.AlterColumn.newBuilder();
    alterBuilder.setDelta(
        Common.ColumnSchemaDeltaPB.newBuilder().setName(name)
            .setDefaultValue(defaultByteString));
    step.setAlterColumn(alterBuilder);
    return this;
  }

  /**
   * Change the block size of a column's storage. A nonpositive value indicates
   * a server-side default.
   * @param name name of the column
   * @param blockSize the new block size
   * @return this instance
   */
  public AlterTableOptions changeDesiredBlockSize(String name, int blockSize) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ALTER_COLUMN);
    AlterTableRequestPB.AlterColumn.Builder alterBuilder =
        AlterTableRequestPB.AlterColumn.newBuilder();
    alterBuilder.setDelta(
        Common.ColumnSchemaDeltaPB.newBuilder().setName(name).setBlockSize(blockSize));
    step.setAlterColumn(alterBuilder);
    return this;
  }

  /**
   * Change the encoding used for a column.
   * @param name name of the column
   * @param encoding the new encoding
   * @return this instance
   */
  public AlterTableOptions changeEncoding(String name, Encoding encoding) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ALTER_COLUMN);
    AlterTableRequestPB.AlterColumn.Builder alterBuilder =
        AlterTableRequestPB.AlterColumn.newBuilder();
    alterBuilder.setDelta(
        Common.ColumnSchemaDeltaPB.newBuilder().setName(name)
            .setEncoding(encoding.getInternalPbType()));
    step.setAlterColumn(alterBuilder);
    return this;
  }

  /**
   * Change the compression used for a column.
   * @param name the name of the column
   * @param ca the new compression algorithm
   * @return this instance
   */
  public AlterTableOptions changeCompressionAlgorithm(String name, CompressionAlgorithm ca) {
    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ALTER_COLUMN);
    AlterTableRequestPB.AlterColumn.Builder alterBuilder =
        AlterTableRequestPB.AlterColumn.newBuilder();
    alterBuilder.setDelta(
        Common.ColumnSchemaDeltaPB.newBuilder().setName(name)
            .setCompression(ca.getInternalPbType()));
    step.setAlterColumn(alterBuilder);
    return this;
  }

  /**
   * Add a range partition to the table with an inclusive lower bound and an exclusive upper bound.
   *
   * If either row is empty, then that end of the range will be unbounded. If a range column is
   * missing a value, the logical minimum value for that column type will be used as the default.
   *
   * Multiple range partitions may be added as part of a single alter table transaction by calling
   * this method multiple times. Added range partitions must not overlap with each
   * other or any existing range partitions (unless the existing range partitions are dropped as
   * part of the alter transaction first). The lower bound must be less than the upper bound.
   *
   * This client will immediately be able to write and scan the new tablets when the alter table
   * operation returns success, however other existing clients may have to wait for a timeout period
   * to elapse before the tablets become visible. This period is configured by the master's
   * 'table_locations_ttl_ms' flag, and defaults to 5 minutes.
   *
   * @param lowerBound inclusive lower bound, may be empty but not null
   * @param upperBound exclusive upper bound, may be empty but not null
   * @return this instance
   */
  public AlterTableOptions addRangePartition(PartialRow lowerBound, PartialRow upperBound) {
    return addRangePartition(lowerBound, upperBound,
                             RangePartitionBound.INCLUSIVE_BOUND,
                             RangePartitionBound.EXCLUSIVE_BOUND);
  }

  /**
   * Add a range partition to the table with a lower bound and upper bound.
   *
   * If either row is empty, then that end of the range will be unbounded. If a range column is
   * missing a value, the logical minimum value for that column type will be used as the default.
   *
   * Multiple range partitions may be added as part of a single alter table transaction by calling
   * this method multiple times. Added range partitions must not overlap with each
   * other or any existing range partitions (unless the existing range partitions are dropped as
   * part of the alter transaction first). The lower bound must be less than the upper bound.
   *
   * This client will immediately be able to write and scan the new tablets when the alter table
   * operation returns success, however other existing clients may have to wait for a timeout period
   * to elapse before the tablets become visible. This period is configured by the master's
   * 'table_locations_ttl_ms' flag, and defaults to 5 minutes.
   *
   * @param lowerBound lower bound, may be empty but not null
   * @param upperBound upper bound, may be empty but not null
   * @param lowerBoundType the type of the lower bound, either inclusive or exclusive
   * @param upperBoundType the type of the upper bound, either inclusive or exclusive
   * @return this instance
   */
  public AlterTableOptions addRangePartition(PartialRow lowerBound,
                                             PartialRow upperBound,
                                             RangePartitionBound lowerBoundType,
                                             RangePartitionBound upperBoundType) {
    Preconditions.checkNotNull(lowerBound);
    Preconditions.checkNotNull(upperBound);
    Preconditions.checkArgument(lowerBound.getSchema().equals(upperBound.getSchema()));

    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.ADD_RANGE_PARTITION);
    AlterTableRequestPB.AddRangePartition.Builder builder =
        AlterTableRequestPB.AddRangePartition.newBuilder();
    builder.setRangeBounds(
        new Operation.OperationsEncoder()
            .encodeLowerAndUpperBounds(lowerBound, upperBound, lowerBoundType, upperBoundType));
    step.setAddRangePartition(builder);
    if (!pb.hasSchema()) {
      pb.setSchema(ProtobufHelper.schemaToPb(lowerBound.getSchema()));
    }
    return this;
  }

  /**
   * Drop the range partition from the table with the specified inclusive lower bound and exclusive
   * upper bound. The bounds must match exactly, and may not span multiple range partitions.
   *
   * If either row is empty, then that end of the range will be unbounded. If a range column is
   * missing a value, the logical minimum value for that column type will be used as the default.
   *
   * Multiple range partitions may be dropped as part of a single alter table transaction by calling
   * this method multiple times.
   *
   * @param lowerBound inclusive lower bound, can be empty but not null
   * @param upperBound exclusive upper bound, can be empty but not null
   * @return this instance
   */
  public AlterTableOptions dropRangePartition(PartialRow lowerBound, PartialRow upperBound) {
    return dropRangePartition(lowerBound, upperBound,
                              RangePartitionBound.INCLUSIVE_BOUND,
                              RangePartitionBound.EXCLUSIVE_BOUND);
  }

  /**
   * Drop the range partition from the table with the specified lower bound and upper bound.
   * The bounds must match exactly, and may not span multiple range partitions.
   *
   * If either row is empty, then that end of the range will be unbounded. If a range column is
   * missing a value, the logical minimum value for that column type will be used as the default.
   *
   * Multiple range partitions may be dropped as part of a single alter table transaction by calling
   * this method multiple times.
   *
   * @param lowerBound inclusive lower bound, can be empty but not null
   * @param upperBound exclusive upper bound, can be empty but not null
   * @param lowerBoundType the type of the lower bound, either inclusive or exclusive
   * @param upperBoundType the type of the upper bound, either inclusive or exclusive
   * @return this instance
   */
  public AlterTableOptions dropRangePartition(PartialRow lowerBound,
                                              PartialRow upperBound,
                                              RangePartitionBound lowerBoundType,
                                              RangePartitionBound upperBoundType) {
    Preconditions.checkNotNull(lowerBound);
    Preconditions.checkNotNull(upperBound);
    Preconditions.checkArgument(lowerBound.getSchema().equals(upperBound.getSchema()));

    AlterTableRequestPB.Step.Builder step = pb.addAlterSchemaStepsBuilder();
    step.setType(AlterTableRequestPB.StepType.DROP_RANGE_PARTITION);
    AlterTableRequestPB.DropRangePartition.Builder builder =
        AlterTableRequestPB.DropRangePartition.newBuilder();
    builder.setRangeBounds(
        new Operation.OperationsEncoder().encodeLowerAndUpperBounds(lowerBound, upperBound,
                                                                    lowerBoundType,
                                                                    upperBoundType));
    step.setDropRangePartition(builder);
    if (!pb.hasSchema()) {
      pb.setSchema(ProtobufHelper.schemaToPb(lowerBound.getSchema()));
    }
    return this;
  }

  /**
   * @return {@code true} if the alter table operation includes an add or drop partition operation
   */
  @InterfaceAudience.Private
  boolean hasAddDropRangePartitions() {
    return pb.hasSchema();
  }

  /**
   * @return the AlterTableRequest protobuf message.
   */
  AlterTableRequestPB.Builder getProtobuf() {
    return pb;
  }
}
