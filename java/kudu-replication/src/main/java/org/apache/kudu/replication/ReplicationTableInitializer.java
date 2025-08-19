// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Partition;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.RangePartitionBound;
import org.apache.kudu.client.RangePartitionWithCustomHashSchema;

/**
 * Helper class that initializes the sink-side table for replication.
 * If the table does not exist on the sink, it creates an identical table
 * with the same schema, table parameters, and partitioning scheme as the source.
 *
 * <p>This implementation is based on the Scala backup/restore logic from the
 * kudu-backup module, specifically the
 * KuduRestore.createTableRangePartitionByRangePartition method.
 *
 * <p>The partition recreation logic closely follows the patterns established
 * in the Kudu backup/restore functionality to ensure correct handling of:
 * - Table-wide hash schemas
 * - Range partitions with custom hash schemas
 * - Mixed partition scenarios
 */
public class ReplicationTableInitializer implements AutoCloseable {
  private final KuduClient sourceClient;
  private final KuduClient sinkClient;
  private final ReplicationJobConfig config;
  private KuduTable sourceTable;

  public ReplicationTableInitializer(ReplicationJobConfig config) {
    sourceClient = new KuduClient.KuduClientBuilder(
            String.join(",", config.getSourceMasterAddresses())).build();
    sinkClient = new KuduClient.KuduClientBuilder(
            String.join(",", config.getSinkMasterAddresses())).build();
    this.config = config;
  }

  public void createTableIfNotExists() throws Exception {
    if (!sinkClient.tableExists(config.getSinkTableName())) {
      try {
        sourceTable = sourceClient.openTable(config.getTableName());
        createTableRangePartitionByRangePartition();
      } catch (Exception e) {
        throw new RuntimeException("Failed to create table " + config.getSinkTableName(), e);
      }
    }
  }

  /**
   * Creates table with range partitions, handling both table-wide and custom hash schemas.
   *
   * <p>This method is a Java port of the Scala method with the same name from
   * the kudu-backup module in KuduRestore.createTableRangePartitionByRangePartition.
   *
   * <p>The logic follows the same pattern:
   * 1. Get ranges with table-wide hash schema vs custom hash schema separately
   * 2. Create table with first range (if any)
   * 3. Add remaining ranges via ALTER TABLE operations
   */
  private void createTableRangePartitionByRangePartition() throws Exception {
    CreateTableOptions options = getCreateTableOptionsWithoutRangePartitions();
    List<Partition> rangePartitionsWithTableHashSchema =
            sourceTable.getRangePartitionsWithTableHashSchema(
                    sourceClient.getDefaultAdminOperationTimeoutMs());
    List<RangePartitionWithCustomHashSchema> boundsWithCustomHashSchema =
            getRangeBoundsPartialRowsWithHashSchemas();

    if (!rangePartitionsWithTableHashSchema.isEmpty()) {
      // Adds the first range partition with table wide hash schema through create.
      options.addRangePartition(
              rangePartitionsWithTableHashSchema.get(0).getDecodedRangeKeyStart(sourceTable),
              rangePartitionsWithTableHashSchema.get(0).getDecodedRangeKeyEnd(sourceTable));
      sinkClient.createTable(config.getSinkTableName(), sourceTable.getSchema(), options);

      // Add the rest of the range partitions with table wide hash schema through alters.
      rangePartitionsWithTableHashSchema.stream().skip(1).forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        alterOptions.addRangePartition(partition.getDecodedRangeKeyStart(sourceTable),
                partition.getDecodedRangeKeyEnd(sourceTable));
        try {
          sinkClient.alterTable(config.getSinkTableName(), alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + config.getSinkTableName(), e);
        }
      });

      // adds range partitions with custom hash schema through alters.
      boundsWithCustomHashSchema.stream().forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        alterOptions.addRangePartition(partition);
        try {
          sinkClient.alterTable(config.getSinkTableName(), alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + config.getSinkTableName(), e);
        }
      });


    } else if (!boundsWithCustomHashSchema.isEmpty()) {
      // Adds first range partition with custom hash schema through create.
      options.addRangePartition(boundsWithCustomHashSchema.get(0));
      sinkClient.createTable(config.getSinkTableName(), sourceTable.getSchema(), options);

      // Adds rest of range partitions with custom hash schema through alters.
      boundsWithCustomHashSchema.stream().skip(1).forEach(partition -> {
        AlterTableOptions alterOptions = new AlterTableOptions();
        alterOptions.addRangePartition(partition);
        try {
          sinkClient.alterTable(config.getSinkTableName(), alterOptions);
        } catch (KuduException e) {
          throw new RuntimeException("Failed to alter table: " + config.getSinkTableName(), e);
        }
      });
    }

  }

  /**
   * Creates base table options including table-wide hash schema but without range partitions.
   *
   * <p>This implementation corresponds to the Scala method
   * TableMetadata.getCreateTableOptionsWithoutRangePartitions from the kudu-backup-common module.
   */
  private CreateTableOptions getCreateTableOptionsWithoutRangePartitions() {
    CreateTableOptions options = new CreateTableOptions();
    if (config.getRestoreOwner()) {
      options.setOwner(sourceTable.getOwner());
    }
    options.setComment(sourceTable.getComment());
    options.setNumReplicas(sourceTable.getNumReplicas());
    options.setExtraConfigs(sourceTable.getExtraConfig());
    PartitionSchema partitionSchema = sourceTable.getPartitionSchema();

    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            partitionSchema.getHashBucketSchemas();
    for (PartitionSchema.HashBucketSchema hashBucketSchema : hashBucketSchemas) {
      List<String> colNames = getColumnNamesFromColumnIds(hashBucketSchema.getColumnIds());
      options.addHashPartitions(colNames, hashBucketSchema.getNumBuckets(),
              hashBucketSchema.getSeed());
    }

    PartitionSchema.RangeSchema rangeSchema = partitionSchema.getRangeSchema();
    List<String> colNames = getColumnNamesFromColumnIds(rangeSchema.getColumnIds());
    options.setRangePartitionColumns(colNames);

    return options;
  }

  /**
   * Extracts range partitions that have custom hash schemas (different from table-wide schema).
   *
   * <p>This implementation corresponds to the Scala method
   * TableMetadata.getRangeBoundsPartialRowsWithHashSchemas from the kudu-backup-common module.
   */
  private List<RangePartitionWithCustomHashSchema> getRangeBoundsPartialRowsWithHashSchemas() {
    List<RangePartitionWithCustomHashSchema> rangeBoundsPartialRowsWithHashSchemas =
            new ArrayList<>();
    PartitionSchema partitionSchema = sourceTable.getPartitionSchema();
    List<PartitionSchema.RangeWithHashSchema> rangesWithHashSchemas =
            partitionSchema.getRangesWithHashSchemas();
    for (PartitionSchema.RangeWithHashSchema rangeWithHashSchema : rangesWithHashSchemas) {
      RangePartitionWithCustomHashSchema partition = new RangePartitionWithCustomHashSchema(
              rangeWithHashSchema.lowerBound, rangeWithHashSchema.upperBound,
              RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND);

      List<PartitionSchema.HashBucketSchema> hashBucketSchemas = rangeWithHashSchema.hashSchemas;
      for (PartitionSchema.HashBucketSchema hashBucketSchema : hashBucketSchemas) {
        List<String> colNames = getColumnNamesFromColumnIds(hashBucketSchema.getColumnIds());
        partition.addHashPartitions(colNames, hashBucketSchema.getNumBuckets(),
                hashBucketSchema.getSeed());
      }
      rangeBoundsPartialRowsWithHashSchemas.add(partition);
    }
    return rangeBoundsPartialRowsWithHashSchemas;
  }

  private List<String> getColumnNamesFromColumnIds(List<Integer> columnIds) {
    List<String> columnNames = new ArrayList<>();
    for (int id : columnIds) {
      int idx = sourceTable.getSchema().getColumnIndex(id);
      columnNames.add(sourceTable.getSchema().getColumnByIndex(idx).getName());
    }
    return columnNames;
  }

  @Override
  public void close() throws Exception {
    sourceClient.close();
    sinkClient.close();
  }
}

