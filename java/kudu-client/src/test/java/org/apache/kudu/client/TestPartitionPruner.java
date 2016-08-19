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

import com.google.common.collect.ImmutableList;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestPartitionPruner extends BaseKuduTest {

  /**
   * Counts the partitions touched by a scan with optional primary key bounds.
   * The table is assumed to have three INT8 columns as the primary key.
   *
   * @param table the table to scan
   * @param partitions the partitions of the table
   * @param lowerBoundPrimaryKey the optional lower bound primary key
   * @param upperBoundPrimaryKey the optional upper bound primary key
   * @return the number of partitions touched by the scan
   */
  private int countPartitionsPrimaryKey(KuduTable table,
                                        List<Partition> partitions,
                                        byte[] lowerBoundPrimaryKey,
                                        byte[] upperBoundPrimaryKey) throws Exception {
    KuduScanToken.KuduScanTokenBuilder scanBuilder = syncClient.newScanTokenBuilder(table);

    if (lowerBoundPrimaryKey != null) {
      PartialRow lower = table.getSchema().newPartialRow();
      for (int i = 0; i < 3; i++) {
        lower.addByte(i, lowerBoundPrimaryKey[i]);
      }
      scanBuilder.lowerBound(lower);
    }

    if (upperBoundPrimaryKey != null) {
      PartialRow upper = table.getSchema().newPartialRow();
      for (int i = 0; i < 3; i++) {
        upper.addByte(i, upperBoundPrimaryKey[i]);
      }
      scanBuilder.exclusiveUpperBound(upper);
    }

    PartitionPruner pruner = PartitionPruner.create(scanBuilder);

    int scannedPartitions = 0;
    for (Partition partition : partitions) {
      if (!pruner.shouldPrune(partition)) scannedPartitions++;
    }

    // Check that the number of ScanTokens built for the scan matches.
    assertEquals(scannedPartitions, scanBuilder.build().size());
    return scannedPartitions;
  }

  /**
   * Counts the partitions touched by a scan with predicates.
   *
   * @param table the table to scan
   * @param partitions the partitions of the table
   * @param predicates the predicates to apply to the scan
   * @return the number of partitions touched by the scan
   */
  private int countPartitions(KuduTable table,
                              List<Partition> partitions,
                              KuduPredicate... predicates) throws Exception {
    KuduScanToken.KuduScanTokenBuilder scanBuilder = syncClient.newScanTokenBuilder(table);

    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }

    PartitionPruner pruner = PartitionPruner.create(scanBuilder);

    int scannedPartitions = 0;
    for (Partition partition : partitions) {
      if (!pruner.shouldPrune(partition)) scannedPartitions++;
    }

    // Check that the number of ScanTokens built for the scan matches.
    assertEquals(scannedPartitions, scanBuilder.build().size());
    return scannedPartitions;
  }

  /**
   * Counts the partitions touched by a scan with predicates and optional partition key bounds.
   *
   * @param table the table to scan
   * @param partitions the partitions of the table
   * @param lowerBoundPartitionKey an optional lower bound partition key
   * @param upperBoundPartitionKey an optional upper bound partition key
   * @param predicates the predicates to apply to the scan
   * @return the number of partitions touched by the scan
   */
  private int countPartitions(KuduTable table,
                              List<Partition> partitions,
                              byte[] lowerBoundPartitionKey,
                              byte[] upperBoundPartitionKey,
                              KuduPredicate... predicates) throws Exception {
    // Partition key bounds can't be applied to the ScanTokenBuilder.
    KuduScanner.KuduScannerBuilder scanBuilder = syncClient.newScannerBuilder(table);

    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }

    if (lowerBoundPartitionKey != null) {
      scanBuilder.lowerBoundPartitionKeyRaw(lowerBoundPartitionKey);
    }
    if (upperBoundPartitionKey != null) {
      scanBuilder.exclusiveUpperBoundPartitionKeyRaw(upperBoundPartitionKey);
    }

    PartitionPruner pruner = PartitionPruner.create(scanBuilder);

    int scannedPartitions = 0;
    for (Partition partition : partitions) {
      if (!pruner.shouldPrune(partition)) scannedPartitions++;
    }

    return scannedPartitions;
  }

  /**
   * Retrieves the partitions of a table.
   *
   * @param table the table
   * @return the partitions of the table
   */
  private List<Partition> getTablePartitions(KuduTable table) {
    List<Partition> partitions = new ArrayList<>();
    for (KuduScanToken token : syncClient.newScanTokenBuilder(table).build()) {
      partitions.add(token.getTablet().getPartition());
    }
    return partitions;
  }

  @Test
  public void testPrimaryKeyRangePruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b INT8, c INT8)
    // PRIMARY KEY (a, b, c))
    // PARTITION BY RANGE (a, b, c)
    //    (PARTITION                 VALUES < (0, 0, 0),
    //     PARTITION    (0, 0, 0) <= VALUES < (10, 10, 10)
    //     PARTITION (10, 10, 10) <= VALUES);

    ArrayList<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build());
    Schema schema = new Schema(columns);

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("a", "b", "c"));

    PartialRow split = schema.newPartialRow();
    split.addByte("a", (byte) 0);
    split.addByte("b", (byte) 0);
    split.addByte("c", (byte) 0);
    tableBuilder.addSplitRow(split);
    split.addByte("a", (byte) 10);
    split.addByte("b", (byte) 10);
    split.addByte("c", (byte) 10);
    tableBuilder.addSplitRow(split);

    String tableName = "testPrimaryKeyRangePruning-" + System.currentTimeMillis();

    syncClient.createTable(tableName, schema, tableBuilder);
    KuduTable table = syncClient.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    byte min = Byte.MIN_VALUE;

    // No bounds
    assertEquals(3, countPartitionsPrimaryKey(table, partitions, null, null));

    // PK < (-1, min, min)
    assertEquals(1, countPartitionsPrimaryKey(table, partitions, null,
                                              new byte[] { -1, min, min }));

    // PK < (10, 10, 10)
    assertEquals(2, countPartitionsPrimaryKey(table, partitions, null, new byte[] { 10, 10, 10 }));

    // PK < (100, min, min)
    assertEquals(3, countPartitionsPrimaryKey(table, partitions, null, new byte[] { 100, min, min }));

    // PK >= (-10, -10, -10)
    assertEquals(3, countPartitionsPrimaryKey(table, partitions, new byte[] { -10, -10, -10 }, null));

    // PK >= (0, 0, 0)
    assertEquals(2, countPartitionsPrimaryKey(table, partitions, new byte[] { 0, 0, 0 }, null));

    // PK >= (100, 0, 0)
    assertEquals(1, countPartitionsPrimaryKey(table, partitions, new byte[] { 100, 0, 0 }, null));

    // PK >= (-10, 0, 0)
    // PK  < (100, 0, 0)
    assertEquals(3, countPartitionsPrimaryKey(table, partitions,
                                              new byte[] { -10, 0, 0 },
                                              new byte[] { 100, 0, 0 }));

    // PK >= (0, 0, 0)
    // PK  < (10, 10, 10)
    assertEquals(1, countPartitionsPrimaryKey(table, partitions,
                                              new byte[] { 0, 0, 0 },
                                              new byte[] { 10, 0, 0 }));

    // PK >= (0, 0, 0)
    // PK  < (10, 10, 11)
    assertEquals(1, countPartitionsPrimaryKey(table, partitions,
                                              new byte[] { 0, 0, 0 },
                                              new byte[] { 10, 0, 0 }));

    // PK < (0, 0, 0)
    // PK >= (10, 10, 11)
    assertEquals(0, countPartitionsPrimaryKey(table, partitions,
                                              new byte[] { 10, 0, 0 },
                                              new byte[] { 0, 0, 0 }));
  }

  @Test
  public void testRangePartitionPruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b STRING, c INT8)
    // PRIMARY KEY (a, b, c))
    // DISTRIBUTE BY RANGE(c, b);
    // PARTITION BY RANGE (a, b, c)
    //    (PARTITION              VALUES < (0, "m"),
    //     PARTITION  (0, "m") <= VALUES < (10, "r")
    //     PARTITION (10, "r") <= VALUES);

    ColumnSchema a = new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build();
    ColumnSchema b = new ColumnSchema.ColumnSchemaBuilder("b", Type.STRING).key(true).build();
    ColumnSchema c = new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build();
    Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("c", "b"));

    PartialRow split = schema.newPartialRow();
    split.addByte("c", (byte) 0);
    split.addString("b", "m");
    tableBuilder.addSplitRow(split);
    split.addByte("c", (byte) 10);
    split.addString("b", "r");
    tableBuilder.addSplitRow(split);

    String tableName = "testRangePartitionPruning-" + System.currentTimeMillis();
    syncClient.createTable(tableName, schema, tableBuilder);
    KuduTable table = syncClient.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    assertEquals(3, countPartitions(table, partitions));

    // c < -10
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, -10)));

    // c = -10
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, -10)));

    // c < 10
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, 10)));

    // c < 100
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, 100)));


    // c >= -10
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, -10)));

    // c >= 0
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, -10)));

    // c >= 5
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 5)));

    // c >= 10
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 10)));

    // c >= 100
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 100)));

    // c >= -10
    // c < 0
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, -10),
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, 0)));

    // c >= 5
    // c < 100
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 5),
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, 100)));

    // b = ""
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.EQUAL, "")));

    // b >= "z"
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.GREATER_EQUAL, "z")));

    // b < "a"
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "a")));

    // b >= "m"
    // b < "z"
    assertEquals(3, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.GREATER_EQUAL, "m"),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "z")));

    // c >= 10
    // b >= "r"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 10),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.GREATER_EQUAL, "r")));

    // c >= 10
    // b < "r"
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.GREATER_EQUAL, 10),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "r")));

    // c = 10
    // b < "r"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 10),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "r")));

    // c < 0
    // b < "m"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 0),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "m")));

    // c < 0
    // b < "z"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.LESS, 0),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "z")));

    // c = 0
    // b = "m\0"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 0),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.EQUAL, "m\0")));

    // c = 0
    // b < "m"
    assertEquals(1, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 0),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "m")));

    // c = 0
    // b < "m\0"
    assertEquals(2, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 0),
        KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.LESS, "m\0")));

    // c = 0
    // c = 2
    assertEquals(0, countPartitions(table, partitions,
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 0),
        KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 2)));
  }

  @Test
  public void testHashPartitionPruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b INT8, c INT8)
    // PRIMARY KEY (a, b, c)
    // PARTITION BY HASH (a) PARTITIONS 2,
    //              HASH (b, c) PARTITIONS 2;

    ColumnSchema a = new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build();
    ColumnSchema b = new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build();
    ColumnSchema c = new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build();
    Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<String>());
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 2);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 2);

    String tableName = "testHashPartitionPruning-" + System.currentTimeMillis();
    syncClient.createTable(tableName, schema, tableBuilder);
    KuduTable table = syncClient.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    assertEquals(4, countPartitions(table, partitions));

    // a = 0;
    assertEquals(2, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.EQUAL, 0)));

    // a >= 0;
    assertEquals(4, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.GREATER_EQUAL, 0)));

    // a >= 0;
    // a < 1;
    assertEquals(2, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.GREATER_EQUAL, 0),
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.LESS, 1)));

    // a >= 0;
    // a < 2;
    assertEquals(4, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.GREATER_EQUAL, 0),
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.LESS, 2)));

    // b = 1;
    assertEquals(4, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.EQUAL, 1)));

    // b = 1;
    // c = 2;
    assertEquals(2, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.EQUAL, 1),
          KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 2)));

    // a = 0;
    // b = 1;
    // c = 2;
    assertEquals(1, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(a, KuduPredicate.ComparisonOp.EQUAL, 0),
          KuduPredicate.newComparisonPredicate(b, KuduPredicate.ComparisonOp.EQUAL, 1),
          KuduPredicate.newComparisonPredicate(c, KuduPredicate.ComparisonOp.EQUAL, 2)));
  }

  @Test
  public void testPruning() throws Exception {
    // CREATE TABLE timeseries
    // (host STRING, metric STRING, timestamp TIMESTAMP, value DOUBLE)
    // PRIMARY KEY (host, metric, time)
    // DISTRIBUTE BY
    //    RANGE(time) SPLIT ROWS [(10)],
    //        (PARTITION       VALUES < 10,
    //         PARTITION 10 <= VALUES);
    //    HASH (host, metric) 2 PARTITIONS;

    ColumnSchema host = new ColumnSchema.ColumnSchemaBuilder("host", Type.STRING).key(true).build();
    ColumnSchema metric = new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).key(true).build();
    ColumnSchema timestamp = new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.TIMESTAMP).key(true).build();
    ColumnSchema value = new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).build();
    Schema schema = new Schema(ImmutableList.of(host, metric, timestamp, value));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("timestamp"));

    PartialRow split = schema.newPartialRow();
    split.addLong("timestamp", 10);
    tableBuilder.addSplitRow(split);

    tableBuilder.addHashPartitions(ImmutableList.of("host", "metric"), 2);

    String tableName = "testPruning-" + System.currentTimeMillis();
    syncClient.createTable(tableName, schema, tableBuilder);
    KuduTable table = syncClient.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    assertEquals(4, countPartitions(table, partitions));

    // host = "a"
    assertEquals(4, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a")));

    // host = "a"
    // metric = "a"
    assertEquals(2, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a")));

    // host = "a"
    // metric = "a"
    // timestamp >= 9;
    assertEquals(2, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.GREATER_EQUAL, 9)));

    // host = "a"
    // metric = "a"
    // timestamp >= 10;
    // timestamp < 20;
    assertEquals(1, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.GREATER_EQUAL, 10),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.LESS, 20)));

    // host = "a"
    // metric = "a"
    // timestamp < 10;
    assertEquals(1, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.LESS, 10)));

    // host = "a"
    // metric = "a"
    // timestamp >= 10;
    assertEquals(1, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.GREATER_EQUAL, 10)));

    // host = "a"
    // metric = "a"
    // timestamp = 10;
    assertEquals(1, countPartitions(table, partitions,
          KuduPredicate.newComparisonPredicate(host, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(metric, KuduPredicate.ComparisonOp.EQUAL, "a"),
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.EQUAL, 10)));

    // partition key < (hash=1)
    assertEquals(2, countPartitions(table, partitions, new byte[] {}, new byte[] { 0, 0, 0, 1 }));

    // partition key >= (hash=1)
    assertEquals(2, countPartitions(table, partitions, new byte[] { 0, 0, 0, 1 }, new byte[] {}));

    // timestamp = 10
    // partition key < (hash=1)
    assertEquals(1, countPartitions(table, partitions, new byte[] {}, new byte[] { 0, 0, 0, 1 },
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.EQUAL, 10)));

    // timestamp = 10
    // partition key >= (hash=1)
    assertEquals(1, countPartitions(table, partitions, new byte[] { 0, 0, 0, 1 }, new byte[] {},
          KuduPredicate.newComparisonPredicate(timestamp, KuduPredicate.ComparisonOp.EQUAL, 10)));
  }
}
