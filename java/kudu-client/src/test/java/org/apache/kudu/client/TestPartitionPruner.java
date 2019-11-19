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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.test.KuduTestHarness;

public class TestPartitionPruner {

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  /**
   * Counts the partitions touched by a scan with optional primary key bounds.
   * The table is assumed to have three INT8 columns as the primary key.
   *
   * @param expectedTablets the expected number of tablets to satisfy the scan
   * @param table                the table to scan
   * @param partitions           the partitions of the table
   * @param lowerBoundPrimaryKey the optional lower bound primary key
   * @param upperBoundPrimaryKey the optional upper bound primary key
   */
  private void checkPartitionsPrimaryKey(int expectedTablets,
                                         KuduTable table,
                                         List<Partition> partitions,
                                         byte[] lowerBoundPrimaryKey,
                                         byte[] upperBoundPrimaryKey) throws Exception {
    KuduScanToken.KuduScanTokenBuilder scanBuilder = client.newScanTokenBuilder(table);

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
      if (!pruner.shouldPruneForTests(partition)) scannedPartitions++;
    }

    // Check that the number of ScanTokens built for the scan matches.
    assertEquals(expectedTablets, scannedPartitions);
    assertEquals(scannedPartitions, scanBuilder.build().size());
    assertEquals(expectedTablets == 0 ? 0 : 1, pruner.numRangesRemainingForTests());
  }

  /**
   * Checks the number of tablets and pruner ranges generated for a scan.
   *
   * @param expectedTablets the expected number of tablets to satisfy the scan
   * @param expectedPrunerRanges the expected number of generated partition pruner ranges
   * @param table the table to scan
   * @param partitions the partitions of the table
   * @param predicates the predicates to apply to the scan
   */
  private void checkPartitions(int expectedTablets,
                               int expectedPrunerRanges,
                               KuduTable table,
                               List<Partition> partitions,
                               KuduPredicate... predicates) {
    checkPartitions(expectedTablets,
                    expectedPrunerRanges,
                    table,
                    partitions,
                    null,
                    null,
                    predicates);
  }

  /**
   * Checks the number of tablets and pruner ranges generated for a scan with
   * predicates and optional partition key bounds.
   *
   * @param expectedTablets the expected number of tablets to satisfy the scan
   * @param expectedPrunerRanges the expected number of generated partition pruner ranges
   * @param table the table to scan
   * @param partitions the partitions of the table
   * @param lowerBoundPartitionKey an optional lower bound partition key
   * @param upperBoundPartitionKey an optional upper bound partition key
   * @param predicates the predicates to apply to the scan
   */
  private void checkPartitions(int expectedTablets,
                               int expectedPrunerRanges,
                               KuduTable table,
                               List<Partition> partitions,
                               byte[] lowerBoundPartitionKey,
                               byte[] upperBoundPartitionKey,
                               KuduPredicate... predicates) {
    // Partition key bounds can't be applied to the ScanTokenBuilder.
    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);

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
      if (!pruner.shouldPruneForTests(partition)) scannedPartitions++;
    }

    assertEquals(expectedTablets, scannedPartitions);
    assertEquals(expectedPrunerRanges, pruner.numRangesRemainingForTests());

    // Check that the scan token builder comes up with the same amount.
    // The scan token builder does not allow for upper/lower partition keys.
    if (lowerBoundPartitionKey == null && upperBoundPartitionKey == null) {
      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);

      for (KuduPredicate predicate : predicates) {
        tokenBuilder.addPredicate(predicate);
      }

      // Check that the number of ScanTokens built for the scan matches.
      assertEquals(expectedTablets, tokenBuilder.build().size());
    }
  }

  /**
   * Retrieves the partitions of a table.
   *
   * @param table the table
   * @return the partitions of the table
   */
  private List<Partition> getTablePartitions(KuduTable table) {
    List<Partition> partitions = new ArrayList<>();
    for (KuduScanToken token : client.newScanTokenBuilder(table).build()) {
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

    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    byte min = Byte.MIN_VALUE;

    // No bounds
    checkPartitionsPrimaryKey(3, table, partitions,
                              null, null);

    // PK < (-1, min, min)
    checkPartitionsPrimaryKey(1, table, partitions,
                              null, new byte[] { -1, min, min });

    // PK < (0, 0, 0)
    checkPartitionsPrimaryKey(1, table, partitions,
                              null, new byte[] { 0, 0, 0 });

    // PK < (0, 0, min)
    checkPartitionsPrimaryKey(1, table, partitions,
                              null, new byte[] { 0, 0, min });

    // PK < (10, 10, 10)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { 10, 10, 10 });

    // PK < (100, min, min)
    checkPartitionsPrimaryKey(3, table, partitions,
                              null, new byte[] { 100, min, min });

    // PK >= (-10, -10, -10)
    checkPartitionsPrimaryKey(3, table, partitions,
                              new byte[] { -10, -10, -10 }, null);

    // PK >= (0, 0, 0)
    checkPartitionsPrimaryKey(2, table, partitions,
                              new byte[] { 0, 0, 0 }, null);

    // PK >= (100, 0, 0)
    checkPartitionsPrimaryKey(1, table, partitions,
                              new byte[] { 100, 0, 0 }, null);

    // PK >= (-10, 0, 0)
    // PK  < (100, 0, 0)
    checkPartitionsPrimaryKey(3, table, partitions,
                              new byte[] { -10, 0, 0 }, new byte[] { 100, 0, 0 });

    // PK >= (0, 0, 0)
    // PK  < (10, 10, 10)
    checkPartitionsPrimaryKey(1, table, partitions,
                              new byte[] { 0, 0, 0 }, new byte[] { 10, 0, 0 });

    // PK >= (0, 0, 0)
    // PK  < (10, 10, 11)
    checkPartitionsPrimaryKey(1, table, partitions,
                              new byte[] { 0, 0, 0 }, new byte[] { 10, 0, 0 });

    // PK < (0, 0, 0)
    // PK >= (10, 10, 11)
    checkPartitionsPrimaryKey(0, table, partitions,
                              new byte[] { 10, 0, 0 }, new byte[] { 0, 0, 0 });
  }

  @Test
  public void testPrimaryKeyPrefixRangePruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b INT8, c INT8)
    // PRIMARY KEY (a, b, c))
    // PARTITION BY RANGE (a, b)
    //    (PARTITION VALUES < (0, 0, 0));

    ArrayList<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build());
    Schema schema = new Schema(columns);

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("a", "b"));

    PartialRow split = schema.newPartialRow();
    split.addByte("a", (byte) 0);
    split.addByte("b", (byte) 0);
    tableBuilder.addSplitRow(split);

    String tableName = "testPrimaryKeyPrefixRangePruning-" + System.currentTimeMillis();

    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    byte min = Byte.MIN_VALUE;
    byte max = Byte.MAX_VALUE;

    // No bounds
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, null);

    // PK < (-1, min, min)
    // TODO(KUDU-2178): prune the upper partition.
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { -1, min, min });

    // PK < (0, 0, min)
    // TODO(KUDU-2178): prune the upper partition.
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { 0, 0, min });

    // PK < (0, 0, 0)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { 0, 0, 0 });

    // PK < (0, 1, min)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { 0, 1, min });

    // PK < (0, 1, 0)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { 0, 1, 0 });

    // PK < (max, max, min)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { max, max, min });

    // PK < (max, max, 0)
    checkPartitionsPrimaryKey(2, table, partitions,
                              null, new byte[] { max, max, 0 });

    // PK >= (0, 0, min)
    // TODO(KUDU-2178): prune the lower partition.
    checkPartitionsPrimaryKey(2, table, partitions,
                              new byte[] { 0, 0, min }, null);

    // PK >= (0, 0, 0)
    // TODO(KUDU-2178): prune the lower partition.
    checkPartitionsPrimaryKey(2, table, partitions,
                              new byte[] { 0, 0, 0 }, null);

    // PK >= (0, -1, 0)
    checkPartitionsPrimaryKey(2, table, partitions,
                              new byte[] { 0, -1, 0 }, null);
  }

  @Test
  public void testRangePartitionPruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b STRING, c INT8)
    // PRIMARY KEY (a, b, c))
    // PARTITION BY RANGE (c, b)
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
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    checkPartitions(3, 1, table, partitions);

    // c < -10
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, -10));

    // c = -10
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, -10));

    // c < 10
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, 10));

    // c < 100
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, 100));

    // c < MIN
    checkPartitions(0, 0, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, Byte.MIN_VALUE));
    // c < MAX
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, Byte.MAX_VALUE));

    // c >= -10
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, -10));

    // c >= 0
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, -10));

    // c >= 5
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 5));

    // c >= 10
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 10));

    // c >= 100
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 100));

    // c >= MIN
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, Byte.MIN_VALUE));
    // c >= MAX
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, Byte.MAX_VALUE));

    // c >= -10
    // c < 0
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, -10),
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, 0));

    // c >= 5
    // c < 100
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 5),
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, 100));

    // b = ""
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL, ""));

    // b >= "z"
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.GREATER_EQUAL, "z"));

    // b < "a"
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "a"));

    // b >= "m"
    // b < "z"
    checkPartitions(3, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.GREATER_EQUAL, "m"),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "z"));

    // c >= 10
    // b >= "r"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 10),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.GREATER_EQUAL, "r"));

    // c >= 10
    // b < "r"
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.GREATER_EQUAL, 10),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "r"));

    // c = 10
    // b < "r"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 10),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "r"));

    // c < 0
    // b < "m"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "m"));

    // c < 0
    // b < "z"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.LESS, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "z"));

    // c = 0
    // b = "m\0"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL, "m\0"));

    // c = 0
    // b < "m"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "m"));

    // c = 0
    // b < "m\0"
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "m\0"));

    // c = 0
    // c = 2
    checkPartitions(0, 0, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 2));

    // c = MIN
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, Byte.MIN_VALUE));
    // c = MAX
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, Byte.MAX_VALUE));

    // c IN (1, 2)
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 1, (byte) 2)));

    // c IN (0, 1, 2)
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1, (byte) 2)));

    // c IN (-10, 0)
    // b < "m"
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) -10, (byte) 0)),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "m"));

    // c IN (-10, 0)
    // b < "m\0"
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) -10, (byte) 0)),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.LESS, "m\0"));
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
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    checkPartitions(4, 1, table, partitions);

    // a = 0;
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.EQUAL, 0));

    // a >= 0;
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.GREATER_EQUAL, 0));

    // a >= 0;
    // a < 1;
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.GREATER_EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.LESS, 1));

    // a >= 0;
    // a < 2;
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.GREATER_EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.LESS, 2));

    // b = 1;
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL, 1));

    // b = 1;
    // c = 2;
    checkPartitions(2, 2, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL, 1),
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 2));

    // a = 0;
    // b = 1;
    // c = 2;
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(a, ComparisonOp.EQUAL, 0),
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL, 1),
                    KuduPredicate.newComparisonPredicate(c, ComparisonOp.EQUAL, 2));

    // a IN (0, 10)
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 10)));
  }

  @Test
  public void testInListHashPartitionPruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b INT8, c INT8)
    // PRIMARY KEY (a, b, c)
    // PARTITION BY HASH (a) PARTITIONS 3,
    //              HASH (b) PARTITIONS 3,
    //              HASH (c) PARTITIONS 3;
    ColumnSchema a = new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build();
    ColumnSchema b = new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build();
    ColumnSchema c = new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build();
    Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<String>());
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("c"), 3);

    String tableName = "testInListHashPartitionPruning-" + System.currentTimeMillis();
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // a in [0, 1];
    checkPartitions(18, 2, table, partitions,
        KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1)));

    // a in [0, 1, 8];
    checkPartitions(27, 1, table, partitions,
        KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1, (byte) 8)));

    // b in [0, 1];
    checkPartitions(18, 6, table, partitions,
        KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)));

    // c in [0, 1];
    checkPartitions(18, 18, table, partitions,
        KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // b in [0, 1], c in [0, 1];
    checkPartitions(12, 12, table, partitions,
        KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)),
        KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // a in [0, 1], b in [0, 1], c in [0, 1];
    checkPartitions(8, 8, table, partitions,
        KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1)),
        KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)),
        KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));
  }

  @Test
  public void testMultiColumnInListHashPruning() throws Exception {
    // CREATE TABLE t
    // (a INT8, b INT8, c INT8)
    // PRIMARY KEY (a, b, c)
    // PARTITION BY HASH (a) PARTITIONS 3,
    //              HASH (b, c) PARTITIONS 3;
    ColumnSchema a = new ColumnSchema.ColumnSchemaBuilder("a", Type.INT8).key(true).build();
    ColumnSchema b = new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build();
    ColumnSchema c = new ColumnSchema.ColumnSchemaBuilder("c", Type.INT8).key(true).build();
    Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<String>());
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 3);

    String tableName = "testMultiColumnInListHashPartitionPruning-" + System.currentTimeMillis();
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // a in [0, 1];
    checkPartitions(6, 2, table, partitions,
                    KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1)));

    // a in [0, 1, 8];
    checkPartitions(9, 1, table, partitions,
                    KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1, (byte) 8)));

    // b in [0, 1];
    checkPartitions(9, 1, table, partitions,
                    KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)));

    // c in [0, 1];
    checkPartitions(9, 1, table, partitions,
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // b in [0, 1], c in [0, 1]
    // (0, 0) in bucket 2
    // (0, 1) in bucket 2
    // (1, 0) in bucket 1
    // (1, 1) in bucket 0
    checkPartitions(9, 1, table, partitions,
                    KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)),
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // b = 0, c in [0, 1]
    checkPartitions(3, 3, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL,  0),
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // b = 1, c in [0, 1]
    checkPartitions(6, 6, table, partitions,
                    KuduPredicate.newComparisonPredicate(b, ComparisonOp.EQUAL,  1),
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));

    // a in [0, 1], b in [0, 1], c in [0, 1];
    checkPartitions(6, 2, table, partitions,
                    KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1)),
                    KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1)),
                    KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1)));
  }

  @Test
  public void testPruning() throws Exception {
    // CREATE TABLE timeseries
    // (host STRING, metric STRING, timestamp UNIXTIME_MICROS, value DOUBLE)
    // PRIMARY KEY (host, metric, time)
    // DISTRIBUTE BY
    //    RANGE(time)
    //        (PARTITION VALUES < 10,
    //         PARTITION VALUES >= 10);
    //    HASH (host, metric) 2 PARTITIONS;

    ColumnSchema host = new ColumnSchema.ColumnSchemaBuilder("host", Type.STRING).key(true).build();
    ColumnSchema metric = new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).key(true).build();
    ColumnSchema timestamp = new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS).key(true).build();
    ColumnSchema value = new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).build();
    Schema schema = new Schema(ImmutableList.of(host, metric, timestamp, value));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("timestamp"));

    PartialRow split = schema.newPartialRow();
    split.addLong("timestamp", 10);
    tableBuilder.addSplitRow(split);

    tableBuilder.addHashPartitions(ImmutableList.of("host", "metric"), 2);

    String tableName = "testPruning-" + System.currentTimeMillis();
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);
    List<Partition> partitions = getTablePartitions(table);

    // No Predicates
    checkPartitions(4, 1, table, partitions);

    // host = "a"
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"));

    // host = "a"
    // metric = "a"
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"));

    // host = "a"
    // metric = "a"
    // timestamp >= 9;
    checkPartitions(2, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 9));

    // host = "a"
    // metric = "a"
    // timestamp >= 10;
    // timestamp < 20;
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 10),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.LESS, 20));

    // host = "a"
    // metric = "a"
    // timestamp < 10;
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.LESS, 10));

    // host = "a"
    // metric = "a"
    // timestamp >= 10;
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 10));

    // host = "a"
    // metric = "a"
    // timestamp = 10;
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 10));

    byte[] hash1 = new byte[] { 0, 0, 0, 1 };

    // partition key < (hash=1)
    checkPartitions(2, 1, table, partitions, null, hash1);

    // partition key >= (hash=1)
    checkPartitions(2, 1, table, partitions, hash1, null);

    // timestamp = 10
    // partition key < (hash=1)
    checkPartitions(1, 1, table, partitions, null, hash1,
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 10));

    // timestamp = 10
    // partition key >= (hash=1)
    checkPartitions(1, 1, table, partitions, hash1,null,
                    KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 10));

    // timestamp IN (0, 9)
    // host = "a"
    // metric IN ("foo", "baz")
    checkPartitions(1, 1, table, partitions,
                    KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(0L, 9L)),
                    KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
                    KuduPredicate.newInListPredicate(metric, ImmutableList.of("foo", "baz")));

    // timestamp IN (10, 100)
    checkPartitions(2, 2, table, partitions,
                    KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(10L, 100L)));

    // timestamp IN (9, 10)
    checkPartitions(4, 2, table, partitions,
                    KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(9L, 10L)));

    // timestamp IS NOT NULL
    checkPartitions(4, 1, table, partitions,
                    KuduPredicate.newIsNotNullPredicate(timestamp));

    // timestamp IS NULL
    checkPartitions(0, 0, table, partitions,
                    KuduPredicate.newIsNullPredicate(timestamp));
  }
}
