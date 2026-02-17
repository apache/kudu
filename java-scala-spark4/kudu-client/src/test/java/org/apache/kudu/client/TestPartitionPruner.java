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

import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.test.KuduTestHarness;

public class TestPartitionPruner {
  public static final Logger LOG = LoggerFactory.getLogger(TestPartitionPruner.class);

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  /**
   * This method is an obsolete implementation of PartitionPruner.java#pruneHashComponent.
   * The implementation is inefficient and may cause OOM.
   *
   * Search all combinations of in-list and equality predicates for pruneable hash partitions.
   * This method is used just for test to check new algorithm's correctness.
   * @deprecated we can remove it later, now just for test.
   * @return a bitset containing {@code false} bits for hash buckets which may be pruned
   */
  private static BitSet pruneHashComponent(Schema schema,
                                           PartitionSchema.HashBucketSchema hashSchema,
                                           Map<String, KuduPredicate> predicates) {
    BitSet hashBuckets = new BitSet(hashSchema.getNumBuckets());
    List<Integer> columnIdxs =
        PartitionPruner.idsToIndexesForTest(schema, hashSchema.getColumnIds());
    for (int idx : columnIdxs) {
      ColumnSchema column = schema.getColumnByIndex(idx);
      KuduPredicate predicate = predicates.get(column.getName());
      if (predicate == null ||
          (predicate.getType() != KuduPredicate.PredicateType.EQUALITY &&
           predicate.getType() != KuduPredicate.PredicateType.IN_LIST)) {
        hashBuckets.set(0, hashSchema.getNumBuckets());
        return hashBuckets;
      }
    }

    List<PartialRow> rows = Arrays.asList(schema.newPartialRow());
    for (int idx : columnIdxs) {
      List<PartialRow> newRows = new ArrayList<>();
      ColumnSchema column = schema.getColumnByIndex(idx);
      KuduPredicate predicate = predicates.get(column.getName());
      List<byte[]> predicateValues;
      if (predicate.getType() == KuduPredicate.PredicateType.EQUALITY) {
        predicateValues = Collections.singletonList(predicate.getLower());
      } else {
        predicateValues = Arrays.asList(predicate.getInListValues());
      }
      // For each of the encoded string, replicate it by the number of values in
      // equality and in-list predicate.
      for (PartialRow row : rows) {
        for (byte[] predicateValue : predicateValues) {
          PartialRow newRow = new PartialRow(row);
          newRow.setRaw(idx, predicateValue);
          newRows.add(newRow);
        }
      }
      rows = newRows;
    }
    for (PartialRow row : rows) {
      int hash = KeyEncoder.getHashBucket(row, hashSchema);
      hashBuckets.set(hash);
    }
    return hashBuckets;
  }

  static class ReturnValueHelper {
    private Schema schema;
    private PartitionSchema partitionSchema;
    private Map<String, KuduPredicate> predicates;

    public ReturnValueHelper(Schema schema, PartitionSchema partitionSchema,
        Map<String, KuduPredicate> predicates) {
      this.schema = schema;
      this.partitionSchema = partitionSchema;
      this.predicates = predicates;
    }
  }

  // Prepare test cases for unit tests to test large in-list predicates.
  public List<ReturnValueHelper> prepareForLargeInListPredicates(KuduClient client,
      String tablePrefix, int totalCount, int inListMaxLength) throws KuduException {
    final int columnSize = 200;
    String keyNamePrefix = "key";
    final int keyColumnNumber = 10;
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    List<String> keyColumnNames = new ArrayList<>();
    List<ColumnSchema> keyColumnSchemas = new ArrayList<>();
    for (int i = 0; i < columnSize; i++) {
      boolean isKey = false;
      String columnName = keyNamePrefix + i;
      if (i < keyColumnNumber) {
        isKey = true;
      }
      ColumnSchema columnSchema = new ColumnSchema.ColumnSchemaBuilder(columnName, Type.INT32)
          .key(isKey).build();
      if (isKey) {
        keyColumnNames.add(columnName);
        keyColumnSchemas.add(columnSchema);
      }
      columnSchemas.add(columnSchema);
    }

    final Schema schema = new Schema(columnSchemas);
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<>());
    tableBuilder.addHashPartitions(ImmutableList.of(keyColumnNames.get(0)), 2);
    tableBuilder.addHashPartitions(ImmutableList.of(
        keyColumnNames.get(1), keyColumnNames.get(2)), 2);
    tableBuilder.addHashPartitions(
        ImmutableList.of(keyColumnNames.get(3), keyColumnNames.get(4)), 3);
    tableBuilder.addHashPartitions(
        ImmutableList.of(
            keyColumnNames.get(5), keyColumnNames.get(6), keyColumnNames.get(7),
            keyColumnNames.get(8), keyColumnNames.get(9)),
        2);

    String tableName = tablePrefix + "-" + System.currentTimeMillis();
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);

    final int keyColumnCount = schema.getPrimaryKeyColumnCount();
    assertEquals(keyColumnNumber, keyColumnCount);
    List<ReturnValueHelper> helpList = new ArrayList<>();
    for (int index = 1; index <= totalCount; index++) {
      List<List<Integer>> testCases = new ArrayList<>();
      Random r = new Random(System.currentTimeMillis());
      for (int i = 0; i < keyColumnCount; i++) {
        int inListLength = 1 + r.nextInt(inListMaxLength);
        List<Integer> testCase = new ArrayList<>();
        for (int j = 0; j < inListLength; j++) {
          testCase.add(r.nextInt());
        }
        testCases.add(testCase);
      }

      KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
      Schema scanSchema = scanBuilder.table.getSchema();
      PartitionSchema partitionSchema = scanBuilder.table.getPartitionSchema();
      for (int i = 0; i < keyColumnCount; i++) {
        KuduPredicate pred = KuduPredicate.newInListPredicate(keyColumnSchemas.get(i),
            ImmutableList.copyOf(testCases.get(i)));
        scanBuilder.addPredicate(pred);
      }
      helpList.add(new ReturnValueHelper(scanSchema, partitionSchema, scanBuilder.predicates));
    }
    return helpList;
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
      if (!pruner.shouldPruneForTests(partition)) {
        scannedPartitions++;
      }
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
      if (!pruner.shouldPruneForTests(partition)) {
        scannedPartitions++;
      }
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

    final byte min = Byte.MIN_VALUE;
    final byte max = Byte.MAX_VALUE;

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
    final Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<>());
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
    final Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<>());
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

    // a in [0, 1, 2], b in [0, 1, 2], c in [0, 1, 2];
    checkPartitions(8, 8, table, partitions,
        KuduPredicate.newInListPredicate(a, ImmutableList.of((byte) 0, (byte) 1, (byte)2)),
        KuduPredicate.newInListPredicate(b, ImmutableList.of((byte) 0, (byte) 1, (byte)2)),
        KuduPredicate.newInListPredicate(c, ImmutableList.of((byte) 0, (byte) 1, (byte)2)));

    // a in [0, 1, 2, 3], b in [0, 1, 2, 3], c in [0, 1, 2, 3];
    checkPartitions(8, 8, table, partitions,
        KuduPredicate.newInListPredicate(
            a, ImmutableList.of((byte) 0, (byte) 1, (byte) 2, (byte) 3)),
        KuduPredicate.newInListPredicate(
            b, ImmutableList.of((byte) 0, (byte) 1, (byte) 2, (byte) 3)),
        KuduPredicate.newInListPredicate(
            c, ImmutableList.of((byte) 0, (byte) 1, (byte) 2, (byte) 3)));

    // The following test cases, we give more tests to make sure its correctess.
    {
      List<List<Integer>> expectedList = new ArrayList<>();
      expectedList.add(ImmutableList.of(1, 1));
      expectedList.add(ImmutableList.of(8, 8));
      expectedList.add(ImmutableList.of(8, 8));
      expectedList.add(ImmutableList.of(8, 8));
      expectedList.add(ImmutableList.of(27, 1));
      expectedList.add(ImmutableList.of(27, 1));
      expectedList.add(ImmutableList.of(27, 1));
      expectedList.add(ImmutableList.of(27, 1));
      expectedList.add(ImmutableList.of(27, 1));
      expectedList.add(ImmutableList.of(27, 1));

      for (int size = 1; size <= 10; size++) {
        int columnCount = schema.getColumnCount();
        List<List<Byte>> testCases = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
          List<Byte> testCase = new ArrayList<>();
          for (int j = 0; j < size; j++) {
            testCase.add((byte) j);
          }
          testCases.add(testCase);
        }

        KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);

        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(a);
        columnSchemas.add(b);
        columnSchemas.add(c);
        KuduPredicate[] predicates = new KuduPredicate[3];
        for (int i = 0; i < 3; i++) {
          predicates[i] = KuduPredicate.newInListPredicate(columnSchemas.get(i),
              ImmutableList.copyOf(testCases.get(i)));
          scanBuilder.addPredicate(predicates[i]);
        }
        checkPartitions(expectedList.get(size - 1).get(0),
            expectedList.get(size - 1).get(1), table, partitions, predicates);
        Schema scanSchema = scanBuilder.table.getSchema();
        PartitionSchema partitionSchema = scanBuilder.table.getPartitionSchema();

        List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            partitionSchema.getHashBucketSchemas();
        assertEquals(columnCount, hashBucketSchemas.size());
        for (PartitionSchema.HashBucketSchema hashSchema : hashBucketSchemas) {
          BitSet oldBitset = pruneHashComponent(scanSchema, hashSchema,
              scanBuilder.predicates);
          BitSet newBitset = PartitionPruner.pruneHashComponentV2ForTest(scanSchema, hashSchema,
              scanBuilder.predicates);
          Assert.assertEquals(oldBitset, newBitset);
        }
      }
    }
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
    final Schema schema = new Schema(ImmutableList.of(a, b, c));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(new ArrayList<>());
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

  // This unit test to make sure to correctness of the newer algorithm compare
  // with older algorithm. Generate a random list of values and make sure they can not cause
  // OOM condition and check the two algorithms' results are the same.
  // At the same time, compare the performance of the two algorithms.
  @Test
  public void testInListHashPartitionPruningUsingLargeList() throws Exception {
    // To test normal case, this unit test should not go out of memory, so
    // set totalCount = 100 and inListMaxLength = 10;
    List<ReturnValueHelper> returnValues = this.prepareForLargeInListPredicates(client,
        "testInListHashPartitionPruningUsingLargeList", 100, 10);
    for (ReturnValueHelper returnValue : returnValues) {
      long v1CostMillis = 0;
      long v2CostMillis = 0;
      long combinationCount = 1;
      for (PartitionSchema.HashBucketSchema hashSchema :
          returnValue.partitionSchema.getHashBucketSchemas()) {
        long startTime = System.currentTimeMillis();
        final BitSet oldBitset = pruneHashComponent(
            returnValue.schema, hashSchema, returnValue.predicates);
        v1CostMillis += (System.currentTimeMillis() - startTime);
        startTime = System.currentTimeMillis();
        final BitSet newBitset = PartitionPruner.pruneHashComponentV2ForTest(
            returnValue.schema, hashSchema, returnValue.predicates);
        v2CostMillis += (System.currentTimeMillis() - startTime);
        Assert.assertEquals(oldBitset, newBitset);
        combinationCount *= returnValue.predicates.size();
      }
      // v2 algorithm is more efficient than v1 algorithm.
      // The following logs can compare v2 and v1.
      // v2 (new algorithm) is quicker 100x than v1 (older one).
      if (v2CostMillis != 0 && v1CostMillis != 0) {
        LOG.info("combination_count: {}, old algorithm " +
            "cost: {}ms, new algorithm cost: {}ms, speedup: {}",
            combinationCount, v1CostMillis, v2CostMillis,
            (double) v1CostMillis / v2CostMillis);
      }
    }
  }

  // The unit test to make sure that very long in-list predicates can cause
  // OOM condition in v1 algorithm(older one), and using v2 algorithm(newer one) can solve it.
  // For details on testing for the OOM condition, see the in-line
  // TODO comment in the end this scenario.
  @Test
  public void testInListHashPartitionPruningUsingLargeListOOM() throws Exception {
    // To test OOM case, set totalCount = 10 and inListMaxLength = 100;
    List<ReturnValueHelper> returnValues = this.prepareForLargeInListPredicates(client,
        "testInListHashPartitionPruningUsingLargeListOOM", 10, 100);
    for (ReturnValueHelper returnValue : returnValues) {
      for (PartitionSchema.HashBucketSchema hashSchema :
          returnValue.partitionSchema.getHashBucketSchemas()) {
        // TODO(duyuqi)
        // How to add a test case for the oom?
        // Comments:
        // the org.apache.kudu.client.TestPartitionPruner >
        //   testInListHashPartitionPruningUsingLargeListOOM FAILED
        //   java.lang.OutOfMemoryError: GC overhead limit exceeded
        // PartitionPruner.pruneHashComponentV1ForTest(scanSchema, hashSchema,
        //     scanBuilder.predicates);
        PartitionPruner.pruneHashComponentV2ForTest(returnValue.schema, hashSchema,
            returnValue.predicates);
      }
    }
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

    ColumnSchema host =
        new ColumnSchema.ColumnSchemaBuilder("host", Type.STRING).key(true).build();
    ColumnSchema metric =
        new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).key(true).build();
    ColumnSchema timestamp =
        new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS)
            .key(true).build();
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

  @Test(timeout = 100000)
  public void testPruningWithCustomHashSchemas() throws Exception {
    // CREATE TABLE timeseries
    //   (host STRING, metric STRING, timestamp UNIXTIME_MICROS, value DOUBLE)
    // PRIMARY KEY (host, metric, timestamp)
    //
    //    RANGE(timestamp)
    //        (PARTITION VALUES >= 0,
    //         PARTITION VALUES <  100)
    //    HASH (host, metric) 2 PARTITIONS,
    //
    //    RANGE(timestamp)
    //        (PARTITION VALUES >= 100,
    //         PARTITION VALUES <  200)
    //    HASH (host) 5 PARTITIONS

    ColumnSchema host =
        new ColumnSchema.ColumnSchemaBuilder("host", Type.STRING).key(true).build();
    ColumnSchema metric =
        new ColumnSchema.ColumnSchemaBuilder("metric", Type.STRING).key(true).build();
    ColumnSchema timestamp =
        new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS)
            .key(true).build();
    ColumnSchema value = new ColumnSchema.ColumnSchemaBuilder("value", Type.DOUBLE).build();
    final Schema schema = new Schema(ImmutableList.of(host, metric, timestamp, value));

    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("timestamp"));

    // Add range partition with table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("timestamp", 0);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("timestamp", 100);
      tableBuilder.addRangePartition(lower, upper);
    }

    // Add range partition with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("timestamp", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("timestamp", 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("host"), 5, 0);

      tableBuilder.addRangePartition(rangePartition);
    }

    // Add table-wide hash schema.
    tableBuilder.addHashPartitions(ImmutableList.of("host", "metric"), 2);

    String tableName = "testPruningCHS";
    client.createTable(tableName, schema, tableBuilder);
    KuduTable table = client.openTable(tableName);

    final List<Partition> partitions = getTablePartitions(table);
    assertEquals(7, partitions.size());

    // No Predicates
    checkPartitions(7, 9, table, partitions);

    checkPartitions(7, 7, table, partitions,
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 50),
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.LESS, 150));

    // host = "a"
    // 2 tablets from the HASH(host, metric) range and 1 from the HASH(host) range.
    checkPartitions(3, 5, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"));

    // host = "a"
    // metric = "a"
    checkPartitions(2, 3, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"));

    // host = "a"
    // metric = "a"
    // timestamp >= 9;
    checkPartitions(2, 3, table, partitions,
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
    // timestamp >= 100;
    // timestamp < 200;
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
    // timestamp < 100;
    checkPartitions(1, 1, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.LESS, 100));

    // host = "a"
    // metric = "a"
    // timestamp >= 10;
    checkPartitions(2, 3, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 10));

    // host = "a"
    // metric = "a"
    // timestamp >= 100;
    checkPartitions(1, 2, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.GREATER_EQUAL, 100));

    // host = "a"
    // metric = "a"
    // timestamp = 100;
    checkPartitions(1, 1, table, partitions,
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(metric, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 100));

    final byte[] hash1 = new byte[] { 0, 0, 0, 1 };

    // partition key < (hash=1)
    // scan partitions: 1 + 1 + 1
    checkPartitions(2, 3, table, partitions, null, hash1);

    // partition key >= (hash=1)
    // scan partitions: 1 + 4 + 1
    checkPartitions(5, 6, table, partitions, hash1, null);

    // timestamp = 10
    // partition key < (hash=1)
    // scan partitions: 0 + 1 + 0
    checkPartitions(1, 1, table, partitions, null, hash1,
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 10));

    // timestamp = 10
    // partition key >= (hash=1)
    checkPartitions(1, 1, table, partitions, hash1, null,
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 10));

    // timestamp = 100
    // partition key >= (hash=1)
    checkPartitions(4, 4, table, partitions, hash1, null,
        KuduPredicate.newComparisonPredicate(timestamp, ComparisonOp.EQUAL, 100));

    // timestamp IN (99, 100)
    // host = "a"
    // metric IN ("foo", "baz")
    checkPartitions(2, 2, table, partitions,
        KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(99L, 100L)),
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newInListPredicate(metric, ImmutableList.of("foo", "baz")));

    // timestamp IN (100, 199)
    // host = "a"
    // metric IN ("foo", "baz")
    checkPartitions(1, 1, table, partitions,
        KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(100L, 199L)),
        KuduPredicate.newComparisonPredicate(host, ComparisonOp.EQUAL, "a"),
        KuduPredicate.newInListPredicate(metric, ImmutableList.of("foo", "baz")));

    // timestamp IN (0, 10)
    checkPartitions(2, 2, table, partitions,
        KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(0L, 10L)));

    // timestamp IN (100, 110)
    checkPartitions(5, 5, table, partitions,
        KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(100L, 110L)));

    // timestamp IN (99, 100)
    checkPartitions(7, 7, table, partitions,
        KuduPredicate.newInListPredicate(timestamp, ImmutableList.of(99L, 100L)));

    // timestamp IS NOT NULL
    checkPartitions(7, 9, table, partitions,
        KuduPredicate.newIsNotNullPredicate(timestamp));

    // timestamp IS NULL
    checkPartitions(0, 0, table, partitions,
        KuduPredicate.newIsNullPredicate(timestamp));
  }
}
