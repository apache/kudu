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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common.DataType;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Operation.ChangeType;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.RandomUtils;
import org.apache.kudu.util.DataGenerator;
import org.apache.kudu.util.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.kudu.client.AsyncKuduScanner.DEFAULT_IS_DELETED_COL_NAME;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.loadDefaultTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestKuduScanner {
  private static final Logger LOG = LoggerFactory.getLogger(TestScannerMultiTablet.class);

  private static final String tableName = "TestKuduScanner";

  private static final int DIFF_FLUSH_SEC = 1;

  private KuduClient client;
  private Random random;
  private DataGenerator generator;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    random = RandomUtils.getRandom();
    generator = new DataGenerator.DataGeneratorBuilder()
        .random(random)
        .build();
  }

  @Test(timeout = 100000)
  public void testIterable() throws Exception {
    KuduTable table = client.createTable(tableName, getBasicSchema(), getBasicCreateTableOptions());
    DataGenerator generator = new DataGenerator.DataGeneratorBuilder()
        .random(RandomUtils.getRandom())
        .build();
    KuduSession session = client.newSession();
    List<Integer> insertKeys = new ArrayList<>();
    int numRows = 10;
    for (int i = 0; i < numRows; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      generator.randomizeRow(row);
      insertKeys.add(row.getInt(0));
      session.apply(insert);
    }

    // Ensure that when an enhanced for-loop is used, there's no sharing of RowResult objects.
    KuduScanner scanner = client.newScannerBuilder(table).build();
    Set<RowResult> results = new HashSet<>();
    Set<Integer> resultKeys = new HashSet<>();
    for (RowResult rowResult : scanner) {
      results.add(rowResult);
      resultKeys.add(rowResult.getInt(0));
    }
    assertEquals(numRows, results.size());
    assertTrue(resultKeys.containsAll(insertKeys));

    // Ensure that when the reuseRowResult optimization is set, only a single RowResult is used.
    KuduScanner reuseScanner = client.newScannerBuilder(table).build();
    reuseScanner.setReuseRowResult(true);
    Set<RowResult> reuseResult = new HashSet<>();
    for (RowResult rowResult : reuseScanner) {
      reuseResult.add(rowResult);
    }
    // Ensure the same RowResult object is reused.
    assertEquals(1, reuseResult.size());
  }

  @Test(timeout = 100000)
  @KuduTestHarness.TabletServerConfig(flags = {
      "--scanner_ttl_ms=5000",
      "--scanner_gc_check_interval_us=500000"}) // 10% of the TTL.
  public void testKeepAlive() throws Exception {
    int rowCount = 500;
    int shortScannerTtlMs = 5000;

    // Create a simple table with a single partition.
    Schema tableSchema = new Schema(Collections.singletonList(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build()
    ));

    CreateTableOptions tableOptions = new CreateTableOptions()
        .setRangePartitionColumns(Collections.singletonList("key"))
        .setNumReplicas(1);
    KuduTable table = client.createTable(tableName, tableSchema, tableOptions);

    KuduSession session = client.newSession();
    for (int i = 0; i < rowCount; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt(0, i);
      session.apply(insert);
    }

    // Test that a keepAlivePeriodMs less than the scanner ttl is successful.
    KuduScanner goodScanner = client.newScannerBuilder(table)
        .batchSizeBytes(100) // Set a small batch size so the first scan doesn't read all the rows.
        .keepAlivePeriodMs(shortScannerTtlMs / 4)
        .build();
    processKeepAliveScanner(goodScanner, shortScannerTtlMs);

    // Test that a keepAlivePeriodMs greater than the scanner ttl fails.
    KuduScanner badScanner = client.newScannerBuilder(table)
        .batchSizeBytes(100) // Set a small batch size so the first scan doesn't read all the rows.
        .keepAlivePeriodMs(shortScannerTtlMs * 2)
        .build();
    try {
      processKeepAliveScanner(badScanner, shortScannerTtlMs);
      fail("Should throw a scanner not found exception");
    } catch (RuntimeException ex) {
      assertTrue(ex.getMessage().matches(".*Scanner .* not found.*"));
    }
  }

  private void processKeepAliveScanner(KuduScanner scanner, int scannerTtlMs) throws Exception {
    int i = 0;
    // Ensure reading takes longer than the scanner ttl.
    for (RowResult unused : scanner) {
      // Sleep for half the ttl for the first few rows. This ensures
      // we are on the same tablet and will go past the ttl without
      // a new scan request. It also ensures a single row doesn't go
      // longer than the ttl.
      if (i < 5) {
        Thread.sleep(scannerTtlMs / 2); // Sleep for half the ttl.
        i++;
      }
    }
  }

  @Test(timeout = 100000)
  public void testOpenScanWithDroppedPartition() throws Exception {
    // Create a table with 2 range partitions.
    Schema basicSchema = getBasicSchema();
    String tableName = "testOpenScanWithDroppedPartition";
    PartialRow bottom = basicSchema.newPartialRow();
    bottom.addInt("key", 0);
    PartialRow middle = basicSchema.newPartialRow();
    middle.addInt("key", 1000);
    PartialRow top = basicSchema.newPartialRow();
    top.addInt("key", 2000);

    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(Collections.singletonList("key"));
    createOptions.addRangePartition(bottom, middle);
    createOptions.addRangePartition(middle, top);
    KuduTable table = client.createTable(tableName, basicSchema, createOptions);

    // Load rows into both partitions.
    int numRows = 1999;
    loadDefaultTable(client, tableName, numRows);

    // Scan the rows while dropping a partition.
    KuduScanner scanner = client.newScannerBuilder(table)
        .batchSizeBytes(100) // Set a small batch size so the first scan doesn't read all the rows.
        .build();

    int rowsScanned = 0;
    int batchNum = 0;
    while (scanner.hasMoreRows()) {
      if (batchNum == 1) {
        CapturingLogAppender capture = new CapturingLogAppender();
        // Drop the partition.
        try (Closeable unused = capture.attach()) {
          client.alterTable(tableName,
              new AlterTableOptions().dropRangePartition(bottom, middle));
          // Give time for the background drop operations.
          Thread.sleep(1000);
        }
        // Verify the partition was dropped.
        KuduPartitioner partitioner =
            new KuduPartitioner.KuduPartitionerBuilder(table).build();
        assertEquals("The partition was not dropped", 1, partitioner.numPartitions());
        assertTrue(capture.getAppendedText().contains("Deleting tablet data"));
        assertTrue(capture.getAppendedText().contains("successfully deleted"));
      }
      rowsScanned += scanner.nextRows().getNumRows();
      batchNum++;
    }

    assertTrue("All messages were consumed in the first batch", batchNum > 1);
    assertEquals("Some message were not consumed", numRows, rowsScanned);
  }

  @Test(timeout = 100000)
  @KuduTestHarness.TabletServerConfig(flags = { "--flush_threshold_secs=" + DIFF_FLUSH_SEC })
  public void testDiffScan() throws Exception {
    Schema schema = new Schema(Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        // Include a column with the default IS_DELETED column name to test collision handling.
        new ColumnSchema.ColumnSchemaBuilder(DEFAULT_IS_DELETED_COL_NAME, Type.INT32).build()
    ));

    KuduTable table = client.createTable(tableName, schema, getBasicCreateTableOptions());

    // Generate some rows before the start time. Ensure there's at least one insert.
    int beforeBounds = 5;
    int numInserts = RandomUtils.nextIntInRange(random, 1, beforeBounds);
    int numUpdates = random.nextInt(beforeBounds);
    int numDeletes = random.nextInt(beforeBounds);
    List<Operation> beforeOps =
        generateMutationOperations(table, numInserts, numUpdates, numDeletes);
    Map<Integer, ChangeType> before = applyOperations(beforeOps);
    LOG.info("Before: {}", before);

    // Set the start timestamp after the initial mutations by getting the propagated timestamp,
    // and incrementing by 1.
    long startHT = client.getLastPropagatedTimestamp() + 1;
    LOG.info("startHT: {}", startHT);

    // Generate row mutations. The mutations performed here are what should be seen by the diff scan.
    int mutationBounds = 10;
    int expectedNumInserts = random.nextInt(mutationBounds);
    int expectedNumUpdates = random.nextInt(mutationBounds);
    int expectedNumDeletes = random.nextInt(mutationBounds);
    List<Operation> operations =
        generateMutationOperations(table, expectedNumInserts, expectedNumUpdates, expectedNumDeletes);
    Map<Integer, ChangeType> mutations = applyOperations(operations);
    LOG.info("Mutations: {}", mutations);

    // Set the end timestamp after the test mutations by getting the propagated timestamp,
    // and incrementing by 1.
    long endHT = client.getLastPropagatedTimestamp() + 1;
    LOG.info("endHT: {}", endHT);

    // Generate some rows after the end time.
    int afterBounds = 5;
    numInserts = random.nextInt(afterBounds);
    numUpdates = random.nextInt(afterBounds);
    numDeletes = random.nextInt(afterBounds);
    List<Operation> afterOps =
        generateMutationOperations(table, numInserts, numUpdates, numDeletes);
    Map<Integer, ChangeType> after = applyOperations(afterOps);
    LOG.info("After: {}", after);

    // Diff scan the time range.
    // Pass through the scan token API to ensure serialization of tokens works too.
    List<KuduScanToken> tokens = client.newScanTokenBuilder(table)
        .diffScan(startHT, endHT)
        .build();
    List<RowResult> results = new ArrayList<>();
    for (KuduScanToken token : tokens) {
      KuduScanner scanner = KuduScanToken.deserializeIntoScanner(token.serialize(), client);

      // Verify the IS_DELETED column is appended at the end of the projection.
      Schema projection = scanner.getProjectionSchema();
      int isDeletedIndex = projection.getIsDeletedIndex();
      assertEquals(projection.getColumnCount() - 1, isDeletedIndex);
      // Verify the IS_DELETED column has the correct types.
      ColumnSchema isDeletedCol = projection.getColumnByIndex(isDeletedIndex);
      assertEquals(Type.BOOL, isDeletedCol.getType());
      assertEquals(DataType.IS_DELETED, isDeletedCol.getWireType());
      // Verify the IS_DELETED column is named to avoid collision.
      assertEquals(projection.getColumnByIndex(isDeletedIndex),
          projection.getColumn(DEFAULT_IS_DELETED_COL_NAME + "_"));

      for (RowResult row : scanner) {
        results.add(row);
      }
    }
    assertEquals(mutations.size(), results.size());

    // Count the results and verify their change type.
    int resultNumInserts = 0;
    int resultNumUpdates = 0;
    int resultNumDeletes = 0;
    int resultExtra = 0;
    for (RowResult result : results) {
      Integer key = result.getInt(0);
      LOG.info("Processing key {}", key);
      ChangeType type = mutations.get(key);
      if (type == ChangeType.INSERT) {
        assertFalse(result.isDeleted());
        resultNumInserts++;
      } else if (type == ChangeType.UPDATE) {
        assertFalse(result.isDeleted());
        resultNumUpdates++;
      } else if (type == ChangeType.DELETE) {
        assertTrue(result.isDeleted());
        resultNumDeletes++;
      } else {
        // The key was not found in the mutations map. This means that we somehow managed to scan
        // a row that was never mutated. It's an error and will trigger an assert below.
        assertNull(type);
        resultExtra++;
      }
    }
    assertEquals(expectedNumInserts, resultNumInserts);
    assertEquals(expectedNumUpdates, resultNumUpdates);
    assertEquals(expectedNumDeletes, resultNumDeletes);
    assertEquals(0, resultExtra);
  }

  /**
   * Applies a list of Operations and returns the final ChangeType for each key.
   * @param operations the operations to apply.
   * @return a map of each key and its final ChangeType.
   */
  private Map<Integer, ChangeType> applyOperations(List<Operation> operations) throws Exception {
    Map<Integer, ChangeType> results = new HashMap<>();
    // If there are no operations, return early.
    if (operations.isEmpty()) {
      return results;
    }
    KuduSession session = client.newSession();
    // On some runs, wait long enough to flush at the start.
    if (random.nextBoolean()) {
      LOG.info("Waiting for a flush at the start of applyOperations");
      Thread.sleep(DIFF_FLUSH_SEC + 1);
    }

    // Pick an int as a flush indicator so we flush once on average while applying operations.
    int flushInt = random.nextInt(operations.size());
    for (Operation op : operations) {
      // On some runs, wait long enough to flush while applying operations.
      if (random.nextInt(operations.size()) == flushInt) {
        LOG.info("Waiting for a flush in the middle of applyOperations");
        Thread.sleep(DIFF_FLUSH_SEC + 1);
      }
      OperationResponse resp = session.apply(op);
      if (resp.hasRowError()) {
        LOG.error("Could not mutate row: " + resp.getRowError().getErrorStatus());
      }
      assertFalse(resp.hasRowError());
      results.put(op.getRow().getInt(0), op.getChangeType());
    }
    return results;
  }

  /**
   * Generates a list of random mutation operations. Any unique row, identified by
   * it's key, could have a random number of operations/mutations. However, the
   * target count of numInserts, numUpdates and numDeletes will always be achieved
   * if the entire list of operations is processed.
   *
   * @param table the table to generate operations for
   * @param numInserts The number of row mutations to end with an insert
   * @param numUpdates The number of row mutations to end with an update
   * @param numDeletes The number of row mutations to end with an delete
   * @return a list of random mutation operations
   */
  private List<Operation> generateMutationOperations(
      KuduTable table, int numInserts, int numUpdates, int numDeletes) throws Exception {

    List<Operation> results = new ArrayList<>();
    List<MutationState> unfinished = new ArrayList<>();
    int minMutationsBound = 5;

    // Generate Operations to initialize all of the row with inserts.
    List<Pair<ChangeType, Integer>> changeCounts = Arrays.asList(
        new Pair<>(ChangeType.INSERT, numInserts),
        new Pair<>(ChangeType.UPDATE, numUpdates),
        new Pair<>(ChangeType.DELETE, numDeletes));
    for (Pair<ChangeType, Integer> changeCount : changeCounts) {
      ChangeType type = changeCount.getFirst();
      int count = changeCount.getSecond();
      for (int i = 0; i < count; i++) {
        // Generate a random insert.
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        generator.randomizeRow(row);
        int key = row.getInt(0);
        // Add the insert to the results.
        results.add(insert);
        // Initialize the unfinished MutationState.
        unfinished.add(new MutationState(key, type, random.nextInt(minMutationsBound)));
      }
    }

    // Randomly pull from the unfinished list, mutate it and add that operation to the results.
    // If it has been mutated at least the minimum number of times, remove it from the unfinished
    // list.
    while (!unfinished.isEmpty()) {
      // Get a random row to mutate.
      int index = random.nextInt(unfinished.size());
      MutationState state = unfinished.get(index);

      // If the row is done, remove it from unfinished and continue.
      if (state.numMutations >= state.minMutations && state.currentType == state.endType) {
        unfinished.remove(index);
        continue;
      }

      // Otherwise, generate an operation to mutate the row based on it's current ChangeType.
      //    insert -> update|delete
      //    update -> update|delete
      //    delete -> insert
      Operation op;
      if (state.currentType == ChangeType.INSERT || state.currentType == ChangeType.UPDATE) {
        op = random.nextBoolean() ? table.newUpdate() : table.newDelete();
      } else {
        // Must be a delete, so we need an insert next.
        op = table.newInsert();
      }
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, state.key);
      generator.randomizeRow(row, /* randomizeKeys */ false);
      op.setRow(row);
      results.add(op);

      state.currentType = op.getChangeType();
      state.numMutations++;
    }

    return results;
  }

  private static class MutationState {
    final int key;
    final ChangeType endType;
    final int minMutations;

    ChangeType currentType = ChangeType.INSERT;
    int numMutations = 0;

    MutationState(int key, ChangeType endType, int minMutations) {
      this.key = key;
      this.endType = endType;
      this.minMutations = minMutations;
    }
  }
}
