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
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.countScanTokenRows;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.loadDefaultTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestScanToken {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClient.class);

  private static final String testTableName = "TestScanToken";

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  /**
   * Tests scan tokens by creating a set of scan tokens, serializing them, and
   * then executing them in parallel with separate client instances. This
   * simulates the normal usecase of scan tokens being created at a central
   * planner and distributed to remote task executors.
   */
  @Test
  public void testScanTokens() throws Exception {
    int saveFetchTablets = AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP;
    try {
      // For this test, make sure that we cover the case that not all tablets
      // are returned in a single batch.
      AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP = 4;

      Schema schema = createManyStringsSchema();
      CreateTableOptions createOptions = new CreateTableOptions();
      createOptions.addHashPartitions(ImmutableList.of("key"), 8);

      PartialRow splitRow = schema.newPartialRow();
      splitRow.addString("key", "key_50");
      createOptions.addSplitRow(splitRow);

      client.createTable(testTableName, schema, createOptions);

      KuduSession session = client.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      KuduTable table = client.openTable(testTableName);
      for (int i = 0; i < 100; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("key", String.format("key_%02d", i));
        row.addString("c1", "c1_" + i);
        row.addString("c2", "c2_" + i);
        session.apply(insert);
      }
      session.flush();

      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
      tokenBuilder.batchSizeBytes(0);
      tokenBuilder.setProjectedColumnIndexes(ImmutableList.of());
      List<KuduScanToken> tokens = tokenBuilder.build();
      assertEquals(16, tokens.size());

      // KUDU-1809, with batchSizeBytes configured to '0',
      // the first call to the tablet server won't return
      // any data.
      {
        KuduScanner scanner = tokens.get(0).intoScanner(client);
        assertEquals(0, scanner.nextRows().getNumRows());
      }

      for (KuduScanToken token : tokens) {
        // Sanity check to make sure the debug printing does not throw.
        LOG.debug(KuduScanToken.stringifySerializedToken(token.serialize(), client));
      }
    } finally {
      AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP = saveFetchTablets;
    }
  }

  /**
   * Tests scan token creation and execution on a table with non-covering range partitions.
   */
  @Test
  public void testScanTokensNonCoveringRangePartitions() throws Exception {
    Schema schema = createManyStringsSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(ImmutableList.of("key"), 2);

    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    lower.addString("key", "a");
    upper.addString("key", "f");
    createOptions.addRangePartition(lower, upper);

    lower = schema.newPartialRow();
    upper = schema.newPartialRow();
    lower.addString("key", "h");
    upper.addString("key", "z");
    createOptions.addRangePartition(lower, upper);

    PartialRow split = schema.newPartialRow();
    split.addString("key", "k");
    createOptions.addSplitRow(split);

    client.createTable(testTableName, schema, createOptions);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = client.openTable(testTableName);
    for (char c = 'a'; c < 'f'; c++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", "" + c);
      row.addString("c1", "c1_" + c);
      row.addString("c2", "c2_" + c);
      session.apply(insert);
    }
    for (char c = 'h'; c < 'z'; c++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", "" + c);
      row.addString("c1", "c1_" + c);
      row.addString("c2", "c2_" + c);
      session.apply(insert);
    }
    session.flush();

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    tokenBuilder.setProjectedColumnIndexes(ImmutableList.of());
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(6, tokens.size());
    assertEquals('f' - 'a' + 'z' - 'h',
                 countScanTokenRows(tokens,
                     client.getMasterAddressesAsString(),
                     client.getDefaultOperationTimeoutMs()));

    for (KuduScanToken token : tokens) {
      // Sanity check to make sure the debug printing does not throw.
      LOG.debug(KuduScanToken.stringifySerializedToken(token.serialize(), client));
    }
  }

  /**
   * Tests the results of creating scan tokens, altering the columns being
   * scanned, and then executing the scan tokens.
   */
  @Test
  public void testScanTokensConcurrentAlterTable() throws Exception {
    Schema schema = new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64).nullable(false).key(false).build()
    ));
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    client.createTable(testTableName, schema, createOptions);

    KuduTable table = client.openTable(testTableName);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());
    KuduScanToken token = tokens.get(0);

    // Drop a column
    client.alterTable(testTableName, new AlterTableOptions().dropColumn("a"));
    try {
      token.intoScanner(client);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown column"));
    }

    // Add a column with the same name, type, and nullability. It will have a different id-- it's a
    // different column-- so the scan token will fail.
    client.alterTable(
        testTableName,
        new AlterTableOptions()
            .addColumn(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64)
                .nullable(false)
                .defaultValue(0L).build()));
    try {
      token.intoScanner(client);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains(
          "Unknown column"));
    }
  }

  /**
   * Tests that it is possible to create a scan token, rename a column, and rehydrate a scanner from
   * the scan token with the old column name.
   */
  @Test
  public void testScanTokensConcurrentColumnRename() throws Exception {
    Schema schema = getBasicSchema();
    String oldColName = schema.getColumnByIndex(1).getName();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    client.createTable(testTableName, schema, createOptions);

    KuduTable table = client.openTable(testTableName);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());
    KuduScanToken token = tokens.get(0);

    // Rename a column.
    String newColName = "new-name";
    client.alterTable(testTableName, new AlterTableOptions().renameColumn(oldColName, newColName));

    KuduScanner scanner = token.intoScanner(client);

    // TODO(wdberkeley): Handle renaming a column between when the token is rehydrated as a scanner
    //  and when the scanner first hits a replica. Note that this is almost certainly a very
    //  short period of vulnerability.

    assertEquals(0, countRowsInScan(scanner));

    // Test that the old name cannot be used and the new name can be.
    Schema alteredSchema = scanner.getProjectionSchema();
    try {
      alteredSchema.getColumn(oldColName);
      fail();
    } catch (IllegalArgumentException ex) {
      // Good.
    }
    alteredSchema.getColumn(newColName);
  }

  /**
   * Tests that it is possible to rehydrate a scan token after a table rename.
   */
  @Test
  public void testScanTokensWithTableRename() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());
    KuduScanToken token = tokens.get(0);

    // Rename the table.
    client.alterTable(
        testTableName,
        new AlterTableOptions().renameTable(testTableName + "-renamed"));

    assertEquals(0, countRowsInScan(token.intoScanner(client)));
  }

  /**
   * Tests scan token creation and execution on a table with interleaved range partition drops.
   */
  @Test
  public void testScanTokensInterleavedRangePartitionDrops() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(ImmutableList.of("key"), 2);

    final int numRows = 30;
    PartialRow lower0 = schema.newPartialRow();
    PartialRow upper0 = schema.newPartialRow();
    lower0.addInt("key", 0);
    upper0.addInt("key", numRows / 3);
    createOptions.addRangePartition(lower0, upper0);

    PartialRow lower1 = schema.newPartialRow();
    PartialRow upper1 = schema.newPartialRow();
    lower1.addInt("key", numRows / 3);
    upper1.addInt("key", 2 * numRows / 3);
    createOptions.addRangePartition(lower1, upper1);

    PartialRow lower2 = schema.newPartialRow();
    PartialRow upper2 = schema.newPartialRow();
    lower2.addInt("key", 2 * numRows / 3);
    upper2.addInt("key", numRows);
    createOptions.addRangePartition(lower2, upper2);

    KuduTable table = client.createTable(testTableName, schema, createOptions);
    KuduSession session = client.newSession();
    for (int i = 0; i < numRows; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    // Build the scan tokens.
    List<KuduScanToken> tokens = client.newScanTokenBuilder(table).build();
    assertEquals(6, tokens.size());

    // Drop the range partition [10, 20).
    AlterTableOptions dropMiddleOptions = new AlterTableOptions();
    dropMiddleOptions.dropRangePartition(lower1, upper1);
    client.alterTable(table.getName(), dropMiddleOptions);

    // Rehydrate the tokens.
    List<KuduScanner> scanners = new ArrayList<>();
    for (KuduScanToken token : tokens) {
      scanners.add(token.intoScanner(client));
    }

    // Drop the range partition [20, 30).
    AlterTableOptions dropEndOptions = new AlterTableOptions();
    dropEndOptions.dropRangePartition(lower2, upper2);
    client.alterTable(table.getName(), dropEndOptions);

    // Check the scanners work. The scanners for the tablets in the range [10, 20) definitely won't
    // see any rows. The scanners for the tablets in the range [20, 30) might see rows.
    int scannedRows = 0;
    for (KuduScanner scanner : scanners) {
      scannedRows += countRowsInScan(scanner);
    }
    assertTrue(String.format("%d >= %d / 3?", scannedRows, numRows), scannedRows >= numRows / 3);
    assertTrue(String.format("%d <= 2 * %d / 3?", scannedRows, numRows), scannedRows <= 2 * numRows / 3);
  }

  /** Test that scanRequestTimeout makes it from the scan token to the underlying Scanner class. */
  @Test
  public void testScanRequestTimeout() throws IOException {
    final int NUM_ROWS_DESIRED = 100;
    final int SCAN_REQUEST_TIMEOUT_MS = 20;
    KuduTable table = createDefaultTable(client, testTableName);
    loadDefaultTable(client, testTableName, NUM_ROWS_DESIRED);
    KuduScanToken.KuduScanTokenBuilder builder =
        new KuduScanToken.KuduScanTokenBuilder(asyncClient, table);
    builder.scanRequestTimeout(SCAN_REQUEST_TIMEOUT_MS);
    List<KuduScanToken> tokens = builder.build();
    for (KuduScanToken token : tokens) {
      byte[] serialized = token.serialize();
      KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serialized, client);
      assertEquals(SCAN_REQUEST_TIMEOUT_MS, scanner.getScanRequestTimeout());
    }
  }

  // Helper for scan token tests that use diff scan.
  private long setupTableForDiffScans(KuduClient client,
                                      KuduTable table,
                                      int numRows) throws Exception {
    KuduSession session = client.newSession();
    for (int i = 0 ; i < numRows / 2; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    // Grab the timestamp, then add more data so there's a diff.
    long timestamp = client.getLastPropagatedTimestamp();
    for (int i = numRows / 2; i < numRows; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    // Delete some data so the is_deleted column can be tested.
    for (int i = 0; i < numRows / 4; i++) {
      Delete delete = table.newDelete();
      PartialRow row = delete.getRow();
      row.addInt(0, i);
      session.apply(delete);
    }

    return timestamp;
  }

  // Helper to check diff scan results.
  private void checkDiffScanResults(KuduScanner scanner,
                                    int numExpectedMutations,
                                    int numExpectedDeletes) throws KuduException {
    int numMutations = 0;
    int numDeletes = 0;
    while (scanner.hasMoreRows()) {
      for (RowResult rowResult : scanner.nextRows()) {
        numMutations++;
        if (rowResult.isDeleted()) numDeletes++;
      }
    }
    assertEquals(numExpectedMutations, numMutations);
    assertEquals(numExpectedDeletes, numDeletes);
  }

  /** Test that scan tokens work with diff scans. */
  @Test
  public void testDiffScanTokens() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    // Set up the table for a diff scan.
    int numRows = 20;
    long timestamp = setupTableForDiffScans(client, table, numRows);

    // Since the diff scan interval is [start, end), increment the start timestamp to exclude
    // the last row inserted in the first group of ops, and increment the end timestamp to include
    // the last row deleted in the second group of ops.
    List<KuduScanToken> tokens = client.newScanTokenBuilder(table)
        .diffScan(timestamp + 1, client.getLastPropagatedTimestamp() + 1)
        .build();
    assertEquals(1, tokens.size());

    checkDiffScanResults(tokens.get(0).intoScanner(client), 3 * numRows / 4, numRows / 4);
  }

  /** Test that scan tokens work with diff scans even when columns are renamed. */
  @Test
  public void testDiffScanTokensConcurrentColumnRename() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    // Set up the table for a diff scan.
    int numRows = 20;
    long timestamp = setupTableForDiffScans(client, table, numRows);

    // Since the diff scan interval is [start, end), increment the start timestamp to exclude
    // the last row inserted in the first group of ops, and increment the end timestamp to include
    // the last row deleted in the second group of ops.
    List<KuduScanToken> tokens = client.newScanTokenBuilder(table)
        .diffScan(timestamp + 1, client.getLastPropagatedTimestamp() + 1)
        .build();
    assertEquals(1, tokens.size());

    // Rename a column between when the token is created and when it is rehydrated into a scanner
    client.alterTable(table.getName(),
                      new AlterTableOptions().renameColumn("column1_i", "column1_i_new"));

    KuduScanner scanner = tokens.get(0).intoScanner(client);

    // TODO(wdberkeley): Handle renaming a column between when the token is rehydrated as a scanner
    //  and when the scanner first hits a replica. Note that this is almost certainly a very
    //  short period of vulnerability.

    checkDiffScanResults(scanner, 3 * numRows / 4, numRows / 4);
  }
}
