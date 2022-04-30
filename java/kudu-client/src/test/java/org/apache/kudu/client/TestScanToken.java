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

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.countScanTokenRows;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.loadDefaultTable;
import static org.apache.kudu.test.MetricTestUtils.totalRequestCount;
import static org.apache.kudu.test.MetricTestUtils.validateRequestCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.protobuf.CodedInputStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.KuduBinaryLocator;
import org.apache.kudu.test.cluster.MiniKuduCluster;

public class TestScanToken {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClient.class);

  private static final String testTableName = "TestScanToken";

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  // Enable Kerberos and access control so we can validate the requests in secure environment.
  // Specifically that authz tokens in the scan tokens work.
  private static final MiniKuduCluster.MiniKuduClusterBuilder clusterBuilder =
      KuduTestHarness.getBaseClusterBuilder()
          .enableKerberos()
          .addTabletServerFlag("--tserver_enforce_access_control=true");

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

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
   * Regression test for KUDU-3349
   */
  @Test
  public void testScanTokenWithWrongUuidSerialization() throws Exception {
    // Prepare the table for testing.
    Schema schema = createManyStringsSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    final int buckets = 8;
    createOptions.addHashPartitions(ImmutableList.of("key"), buckets);
    client.createTable(testTableName, schema, createOptions);

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(testTableName);
    final int totalRows = 100;
    for (int i = 0; i < totalRows; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c1", "c1_" + i);
      row.addString("c2", "c2_" + i);
      assertEquals(session.apply(insert).hasRowError(), false);
    }
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    tokenBuilder.setProjectedColumnIndexes(ImmutableList.of());
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(buckets, tokens.size());

    // Create a new client, open the newly created kudu table, and new scanners.
    AsyncKuduClient newAsyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(
        harness.getMasterAddressesAsString())
        .build();
    KuduClient newClient = newAsyncClient.syncClient();
    KuduTable newTable = newClient.openTable(testTableName);
    List<KuduScanner> kuduScanners = new ArrayList<>(buckets);
    List<String> tabletIds = new ArrayList<>(buckets);
    for (KuduScanToken token : tokens) {
      tabletIds.add(new String(token.getTablet().getTabletId(),
          java.nio.charset.StandardCharsets.UTF_8));
      KuduScanner kuduScanner = token.intoScanner(newAsyncClient.syncClient());
      kuduScanners.add(kuduScanner);
    }

    // Step down all tablet leaders.
    KuduBinaryLocator.ExecutableInfo exeInfo = null;
    try {
      exeInfo = KuduBinaryLocator.findBinary("kudu");
    } catch (FileNotFoundException e) {
      LOG.error(e.getMessage());
      fail();
    }
    for (String tabletId : tabletIds) {
      List<String> commandLine = Lists.newArrayList(exeInfo.exePath(),
              "tablet",
              "leader_step_down",
              harness.getMasterAddressesAsString(),
              tabletId);
      ProcessBuilder processBuilder = new ProcessBuilder(commandLine);
      processBuilder.environment().putAll(exeInfo.environment());
      // Step down the tablet leaders one by one after a fix duration.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.error(e.getMessage());
      }
    }
    // Delete all rows first through the new client.
    KuduSession newSession = newClient.newSession();

    for (int i = 0; i < totalRows; i++) {
      Operation del = newTable.newDelete();
      PartialRow row = del.getRow();
      row.addString("key", String.format("key_%02d", i));
      del.setRow(row);
      OperationResponse response = newSession.apply(del);
      assertEquals(response.hasRowError(), false);
    }

    // Insert all rows again through the new client.
    for (int i = 0; i < totalRows; i++) {
      Insert insert = newTable.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c1", "c1_" + i);
      row.addString("c2", "c2_" + i);
      assertEquals(newSession.apply(insert).hasRowError(), false);
    }

    // Verify all the row count.
    int rowCount = 0;
    for (KuduScanner kuduScanner : kuduScanners) {
      while (kuduScanner.hasMoreRows()) {
        rowCount += kuduScanner.nextRows().numRows;
      }
    }
    assertEquals(totalRows, rowCount);
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
    List<KuduScanToken> tokens = tokenBuilder.includeTableMetadata(false).build();
    List<KuduScanToken> tokensWithMetadata = tokenBuilder.includeTableMetadata(true).build();
    assertEquals(1, tokens.size());
    assertEquals(1, tokensWithMetadata.size());
    KuduScanToken token = tokens.get(0);
    KuduScanToken tokenWithMetadata = tokensWithMetadata.get(0);

    // Drop a column
    client.alterTable(testTableName, new AlterTableOptions().dropColumn("a"));
    try {
      token.intoScanner(client);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown column"));
    }
    try {
      KuduScanner scanner = tokenWithMetadata.intoScanner(client);
      countRowsInScan(scanner);
      fail();
    } catch (KuduException e) {
      assertTrue(e.getMessage().contains("Some columns are not present in the current schema: a"));
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
    final String oldColName = schema.getColumnByIndex(1).getName();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    client.createTable(testTableName, schema, createOptions);

    KuduTable table = client.openTable(testTableName);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    // TODO(KUDU-3146): Disable including the table metadata so the new column name is retrieved
    //  when deserializing the scanner.
    tokenBuilder.includeTableMetadata(false);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());
    KuduScanToken token = tokens.get(0);

    // Rename a column.
    String newColName = "new-name";
    client.alterTable(testTableName, new AlterTableOptions().renameColumn(oldColName, newColName));

    KuduScanner scanner = token.intoScanner(client);

    // TODO(KUDU-3146): Handle renaming a column between when the token is rehydrated as a scanner
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
    assertTrue(String.format("%d >= %d / 3?", scannedRows, numRows),
        scannedRows >= numRows / 3);
    assertTrue(String.format("%d <= 2 * %d / 3?", scannedRows, numRows),
        scannedRows <= 2 * numRows / 3);
  }

  /**
   * Test that scanRequestTimeout makes it from the scan token to the underlying Scanner class.
   */
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
    for (int i = 0; i < numRows / 2; i++) {
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
        if (rowResult.isDeleted()) {
          numDeletes++;
        }
      }
    }
    assertEquals(numExpectedMutations, numMutations);
    assertEquals(numExpectedDeletes, numDeletes);
  }

  /**
   * Test that scan tokens work with diff scans.
   */
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
        // TODO(KUDU-3146): Disable including the table metadata so the new column name is
        //  retrieved when deserializing the scanner.
        .includeTableMetadata(false)
        .diffScan(timestamp + 1, client.getLastPropagatedTimestamp() + 1)
        .build();
    assertEquals(1, tokens.size());

    checkDiffScanResults(tokens.get(0).intoScanner(client), 3 * numRows / 4, numRows / 4);
  }

  /**
   * Test that scan tokens work with diff scans even when columns are renamed.
   */
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
        // TODO(KUDU-3146): Disable including the table metadata so the new column name is
        //  retrieved when deserializing the scanner.
        .includeTableMetadata(false)
        .diffScan(timestamp + 1, client.getLastPropagatedTimestamp() + 1)
        .build();
    assertEquals(1, tokens.size());

    // Rename a column between when the token is created and when it is rehydrated into a scanner
    client.alterTable(table.getName(),
        new AlterTableOptions().renameColumn("column1_i", "column1_i_new"));

    KuduScanner scanner = tokens.get(0).intoScanner(client);

    // TODO(KUDU-3146): Handle renaming a column between when the token is rehydrated as a scanner
    //  and when the scanner first hits a replica. Note that this is almost certainly a very
    //  short period of vulnerability.

    checkDiffScanResults(scanner, 3 * numRows / 4, numRows / 4);
  }

  @Test
  public void testScanTokenRequestsWithMetadata() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    // Use a new client to simulate hydrating in a new process.
    KuduClient newClient =
        new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build();
    newClient.getTablesList(); // List the tables to prevent counting initialization RPCs.
    // Ensure the client doesn't have an authorization token for the table.
    assertNull(newClient.asyncClient.getAuthzTokenCache().get(table.getTableId()));

    KuduMetrics.logMetrics(); // Log the metric values to help debug failures.
    final long beforeRequests = totalRequestCount();

    // Validate that building a scan token results in a single GetTableLocations request.
    KuduScanToken token = validateRequestCount(1, client.getClientId(),
        "GetTableLocations", () -> {
          KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
          List<KuduScanToken> tokens = tokenBuilder.includeTableMetadata(true).build();
          assertEquals(1, tokens.size());
          return tokens.get(0);
        });

    // Validate that hydrating a token doesn't result in a request.
    KuduScanner scanner = validateRequestCount(0, newClient.getClientId(),
        () -> token.intoScanner(newClient));
    // Ensure the client now has an authorization token.
    assertNotNull(newClient.asyncClient.getAuthzTokenCache().get(table.getTableId()));

    // Validate that starting to scan results in a Scan request.
    validateRequestCount(1, newClient.getClientId(), "Scan",
        scanner::nextRows);

    final long afterRequests = totalRequestCount();

    // Validate no other unexpected requests were sent.
    // GetTableLocations, Scan.
    KuduMetrics.logMetrics(); // Log the metric values to help debug failures.
    assertEquals(2, afterRequests - beforeRequests);
  }

  @Test
  public void testScanTokenRequestsNoMetadata() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    // Use a new client to simulate hydrating in a new process.
    KuduClient newClient =
        new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build();
    newClient.getTablesList(); // List the tables to prevent counting initialization RPCs.

    KuduMetrics.logMetrics(); // Log the metric values to help debug failures.
    long beforeRequests = totalRequestCount();

    // Validate that building a scan token results in a single GetTableLocations request.
    KuduScanToken token = validateRequestCount(1, client.getClientId(),
        "GetTableLocations", () -> {
          KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
          List<KuduScanToken> tokens = tokenBuilder
              .includeTableMetadata(false)
              .includeTabletMetadata(false)
              .build();
          assertEquals(1, tokens.size());
          return tokens.get(0);
        });

    // Validate that hydrating a token into a scanner results in a single GetTableSchema request.
    KuduScanner scanner = validateRequestCount(1, newClient.getClientId(), "GetTableSchema",
        () -> token.intoScanner(newClient));

    // Validate that starting to scan results in a GetTableLocations request and a Scan request.
    validateRequestCount(2, newClient.getClientId(), Arrays.asList("GetTableLocations", "Scan"),
        scanner::nextRows);

    long afterRequests = totalRequestCount();

    // Validate no other unexpected requests were sent.
    // GetTableLocations x 2, GetTableSchema, Scan.
    KuduMetrics.logMetrics(); // Log the metric values to help debug failures.
    assertEquals(4, afterRequests - beforeRequests);
  }

  @Test
  public void testScanTokenSize() throws Exception {
    List<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT8).key(true).build());
    for (int i = 0; i < 100; i++) {
      columns.add(new ColumnSchema.ColumnSchemaBuilder("int64-" + i, Type.INT64).build());
    }
    Schema schema = new Schema(columns);
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(1);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder
        .includeTabletMetadata(false)
        .includeTableMetadata(false)
        .build();
    assertEquals(1, tokens.size());
    final byte[] tokenBytes = tokens.get(0).serialize();

    List<KuduScanToken> tokensWithTabletMetadata = tokenBuilder
        .includeTabletMetadata(true)
        .includeTableMetadata(false)
        .build();
    assertEquals(1, tokensWithTabletMetadata.size());
    final byte[] tokenWithTabletMetadataBytes = tokensWithTabletMetadata.get(0).serialize();

    List<KuduScanToken> tokensWithTableMetadata = tokenBuilder
        .includeTabletMetadata(false)
        .includeTableMetadata(true)
        .build();
    assertEquals(1, tokensWithTabletMetadata.size());
    final byte[] tokenWithTableMetadataBytes = tokensWithTableMetadata.get(0).serialize();

    List<KuduScanToken> tokensWithAllMetadata = tokenBuilder
        .includeTabletMetadata(true)
        .includeTableMetadata(true)
        .build();
    assertEquals(1, tokensWithAllMetadata.size());
    final byte[] tokenWithAllMetadataBytes = tokensWithAllMetadata.get(0).serialize();

    LOG.info("tokenBytes: " + tokenBytes.length);
    LOG.info("tokenWithTabletMetadataBytes: " + tokenWithTabletMetadataBytes.length);
    LOG.info("tokenWithTableMetadataBytes: " + tokenWithTableMetadataBytes.length);
    LOG.info("tokenWithAllMetadataBytes: " + tokenWithAllMetadataBytes.length);

    assertTrue(tokenWithAllMetadataBytes.length > tokenWithTableMetadataBytes.length);
    assertTrue(tokenWithTableMetadataBytes.length > tokenWithTabletMetadataBytes.length);
    assertTrue(tokenWithTabletMetadataBytes.length > tokenBytes.length);
  }

  @Test
  public void testScanTokensWithExtraPredicate() throws IOException {
    final int NUM_ROWS_DESIRED = 100;
    final int PREDICATE_INDEX = 0;
    final int PREDICATE_VAL = 1;
    KuduTable table = createDefaultTable(client, testTableName);
    loadDefaultTable(client, testTableName, NUM_ROWS_DESIRED);
    KuduScanToken.KuduScanTokenBuilder builder =
        new KuduScanToken.KuduScanTokenBuilder(asyncClient, table);
    List<KuduScanToken> tokens = builder.build();
    ColumnSchema cs = table.getSchema().getColumnByIndex(PREDICATE_INDEX);
    KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
        cs, KuduPredicate.ComparisonOp.EQUAL, PREDICATE_VAL);
    Set<Integer> resultKeys = new HashSet<>();
    for (KuduScanToken token : tokens) {
      byte[] serialized = token.serialize();
      KuduScanner.KuduScannerBuilder scannerBuilder = KuduScanToken.deserializeIntoScannerBuilder(
          serialized, client);
      scannerBuilder.addPredicate(predicate);
      KuduScanner scanner = scannerBuilder.build();
      for (RowResult rowResult : scanner) {
        resultKeys.add(rowResult.getInt(PREDICATE_INDEX));
      }
    }
    assertEquals(1, resultKeys.size());
    assertEquals(PREDICATE_VAL, Iterables.getOnlyElement(resultKeys).intValue());
  }

  /**
   * Verify the deserialization of RemoteTablet from KuduScanToken.
   * Regression test for KUDU-3349.
   */
  @Test
  public void testRemoteTabletVerification() throws IOException {
    final int NUM_ROWS_DESIRED = 100;
    KuduTable table = createDefaultTable(client, testTableName);
    loadDefaultTable(client, testTableName, NUM_ROWS_DESIRED);
    KuduScanToken.KuduScanTokenBuilder builder =
            new KuduScanToken.KuduScanTokenBuilder(asyncClient, table);
    List<KuduScanToken> tokens = builder.build();
    List<HostAndPort> tservers = harness.getTabletServers();
    for (KuduScanToken token : tokens) {
      byte[] serialized = token.serialize();
      Client.ScanTokenPB scanTokenPB =
          Client.ScanTokenPB.parseFrom(CodedInputStream.newInstance(serialized));
      Client.TabletMetadataPB tabletMetadata = scanTokenPB.getTabletMetadata();
      Partition partition =
          ProtobufHelper.pbToPartition(tabletMetadata.getPartition());
      RemoteTablet remoteTablet = KuduScanToken.newRemoteTabletFromTabletMetadata(tabletMetadata,
          table.getTableId(), partition);
      for (ServerInfo si : remoteTablet.getTabletServersCopy()) {
        assertEquals(si.getUuid().length(), 32);
        HostAndPort hostAndPort = si.getHostAndPort();
        assertEquals(tservers.contains(hostAndPort), true);
      }
    }
  }

  /**
   * Regression test for KUDU-3205.
   */
  @Test
  public void testBuildTokensWithDownTabletServer() throws Exception {
    Schema schema = getBasicSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.of());
    createOptions.setNumReplicas(3);
    KuduTable table = client.createTable(testTableName, schema, createOptions);

    // Insert a row.
    KuduSession session = client.newSession();
    Insert insert = createBasicSchemaInsert(table, 1);
    session.apply(insert);
    session.close();

    // Remove a tablet server from the remote tablet by calling `removeTabletClient`.
    // This is done in normal applications via AsyncKuduClient.invalidateTabletCache
    // when a tablet not found error is handled.
    TableLocationsCache.Entry entry =
        asyncClient.getTableLocationEntry(table.getTableId(), insert.partitionKey());
    RemoteTablet remoteTablet = entry.getTablet();
    List<ServerInfo> tabletServers = remoteTablet.getTabletServersCopy();
    remoteTablet.removeTabletClient(tabletServers.get(0).getUuid());

    // Ensure we can build and use the token without an error.
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    tokenBuilder.includeTableMetadata(true);
    tokenBuilder.includeTabletMetadata(true);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Use a new client to simulate hydrating in a new process.
    KuduClient newClient =
        new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build();
    KuduScanner scanner = tokens.get(0).intoScanner(newClient);
    assertEquals(1, countRowsInScan(scanner));
  }

  @Test
  public void testScannerBuilderFaultToleranceToggle() throws IOException {
    KuduTable table = createDefaultTable(client, testTableName);
    KuduScanner.KuduScannerBuilder scannerBuilder =
        new KuduScanner.KuduScannerBuilder(asyncClient, table);
    assertFalse(scannerBuilder.isFaultTolerant);
    assertEquals(AsyncKuduScanner.ReadMode.READ_LATEST, scannerBuilder.readMode);

    scannerBuilder.setFaultTolerant(true);
    assertTrue(scannerBuilder.isFaultTolerant);
    assertEquals(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT, scannerBuilder.readMode);

    scannerBuilder.setFaultTolerant(false);
    assertFalse(scannerBuilder.isFaultTolerant);
    assertEquals(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT, scannerBuilder.readMode);

    scannerBuilder.readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES);
    assertFalse(scannerBuilder.isFaultTolerant);
    assertEquals(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES, scannerBuilder.readMode);
  }
}
