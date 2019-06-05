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
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.countScanTokenRows;
import static org.apache.kudu.test.ClientTestUtil.createDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.loadDefaultTable;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
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

    // Add back the column with the wrong type.
    client.alterTable(
        testTableName,
        new AlterTableOptions().addColumn(
            new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).nullable(true).build()));
    try {
      token.intoScanner(client);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          "invalid type INT64 for column 'a' in scan token, expected: STRING"));
    }

    // Add the column with the wrong nullability.
    client.alterTable(
        testTableName,
        new AlterTableOptions().dropColumn("a")
                               .addColumn(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64)
                                                          .nullable(true).build()));
    try {
      token.intoScanner(client);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          "invalid nullability for column 'a' in scan token, expected: NOT NULL"));
    }

    // Add the column with the correct type and nullability.
    client.alterTable(
        testTableName,
        new AlterTableOptions().dropColumn("a")
                               .addColumn(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64)
                                                          .nullable(false)
                                                          .defaultValue(0L).build()));
    token.intoScanner(client);
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
}
