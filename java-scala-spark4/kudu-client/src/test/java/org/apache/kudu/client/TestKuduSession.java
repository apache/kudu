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
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithImmutableColumns;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduSession {
  private static final String tableName = "TestKuduSession";

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  @Test(timeout = 100000)
  public void testBasicOps() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    for (int i = 0; i < 10; i++) {
      session.apply(createInsert(table, i));
    }
    assertEquals(10, countRowsInScan(client.newScannerBuilder(table).build()));

    OperationResponse resp = session.apply(createInsert(table, 0));
    assertTrue(resp.hasRowError());

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    for (int i = 10; i < 20; i++) {
      session.apply(createInsert(table, i));
    }
    session.flush();
    assertEquals(20, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  @Test(timeout = 100000)
  public void testIgnoreAllDuplicateRows() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setIgnoreAllDuplicateRows(true);
    for (int i = 0; i < 10; i++) {
      session.apply(createInsert(table, i));
    }
    // Test all of the various flush modes to be sure we correctly handle errors in
    // individual operations and batches.
    for (SessionConfiguration.FlushMode mode : SessionConfiguration.FlushMode.values()) {
      session.setFlushMode(mode);
      for (int i = 0; i < 10; i++) {
        OperationResponse resp = session.apply(createInsert(table, i));
        if (mode == SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC) {
          assertFalse(resp.hasRowError());
        }
      }
      if (mode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
        List<OperationResponse> responses = session.flush();
        for (OperationResponse resp : responses) {
          assertFalse(resp.hasRowError());
        }
      } else if (mode == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
        while (session.hasPendingOperations()) {
          Thread.sleep(100);
        }
        assertEquals(0, session.countPendingErrors());
      }
    }
  }

  @Test(timeout = 100000)
  public void testIgnoreAllNotFoundRows() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setIgnoreAllNotFoundRows(true);
    // Test all of the various flush modes to be sure we correctly handle errors in
    // individual operations and batches.
    for (SessionConfiguration.FlushMode mode : SessionConfiguration.FlushMode.values()) {
      session.setFlushMode(mode);
      for (int i = 0; i < 10; i++) {
        session.apply(createDelete(table, i));
        OperationResponse resp = session.apply(createInsert(table, i));
        if (mode == SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC) {
          assertFalse(resp.hasRowError());
        }
      }
      if (mode == SessionConfiguration.FlushMode.MANUAL_FLUSH) {
        List<OperationResponse> responses = session.flush();
        for (OperationResponse resp : responses) {
          assertFalse(resp.hasRowError());
        }
      } else if (mode == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
        while (session.hasPendingOperations()) {
          Thread.sleep(100);
        }
        assertEquals(0, session.countPendingErrors());
      }
    }
  }

  @Test(timeout = 100000)
  public void testBatchWithSameRow() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    // Insert 25 rows, one per batch, along with 50 updates for each, and a delete at the end,
    // while also clearing the cache between each batch half the time. The delete is added here
    // so that a misplaced update would fail if it happens later than its delete.
    for (int i = 0; i < 25; i++) {
      session.apply(createInsert(table, i));
      for (int j = 0; j < 50; j++) {
        Update update = table.newUpdate();
        PartialRow row = update.getRow();
        row.addInt(basicSchema.getColumnByIndex(0).getName(), i);
        row.addInt(basicSchema.getColumnByIndex(1).getName(), 1000);
        session.apply(update);
      }
      Delete del = table.newDelete();
      PartialRow row = del.getRow();
      row.addInt(basicSchema.getColumnByIndex(0).getName(), i);
      session.apply(del);
      session.flush();
      if (i % 2 == 0) {
        asyncClient.emptyTabletsCacheForTable(table.getTableId());
      }
    }
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  @Test(timeout = 100000)
  public void testDeleteWithFullRow() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    List<PartialRow> rows = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      Insert insert = createInsert(table, i);
      rows.add(insert.getRow());
      session.apply(insert);
    }
    session.flush();

    for (PartialRow row : rows) {
      Operation del;
      if (row.getInt(0) % 2 == 0) {
        del = table.newDelete();
      } else {
        del = table.newDeleteIgnore();
      }
      del.setRow(row);
      session.apply(del);
    }
    session.flush();

    assertEquals(0, session.countPendingErrors());
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  /** Regression test for KUDU-3198. Delete with full row from a 64-column table. */
  @Test(timeout = 100000)
  public void testDeleteWithFullRowFrom64ColumnTable() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(64);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    for (int i = 1; i < 64; i++) {
      columns.add(new ColumnSchema.ColumnSchemaBuilder("column_" + i, Type.STRING)
          .nullable(true)
          .build());
    }
    Schema schema = new Schema(columns);

    KuduTable table = client.createTable(tableName, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    // Insert 25 rows and then delete them.
    List<PartialRow> rows = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt(0, 1);
      for (int j = 1; j < 64; j++) {
        if (j % 2 == 0) {
          row.setNull(j);
        } else {
          row.addString(j, "val_" + j);
        }
      }
      rows.add(row);
      session.apply(insert);
    }
    session.flush();

    for (PartialRow row : rows) {
      Operation del;
      if (row.getInt(0) % 2 == 0) {
        del = table.newDelete();
      } else {
        del = table.newDeleteIgnore();
      }
      del.setRow(row);
      session.apply(del);
    }
    session.flush();

    assertEquals(0, session.countPendingErrors());
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  /**
   * Regression test for KUDU-1402. Calls to session.flush() should return an empty list
   * instead of null.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testEmptyFlush() throws Exception {
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    List<OperationResponse> result = session.flush();
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  /**
   * Regression test for KUDU-1226. Calls to session.flush() concurrent with AUTO_FLUSH_BACKGROUND
   * can end up giving ConvertBatchToListOfResponsesCB a list with nulls if a tablet was already
   * flushed. Only happens with multiple tablets.
   */
  @Test(timeout = 100000)
  public void testConcurrentFlushes() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    int numTablets = 4;
    int numRowsPerTablet = 100;

    // Create a 4 tablets table split on 1000, 2000, and 3000.
    for (int i = 1; i < numTablets; i++) {
      PartialRow split = basicSchema.newPartialRow();
      split.addInt(0, i * numRowsPerTablet);
      builder.addSplitRow(split);
    }
    KuduTable table = client.createTable(tableName, basicSchema, builder);

    // Configure the session to background flush as often as it can (every 1ms).
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    session.setFlushInterval(1);

    // Fill each tablet in parallel 1 by 1 then flush. Without the fix this would quickly get an
    // NPE.
    for (int i = 0; i < numRowsPerTablet; i++) {
      for (int j = 0; j < numTablets; j++) {
        session.apply(createInsert(table, i + (numRowsPerTablet * j)));
      }
      session.flush();
    }
  }

  @Test(timeout = 10000)
  public void testOverWritingValues() throws Exception {
    final KuduTable table =
        client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    final KuduSession session = client.newSession();
    Insert insert = createInsert(table, 0);
    PartialRow row = insert.getRow();

    // Overwrite all the normal columns.
    int magicNumber = 9999;
    row.addInt(1, magicNumber);
    row.addInt(2, magicNumber);
    row.addBoolean(4, false);
    // Spam the string column since it's backed by an array.
    for (int i = 0; i <= magicNumber; i++) {
      row.addString(3, i + "");
    }
    // We're supposed to keep a constant size.
    assertEquals(5, row.getVarLengthData().size());
    session.apply(insert);

    KuduScanner scanner = client.newScannerBuilder(table).build();
    RowResult rr = scanner.nextRows().next();
    assertEquals(magicNumber, rr.getInt(1));
    assertEquals(magicNumber, rr.getInt(2));
    assertEquals(magicNumber + "", rr.getString(3));
    assertEquals(false, rr.getBoolean(4));

    // Test setting a value post-apply.
    try {
      row.addInt(1, 0);
      fail("Row should be frozen and throw");
    } catch (IllegalStateException ex) {
      // Ok.
    }
  }

  private void doVerifyMetrics(KuduSession session,
                              long successfulInserts,
                              long insertIgnoreErrors,
                              long successfulUpserts,
                              long upsertIgnoreErrors,
                              long successfulUpdates,
                              long updateIgnoreErrors,
                              long successfulDeletes,
                              long deleteIgnoreErrors) {
    ResourceMetrics metrics = session.getWriteOpMetrics();
    assertEquals(successfulInserts, metrics.getMetric("successful_inserts"));
    assertEquals(insertIgnoreErrors, metrics.getMetric("insert_ignore_errors"));
    assertEquals(successfulUpserts, metrics.getMetric("successful_upserts"));
    assertEquals(upsertIgnoreErrors, metrics.getMetric("upsert_ignore_errors"));
    assertEquals(successfulUpdates, metrics.getMetric("successful_updates"));
    assertEquals(updateIgnoreErrors, metrics.getMetric("update_ignore_errors"));
    assertEquals(successfulDeletes, metrics.getMetric("successful_deletes"));
    assertEquals(deleteIgnoreErrors, metrics.getMetric("delete_ignore_errors"));
  }

  @Test(timeout = 10000)
  public void testUpsert() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Test an Upsert that acts as an Insert.
    assertFalse(session.apply(createUpsert(table, 1, 1, false)).hasRowError());

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "INT32 key=1, INT32 column1_i=1, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=true",
        rowStrings.get(0));
    doVerifyMetrics(session, 0, 0, 1, 0, 0, 0, 0, 0);

    // Test an Upsert that acts as an Update.
    assertFalse(session.apply(createUpsert(table, 1, 2, false)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals(
        "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=true",
        rowStrings.get(0));
    doVerifyMetrics(session, 0, 0, 2, 0, 0, 0, 0, 0);
  }

  @Test(timeout = 10000)
  public void testInsertIgnoreAfterInsertHasNoRowError() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    session.apply(createInsert(table, 1));
    session.apply(createUpsert(table, 1, 1, false));
    session.apply(createInsertIgnore(table, 1));
    List<OperationResponse> results = session.flush();
    doVerifyMetrics(session, 1, 1, 1, 0, 0, 0, 0, 0);
    for (OperationResponse result : results) {
      assertFalse(result.toString(), result.hasRowError());
    }
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
            "INT32 key=1, INT32 column1_i=1, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true",
            rowStrings.get(0));
  }

  @Test(timeout = 10000)
  public void testInsertAfterInsertIgnoreHasRowError() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    session.apply(createInsertIgnore(table, 1));
    session.apply(createInsert(table, 1));
    List<OperationResponse> results = session.flush();
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 0, 0);
    assertFalse(results.get(0).toString(), results.get(0).hasRowError());
    assertTrue(results.get(1).toString(), results.get(1).hasRowError());
    assertTrue(results.get(1).getRowError().getErrorStatus().isAlreadyPresent());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true",
            rowStrings.get(0));
  }

  @Test(timeout = 10000)
  public void testInsertIgnore() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Test insert ignore implements normal insert.
    assertFalse(session.apply(createInsertIgnore(table, 1)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true",
            rowStrings.get(0));
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 0, 0);

    // Test insert ignore does not return a row error.
    assertFalse(session.apply(createInsertIgnore(table, 1)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals(
            "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true",
            rowStrings.get(0));
    doVerifyMetrics(session, 1, 1, 0, 0, 0, 0, 0, 0);

  }

  @Test(timeout = 10000)
  public void testUpdateIgnore() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Test update ignore does not return a row error.
    assertFalse(session.apply(createUpdateIgnore(table, 1, 1, false)).hasRowError());
    assertEquals(0, scanTableToStrings(table).size());
    doVerifyMetrics(session, 0, 0, 0, 0, 0, 1, 0, 0);

    assertFalse(session.apply(createInsert(table, 1)).hasRowError());
    assertEquals(1, scanTableToStrings(table).size());
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 1, 0, 0);

    // Test update ignore implements normal update.
    assertFalse(session.apply(createUpdateIgnore(table, 1, 2, false)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=true",
        rowStrings.get(0));
    doVerifyMetrics(session, 1, 0, 0, 0, 1, 1, 0, 0);
  }

  @Test(timeout = 10000)
  public void testDeleteIgnore() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Test delete ignore does not return a row error.
    assertFalse(session.apply(createDeleteIgnore(table, 1)).hasRowError());
    doVerifyMetrics(session, 0, 0, 0, 0, 0, 0, 0, 1);

    assertFalse(session.apply(createInsert(table, 1)).hasRowError());
    assertEquals(1, scanTableToStrings(table).size());
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 0, 1);

    // Test delete ignore implements normal delete.
    assertFalse(session.apply(createDeleteIgnore(table, 1)).hasRowError());
    assertEquals(0, scanTableToStrings(table).size());
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 1, 1);
  }

  @Test(timeout = 10000)
  public void testInsertManualFlushNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    client.createTable(tableName, basicSchema, createOptions);
    KuduTable table = client.openTable(tableName);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    // Insert in reverse sorted order so that more table location lookups occur
    // (the extra results in table location lookups always occur past the inserted key).
    List<Integer> nonCoveredKeys = ImmutableList.of(350, 300, 199, 150, 100, -1, -50);
    for (int key : nonCoveredKeys) {
      assertNull(session.apply(createBasicSchemaInsert(table, key)));
    }
    List<OperationResponse> results = session.flush();
    assertEquals(nonCoveredKeys.size(), results.size());
    for (OperationResponse result : results) {
      assertTrue(result.hasRowError());
      assertTrue(result.getRowError().getErrorStatus().isNotFound());
    }

    // Insert a batch of some valid and some invalid.
    for (int key = 90; key < 110; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }
    results = session.flush();

    int failures = 0;
    for (OperationResponse result : results) {
      if (result.hasRowError()) {
        failures++;
        assertTrue(result.getRowError().getErrorStatus().isNotFound());
      }
    }
    assertEquals(10, failures);
  }

  @Test(timeout = 10000)
  public void testInsertManualFlushResponseOrder() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    client.createTable(tableName, basicSchema, createOptions);
    KuduTable table = client.openTable(tableName);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    // Insert a batch of some valid and some invalid.
    for (int i = 0; i < 10; i++) {
      assertNull(session.apply(createBasicSchemaInsert(table, 100 + i * 10)));
      assertNull(session.apply(createBasicSchemaInsert(table, 200 + i * 10)));
    }
    List<OperationResponse> results = session.flush();

    assertEquals(20, results.size());

    for (int i = 0; i < 20; i++) {
      OperationResponse result = results.get(i);
      if (i % 2 == 0) {
        assertTrue(result.hasRowError());
        assertTrue(result.getRowError().getErrorStatus().isNotFound());
      } else {
        assertFalse(result.hasRowError());
      }
    }
  }

  @Test(timeout = 10000)
  public void testNonCoveredRangeException() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    client.createTable(tableName, basicSchema, createOptions);
    KuduTable table = client.openTable(tableName);
    Insert insert = createInsert(table, 150);

    //AUTO_FLUSH_SYNC case
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    OperationResponse apply = session.apply(insert);
    assertTrue(apply.hasRowError());
    System.err.println(apply.getRowError().getErrorStatus().getMessage());
    assertTrue(apply.getRowError().getErrorStatus().getMessage().contains(
            "does not exist in table: TestKuduSession"));
    //AUTO_FLUSH_BACKGROUND case
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    assertEquals(null, session.apply(insert));
    List<OperationResponse> autoFlushResult = session.flush();
    assertEquals(1, autoFlushResult.size());
    OperationResponse responseAuto = autoFlushResult.get(0);
    assertTrue(responseAuto.hasRowError());
    assertTrue(responseAuto.getRowError().getErrorStatus().getMessage().contains(
            "does not exist in table: TestKuduSession"));
    //MANUAL_FLUSH case
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    assertEquals(null, session.apply(insert));
    List<OperationResponse> manualFlushResult = session.flush();
    assertEquals(1, manualFlushResult.size());
    OperationResponse responseManual = manualFlushResult.get(0);
    assertTrue(responseManual.hasRowError());
    assertTrue(responseManual.getRowError().getErrorStatus().getMessage().contains(
            "does not exist in table: TestKuduSession"));
  }

  @Test(timeout = 10000)
  public void testInsertAutoFlushSyncNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    client.createTable(tableName, basicSchema, createOptions);
    KuduTable table = client.openTable(tableName);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

    List<Integer> nonCoveredKeys = ImmutableList.of(350, 300, 199, 150, 100, -1, -50);
    for (int key : nonCoveredKeys) {
      OperationResponse response = session.apply(createBasicSchemaInsert(table, key));
      assertTrue(response.hasRowError());
      assertTrue(response.getRowError().getErrorStatus().isNotFound());
    }
  }

  @Test(timeout = 10000)
  public void testInsertAutoFlushBackgroundNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    client.createTable(tableName, basicSchema, createOptions);
    KuduTable table = client.openTable(tableName);

    AsyncKuduSession session = asyncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    List<Integer> nonCoveredKeys = ImmutableList.of(350, 300, 199, 150, 100, -1, -50);
    for (int key : nonCoveredKeys) {
      OperationResponse result = session.apply(createBasicSchemaInsert(table, key)).join(5000);
      assertTrue(result.hasRowError());
      assertTrue(result.getRowError().getErrorStatus().isNotFound());
    }

    RowErrorsAndOverflowStatus errors = session.getPendingErrors();
    assertEquals(nonCoveredKeys.size(), errors.getRowErrors().length);
    for (RowError error : errors.getRowErrors()) {
      assertTrue(error.getErrorStatus().isNotFound());
    }

    // Insert a batch of some valid and some invalid.
    for (int key = 90; key < 110; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }
    session.flush().join(5000);

    errors = session.getPendingErrors();
    assertEquals(10, errors.getRowErrors().length);
    for (RowError error : errors.getRowErrors()) {
      assertTrue(error.getErrorStatus().isNotFound());
    }
  }

  @Test(timeout = 10000)
  public void testUpdateOnTableWithImmutableColumn() throws Exception {
    // Create a table with an immutable column.
    KuduTable table = client.createTable(
            tableName, createSchemaWithImmutableColumns(), getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Insert some data and verify it.
    assertFalse(session.apply(createInsertOnTableWithImmutableColumn(table, 1)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals("INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true, INT32 column5_i=6",
            rowStrings.get(0));
    // successfulInserts++
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 0, 0);

    // Test an Update can update row without immutable column set.
    final String expectRow = "INT32 key=1, INT32 column1_i=3, INT32 column2_i=3, " +
        "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=6";
    assertFalse(session.apply(createUpdateOnTableWithImmutableColumn(
            table, 1, false)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals(expectRow, rowStrings.get(0));
    // successfulUpdates++
    doVerifyMetrics(session, 1, 0, 0, 0, 1, 0, 0, 0);

    // Test an Update results in an error when attempting to update row having at least
    // one column with the immutable attribute set.
    OperationResponse resp = session.apply(createUpdateOnTableWithImmutableColumn(
            table, 1, true));
    assertTrue(resp.hasRowError());
    assertTrue(resp.getRowError().getErrorStatus().isImmutable());
    Assert.assertThat(resp.getRowError().getErrorStatus().toString(),
            CoreMatchers.containsString("Immutable: UPDATE not allowed for " +
                    "immutable column: column5_i INT32 NULLABLE IMMUTABLE"));

    // nothing changed
    rowStrings = scanTableToStrings(table);
    assertEquals(expectRow, rowStrings.get(0));
    doVerifyMetrics(session, 1, 0, 0, 0, 1, 0, 0, 0);
  }

  @Test(timeout = 10000)
  public void testUpdateIgnoreOnTableWithImmutableColumn() throws Exception {
    // Create a table with an immutable column.
    KuduTable table = client.createTable(
            tableName, createSchemaWithImmutableColumns(), getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Insert some data and verify it.
    assertFalse(session.apply(createInsertOnTableWithImmutableColumn(table, 1)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals("INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=a string, BOOL column4_b=true, INT32 column5_i=6",
            rowStrings.get(0));
    // successfulInserts++
    doVerifyMetrics(session, 1, 0, 0, 0, 0, 0, 0, 0);

    final String expectRow = "INT32 key=1, INT32 column1_i=3, INT32 column2_i=3, " +
            "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=6";

    // Test an UpdateIgnore can update a row without changing the immutable column cell,
    // the error of updating the immutable column will be ignored.
    assertFalse(session.apply(createUpdateIgnoreOnTableWithImmutableColumn(
            table, 1, true)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals(expectRow, rowStrings.get(0));
    // successfulUpdates++, updateIgnoreErrors++
    doVerifyMetrics(session, 1, 0, 0, 0, 1, 1, 0, 0);

    // Test an UpdateIgnore only on immutable column. Note that this will result in
    // a 'Invalid argument: No fields updated' error.
    OperationResponse resp = session.apply(createUpdateIgnoreOnTableWithImmutableColumn(
            table, 1, false));
    assertTrue(resp.hasRowError());
    assertTrue(resp.getRowError().getErrorStatus().isInvalidArgument());
    Assert.assertThat(resp.getRowError().getErrorStatus().toString(),
        CoreMatchers.containsString("Invalid argument: No fields updated, " +
                "key is: (int32 key=<redacted>)"));

    // nothing changed
    rowStrings = scanTableToStrings(table);
    assertEquals(expectRow, rowStrings.get(0));
    doVerifyMetrics(session, 1, 0, 0, 0, 1, 1, 0, 0);
  }

  @Test(timeout = 10000)
  public void testUpsertIgnoreOnTableWithImmutableColumn() throws Exception {
    // Create a table with an immutable column.
    KuduTable table = client.createTable(
            tableName, createSchemaWithImmutableColumns(), getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    // Insert some data and verify it.
    assertFalse(session.apply(createUpsertIgnoreOnTableWithImmutableColumn(
            table, 1, 2, true)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals("INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
                    "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=4",
            rowStrings.get(0));
    // successfulUpserts++
    doVerifyMetrics(session, 0, 0, 1, 0, 0, 0, 0, 0);

    // Test an UpsertIgnore can update row without immutable column set.
    assertFalse(session.apply(createUpsertIgnoreOnTableWithImmutableColumn(
            table, 1, 3, false)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals("INT32 key=1, INT32 column1_i=3, INT32 column2_i=3, " +
            "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=4", rowStrings.get(0));
    // successfulUpserts++
    doVerifyMetrics(session, 0, 0, 2, 0, 0, 0, 0, 0);

    // Test an UpsertIgnore can update row with immutable column set.
    assertFalse(session.apply(createUpsertIgnoreOnTableWithImmutableColumn(
            table, 1, 4, true)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals("INT32 key=1, INT32 column1_i=4, INT32 column2_i=3, " +
            "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=4", rowStrings.get(0));
    // successfulUpserts++, upsertIgnoreErrors++
    doVerifyMetrics(session, 0, 0, 3, 1, 0, 0, 0, 0);
  }

  @Test(timeout = 10000)
  public void testUpsertOnTableWithImmutableColumn() throws Exception {
    // Create a table with an immutable column.
    KuduTable table = client.createTable(
            tableName, createSchemaWithImmutableColumns(), getBasicCreateTableOptions());
    KuduSession session = client.newSession();

    final String expectRow = "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
        "STRING column3_s=NULL, BOOL column4_b=true, INT32 column5_i=4";
    // Insert some data and verify it.
    assertFalse(session.apply(createUpsertOnTableWithImmutableColumn(
            table, 1, 2, true)).hasRowError());
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(expectRow, rowStrings.get(0));
    // successfulUpserts++
    doVerifyMetrics(session, 0, 0, 1, 0, 0, 0, 0, 0);

    // Test an Upsert attemp to update an immutable column, which will result an error.
    OperationResponse resp = session.apply(createUpsertOnTableWithImmutableColumn(
            table, 1, 3, true));
    assertTrue(resp.hasRowError());
    assertTrue(resp.getRowError().getErrorStatus().isImmutable());
    Assert.assertThat(resp.getRowError().getErrorStatus().toString(),
        CoreMatchers.containsString("Immutable: UPDATE not allowed for " +
                "immutable column: column5_i INT32 NULLABLE IMMUTABLE"));

    // nothing changed
    rowStrings = scanTableToStrings(table);
    assertEquals(expectRow, rowStrings.get(0));
    doVerifyMetrics(session, 0, 0, 1, 0, 0, 0, 0, 0);
  }

  private Insert createInsert(KuduTable table, int key) {
    return createBasicSchemaInsert(table, key);
  }

  private Insert createInsertOnTableWithImmutableColumn(KuduTable table, int key) {
    Insert insert = createBasicSchemaInsert(table, key);
    insert.getRow().addInt(5, 6);
    return insert;
  }

  private Update createUpdateOnTableWithImmutableColumn(KuduTable table, int key,
                                                        boolean updateImmutableColumn) {
    Update update = table.newUpdate();
    populateUpdateRow(update.getRow(), key, key * 3, true);
    if (updateImmutableColumn) {
      update.getRow().addInt(5, 6);
    }

    return update;
  }

  private UpdateIgnore createUpdateIgnoreOnTableWithImmutableColumn(
          KuduTable table, int key, boolean updateNonImmutableColumns) {
    UpdateIgnore updateIgnore = table.newUpdateIgnore();
    if (updateNonImmutableColumns) {
      populateUpdateRow(updateIgnore.getRow(), key, key * 3, true);
    } else {
      updateIgnore.getRow().addInt(0, key);
    }
    updateIgnore.getRow().addInt(5, 6);

    return updateIgnore;
  }

  private UpsertIgnore createUpsertIgnoreOnTableWithImmutableColumn(
          KuduTable table, int key, int times, boolean updateImmutableColumn) {
    UpsertIgnore upsertIgnore = table.newUpsertIgnore();
    populateUpdateRow(upsertIgnore.getRow(), key, key * times, true);
    if (updateImmutableColumn) {
      upsertIgnore.getRow().addInt(5, key * times * 2);
    }

    return upsertIgnore;
  }

  private Upsert createUpsertOnTableWithImmutableColumn(
          KuduTable table, int key, int times, boolean updateImmutableColumn) {
    Upsert upsert = table.newUpsert();
    populateUpdateRow(upsert.getRow(), key, key * times, true);
    if (updateImmutableColumn) {
      upsert.getRow().addInt(5, key * times * 2);
    }

    return upsert;
  }

  private Upsert createUpsert(KuduTable table, int key, int secondVal, boolean hasNull) {
    Upsert upsert = table.newUpsert();
    populateUpdateRow(upsert.getRow(), key, secondVal, hasNull);
    return upsert;
  }

  private UpdateIgnore createUpdateIgnore(KuduTable table, int key, int secondVal,
                                          boolean hasNull) {
    UpdateIgnore updateIgnore = table.newUpdateIgnore();
    populateUpdateRow(updateIgnore.getRow(), key, secondVal, hasNull);
    return updateIgnore;
  }

  private void populateUpdateRow(PartialRow row, int key, int secondVal, boolean hasNull) {
    row.addInt(0, key);
    row.addInt(1, secondVal);
    row.addInt(2, 3);
    if (hasNull) {
      row.setNull(3);
    } else {
      row.addString(3, "a string");
    }
    row.addBoolean(4, true);
  }

  private Delete createDelete(KuduTable table, int key) {
    Delete delete = table.newDelete();
    PartialRow row = delete.getRow();
    row.addInt(0, key);
    return delete;
  }

  private DeleteIgnore createDeleteIgnore(KuduTable table, int key) {
    DeleteIgnore deleteIgnore = table.newDeleteIgnore();
    PartialRow row = deleteIgnore.getRow();
    row.addInt(0, key);
    return deleteIgnore;
  }

  protected InsertIgnore createInsertIgnore(KuduTable table, int key) {
    InsertIgnore insertIgnore = table.newInsertIgnore();
    PartialRow row = insertIgnore.getRow();
    row.addInt(0, key);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);
    return insertIgnore;
  }
}
