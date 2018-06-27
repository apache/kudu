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

import static org.apache.kudu.util.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.util.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.util.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.util.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.util.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

public class TestKuduSession extends BaseKuduTest {
  private static final String tableName = "TestKuduSession";

  @Test(timeout = 100000)
  public void testBasicOps() throws Exception {
    KuduTable table = createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = syncClient.newSession();
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
    KuduTable table = createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = syncClient.newSession();
    session.setIgnoreAllDuplicateRows(true);
    for (int i = 0; i < 10; i++) {
      session.apply(createInsert(table, i));
    }
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
  public void testBatchWithSameRow() throws Exception {
    KuduTable table = createTable(tableName, basicSchema, getBasicCreateTableOptions());

    KuduSession session = syncClient.newSession();
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
        client.emptyTabletsCacheForTable(table.getTableId());
      }
    }
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  /**
   * Regression test for KUDU-1402. Calls to session.flush() should return an empty list
   * instead of null.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testEmptyFlush() throws Exception {
    KuduSession session = syncClient.newSession();
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
  @Test(timeout = 10000)
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
    KuduTable table = createTable(tableName, basicSchema, builder);

    // Configure the session to background flush as often as it can (every 1ms).
    KuduSession session = syncClient.newSession();
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
    KuduTable table = createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = syncClient.newSession();
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

    KuduScanner scanner = syncClient.newScannerBuilder(table).build();
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

  @Test(timeout = 10000)
  public void testUpsert() throws Exception {
    KuduTable table = createTable(tableName, basicSchema, getBasicCreateTableOptions());
    KuduSession session = syncClient.newSession();

    // Test an Upsert that acts as an Insert.
    assertFalse(session.apply(createUpsert(table, 1, 1, false)).hasRowError());

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "INT32 key=1, INT32 column1_i=1, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=true",
        rowStrings.get(0));

    // Test an Upsert that acts as an Update.
    assertFalse(session.apply(createUpsert(table, 1, 2, false)).hasRowError());
    rowStrings = scanTableToStrings(table);
    assertEquals(
        "INT32 key=1, INT32 column1_i=2, INT32 column2_i=3, " +
            "STRING column3_s=a string, BOOL column4_b=true",
        rowStrings.get(0));
  }

  @Test(timeout = 10000)
  public void testInsertManualFlushNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    syncClient.createTable(tableName, basicSchema, createOptions);
    KuduTable table = syncClient.openTable(tableName);

    KuduSession session = syncClient.newSession();
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
    syncClient.createTable(tableName, basicSchema, createOptions);
    KuduTable table = syncClient.openTable(tableName);

    KuduSession session = syncClient.newSession();
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
        assertTrue(!result.hasRowError());
      }
    }
  }

  @Test(timeout = 10000)
  public void testInsertAutoFlushSyncNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    syncClient.createTable(tableName, basicSchema, createOptions);
    KuduTable table = syncClient.openTable(tableName);

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

    List<Integer> nonCoveredKeys = ImmutableList.of(350, 300, 199, 150, 100, -1, -50);
    for (int key : nonCoveredKeys) {
      OperationResponse response = session.apply(createBasicSchemaInsert(table, key));
      assertTrue(response.hasRowError());
      assertTrue(response.getRowError().getErrorStatus().isNotFound());
    }
  }

  @Test(timeout = 10000)
  public void testInsertAutoFlushBackgrounNonCoveredRange() throws Exception {
    CreateTableOptions createOptions = getBasicTableOptionsWithNonCoveredRange();
    createOptions.setNumReplicas(1);
    syncClient.createTable(tableName, basicSchema, createOptions);
    KuduTable table = syncClient.openTable(tableName);

    AsyncKuduSession session = client.newSession();
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
    session.flush();

    errors = session.getPendingErrors();
    assertEquals(10, errors.getRowErrors().length);
    for (RowError error : errors.getRowErrors()) {
      assertTrue(error.getErrorStatus().isNotFound());
    }
  }

  private Insert createInsert(KuduTable table, int key) {
    return createBasicSchemaInsert(table, key);
  }

  private Upsert createUpsert(KuduTable table, int key, int secondVal, boolean hasNull) {
    Upsert upsert = table.newUpsert();
    PartialRow row = upsert.getRow();
    row.addInt(0, key);
    row.addInt(1, secondVal);
    row.addInt(2, 3);
    if (hasNull) {
      row.setNull(3);
    } else {
      row.addString(3, "a string");
    }
    row.addBoolean(4, true);
    return upsert;
  }
}
