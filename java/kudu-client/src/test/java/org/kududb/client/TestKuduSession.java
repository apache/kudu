// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestKuduSession extends BaseKuduTest {
  // Generate a unique table name
  private static final String TABLE_NAME_PREFIX =
      TestKuduSession.class.getName()+"-"+System.currentTimeMillis();

  private KuduTable table;

  @Test(timeout = 100000)
  public void testBasicOps() throws Exception {
    String tableName = TABLE_NAME_PREFIX + "-testBasicOps";
    createTable(tableName, basicSchema, new CreateTableBuilder());

    table = openTable(tableName);
    KuduSession session = syncClient.newSession();
    for (int i = 0; i < 10; i++) {
      session.apply(createInsert(i));
    }
    assertEquals(10, countRowsInScan(client.newScannerBuilder(table).build()));

    OperationResponse resp = session.apply(createInsert(0));
    assertTrue(resp.hasRowError());

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    for (int i = 10; i < 20; i++) {
      session.apply(createInsert(i));
    }
    session.flush();
    assertEquals(20, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  @Test(timeout = 100000)
  public void testBatchWithSameRow() throws Exception {
    String tableName = TABLE_NAME_PREFIX + "-testBatchWithSameRow";
    createTable(tableName, basicSchema, new CreateTableBuilder());

    table = openTable(tableName);

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    // Insert 25 rows, one per batch, along with 50 updates for each, and a delete at the end,
    // while also clearing the cache between each batch half the time. The delete is added here
    // so that a misplaced update would fail if it happens later than its delete.
    for (int i = 0; i < 25; i++) {
      session.apply(createInsert(i));
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
        client.emptyTabletsCacheForTable(tableName);
      }
    }
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));
  }

  private Insert createInsert(int key) {
    return createBasicSchemaInsert(table, key);
  }
}
