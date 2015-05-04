// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.stumbleupon.async.Deferred;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAsyncKuduClient extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestAsyncKuduClient.class.getName() + "-" + System.currentTimeMillis();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());

    table = openTable(TABLE_NAME);
  }

  @Test(timeout = 100000)
  public void testDisconnect() throws Exception {
    // Test that we can reconnect to a TS after a disconnection.
    // 1. Warm up the cache.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table, basicSchema).build()));

    // 2. Disconnect the TabletClient.
    client.getTableClients().get(0).shutdown().join(DEFAULT_SLEEP);

    // 3. Count again, it will trigger a re-connection and we should not hang or fail to scan.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table, basicSchema).build()));


    // Test that we can reconnect to a TS while scanning.
    // 1. Insert enough rows to have to call next() mulitple times.
    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    int rowCount = 200;
    for (int i = 0; i < rowCount; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    session.flush();

    // 2. Start a scanner with a small max num bytes.
    KuduScanner scanner = client.newScannerBuilder(table, basicSchema)
        .maxNumBytes(1)
        .build();
    Deferred<KuduScanner.RowResultIterator> rri = scanner.nextRows();
    // 3. Register the number of rows we get back. We have no control over how many rows are
    // returned. When this test was written we were getting 100 rows back.
    int numRows = rri.join(DEFAULT_SLEEP).getNumRows();
    assertNotEquals("The TS sent all the rows back, we can't properly test disconnection",
        rowCount, numRows);

    // 4. Disconnect the TS.
    client.getTableClients().get(0).shutdown().join(DEFAULT_SLEEP);
    // 5. Make sure that we can continue scanning and that we get the remaining rows back.
    assertEquals(rowCount - numRows, countRowsInScan(scanner));
  }
}
