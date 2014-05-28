// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKuduClient extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestKuduSession.class.getName()+"-"+System.currentTimeMillis();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());

    table = openTable(TABLE_NAME);
  }

  @Test(timeout = 100000)
  public void testDisconnect() throws Exception {
    // Warm up the cache
    assertEquals(0, countRowsInScan(client.newScanner(table, basicSchema)));

    // Destroy the cache
    client.getTableClients().get(0).shutdown().join(DEFAULT_SLEEP);

    // Count again, it will trigger a re-connection and we should not hang or fail to scan
    assertEquals(0, countRowsInScan(client.newScanner(table, basicSchema)));
  }
}
