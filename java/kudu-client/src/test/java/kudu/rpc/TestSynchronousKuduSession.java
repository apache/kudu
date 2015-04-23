// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSynchronousKuduSession extends BaseKuduTest {
  // Generate a unique table name
  private static final String TABLE_NAME =
      TestSynchronousKuduSession.class.getName()+"-"+System.currentTimeMillis();

  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());

    table = openTable(TABLE_NAME);
  }

  @Test(timeout = 100000)
  public void test() throws Exception {
    SynchronousKuduSession session = client.newSynchronousSession();
    for (int i = 0; i < 10; i++) {
      session.apply(createInsert(i));
    }
    assertEquals(10, countRowsInScan(client.newScannerBuilder(table, basicSchema).build()));

    try {
      session.apply(createInsert(0));
      fail();
    } catch (Exception ex) {
      assertTrue(ex instanceof RowsWithErrorException);
    }

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

    for (int i = 10; i < 20; i++) {
      session.apply(createInsert(i));
    }
    session.flush();
    assertEquals(20, countRowsInScan(client.newScannerBuilder(table, basicSchema).build()));
  }

  private Insert createInsert(int key) {
    return createBasicSchemaInsert(table, key);
  }
}
