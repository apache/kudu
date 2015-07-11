// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLeaderFailover extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestLeaderFailover.class.getName() + "-" + System.currentTimeMillis();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();

    CreateTableBuilder builder = new CreateTableBuilder();
    builder.setNumReplicas(3);
    createTable(TABLE_NAME, basicSchema, builder);

    table = openTable(TABLE_NAME);
  }

  /**
   * This test writes 3 rows, kills the leader, then tries to write another 3 rows. Finally it
   * counts to make sure we have 6 of them.
   *
   * This test won't run if we didn't start the cluster.
   */
  @Test(timeout = 100000)
  public void testFailover() throws Exception {
    if (!startCluster) {
      return;
    }

    KuduSession session = syncClient.newSession();
    session.setIgnoreAllDuplicateRows(true);
    for (int i = 0; i < 3; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    // Make sure the rows are in there before messing things up.
    AsyncKuduScanner scanner = client.newScannerBuilder(table).build();
    assertEquals(3, countRowsInScan(scanner));

    killTabletLeader(table);

    for (int i = 3; i < 6; i++) {
      OperationResponse resp = session.apply(createBasicSchemaInsert(table, i));
      if (resp.hasRowError()) {
        fail("Encountered a row error " + resp.getRowError());
      }
    }

    scanner = client.newScannerBuilder(table).build();
    assertEquals(6, countRowsInScan(scanner));
  }
}