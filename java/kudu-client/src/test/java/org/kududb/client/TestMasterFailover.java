// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;


/**
 * Tests {@link AsyncKuduClient} with multiple masters.
 */
public class TestMasterFailover extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestMasterFailover.class);
  private static final String TABLE_NAME =
      TestMasterFailover.class.getName() + "-" + System.currentTimeMillis();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());
  }

  @Test(timeout = 30000)
  public void testKillLeader() throws Exception {
    int countMasters = masterHostPorts.size();
    if (countMasters < 3) {
      LOG.info("This test requires at least 3 master servers, but only " + countMasters +
          " are specified.");
      return;
    }
    killMasterLeader();

    // Test that we can open a previously created table after killing the leader master.
    KuduTable table = openTable(TABLE_NAME);
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // Test that we can create a new table when one of the masters is down.
    String newTableName = TABLE_NAME + "-afterLeaderIsDead";
    createTable(newTableName, basicSchema, new CreateTableBuilder());
    table = openTable(newTableName);
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // Test that we can initialize a client when one of the masters specified in the
    // connection string is down.
    AsyncKuduClient newClient = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses).build();
    table = newClient.openTable(newTableName).join(DEFAULT_SLEEP);
    assertEquals(0, countRowsInScan(newClient.newScannerBuilder(table).build()));
  }
}
