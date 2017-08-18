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

import static org.apache.kudu.util.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.List;
import org.apache.kudu.util.AssertHelpers.BooleanExpression;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestClientFailoverSupport extends BaseKuduTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    final int NUM_TABLET_SERVERS = 6;
    BaseKuduTest.doSetup(3, NUM_TABLET_SERVERS);
  }

  private void waitUntilRowCount(final KuduTable table, final int rowCount, long timeoutMs)
      throws Exception {
    assertEventuallyTrue(String.format("Read count should be %s", rowCount),
        new BooleanExpression() {
          @Override
          public boolean get() throws Exception {
            AsyncKuduScanner scanner = client.newScannerBuilder(table).build();
            int read_count = countRowsInScan(scanner);
            return read_count == rowCount;
          }
        }, timeoutMs);
  }

  /**
   * Tests that the Java client will appropriately failover when a new master leader is elected.
   * We force a metadata update by killing the active tablet servers.
   * Then we kill the master leader.
   * We write some more rows.
   * If we can successfully read back the rows written, that shows the client handled the failover
   * correctly.
   */
  @Test(timeout = 100000)
  public void testMultipleFailover() throws Exception {
    final String TABLE_NAME = TestClientFailoverSupport.class.getName();

    createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());

    KuduTable table = openTable(TABLE_NAME);
    KuduSession session = syncClient.newSession();

    final int TOTAL_ROWS_TO_INSERT = 10;
    final int TSERVER_LEADERS_TO_KILL = 3;

    for (int i = 0; i < TOTAL_ROWS_TO_INSERT; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    waitUntilRowCount(table, TOTAL_ROWS_TO_INSERT, DEFAULT_SLEEP);

    for (int i = 0; i < TSERVER_LEADERS_TO_KILL; i++) {
      List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
      assertEquals(1, tablets.size());
      final int leaderPort = findLeaderTabletServerPort(tablets.get(0));
      miniCluster.killTabletServerOnPort(leaderPort);
    }
    killMasterLeader();

    for (int i = TOTAL_ROWS_TO_INSERT; i < 2*TOTAL_ROWS_TO_INSERT; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    waitUntilRowCount(table, 2*TOTAL_ROWS_TO_INSERT, DEFAULT_SLEEP);
    syncClient.deleteTable(TABLE_NAME);
    assertFalse(syncClient.tableExists(TABLE_NAME));
  }
}