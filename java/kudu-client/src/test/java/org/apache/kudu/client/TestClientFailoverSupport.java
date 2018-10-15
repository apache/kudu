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

import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.apache.kudu.test.junit.AssertHelpers.assertEventuallyTrue;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.junit.AssertHelpers.BooleanExpression;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestClientFailoverSupport {

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private CapturingLogAppender cla = new CapturingLogAppender();
  private Closeable claAttach;

  enum MasterFailureType {
    RESTART,
    KILL
  }

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
    claAttach = cla.attach();
  }

  @After
  public void checkLogs() throws IOException {
    claAttach.close();
    String log = cla.getAppendedText();
    assertFalse("Log should not contain Netty internals",
        log.contains("socket.nio.AbstractNioSelector"));
  }

  private void waitUntilRowCount(final KuduTable table, final int rowCount, long timeoutMs)
      throws Exception {
    assertEventuallyTrue(String.format("Read count should be %s", rowCount),
        new BooleanExpression() {
          @Override
          public boolean get() throws Exception {
            AsyncKuduScanner scanner = asyncClient.newScannerBuilder(table).build();
            int read_count = countRowsInScan(scanner);
            return read_count == rowCount;
          }
        }, timeoutMs);
  }

  @Test(timeout = 100000)
  public void testRestartLeaderMaster() throws Exception {
    doTestMasterFailover(MasterFailureType.RESTART);
  }

  @Test(timeout = 100000)
  public void testKillLeaderMaster() throws Exception {
    doTestMasterFailover(MasterFailureType.KILL);
  }

  /**
   * Tests that the Java client will appropriately failover when a new master leader is elected.
   *
   * We inject some failure on the master, based on 'failureType'. Then we force a tablet
   * re-election by killing the leader replica. The client then needs to reconnect to the masters
   * to find the new location information.
   *
   * If we can successfully read back the rows written, that shows the client handled the failover
   * correctly.
   */
  private void doTestMasterFailover(MasterFailureType failureType) throws Exception {
    final String TABLE_NAME = TestClientFailoverSupport.class.getName()
        + "-" + failureType;
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());

    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();

    final int TOTAL_ROWS_TO_INSERT = 10;

    for (int i = 0; i < TOTAL_ROWS_TO_INSERT; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    waitUntilRowCount(table, TOTAL_ROWS_TO_INSERT, DEFAULT_SLEEP);

    // Kill or restart the leader master.
    switch (failureType) {
    case KILL:
      harness.killLeaderMasterServer();
      break;
    case RESTART:
      harness.restartLeaderMaster();
      break;
    }

    // Kill the tablet server leader. This will force us to go back to the
    // master to find the new location. At that point, the client will
    // notice that the old leader master is no longer current and fail over
    // to the new one.
    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(1, tablets.size());
    harness.killTabletLeader(tablets.get(0));

    // Insert some more rows.
    for (int i = TOTAL_ROWS_TO_INSERT; i < 2*TOTAL_ROWS_TO_INSERT; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    waitUntilRowCount(table, 2*TOTAL_ROWS_TO_INSERT, DEFAULT_SLEEP);
    client.deleteTable(TABLE_NAME);
    assertFalse(client.tableExists(TABLE_NAME));
  }
}