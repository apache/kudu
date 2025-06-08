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
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.KuduTestHarness;

/**
 * Tests {@link AsyncKuduClient} with multiple masters.
 */
public class TestMasterFailover {
  enum KillBefore {
    CREATE_CLIENT,
    CREATE_TABLE,
    OPEN_TABLE,
    SCAN_TABLE
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test(timeout = 30000)
  public void testKillLeaderBeforeCreateClient() throws Exception {
    doTestKillLeader(KillBefore.CREATE_CLIENT);
  }

  @Test(timeout = 30000)
  public void testKillLeaderBeforeCreateTable() throws Exception {
    doTestKillLeader(KillBefore.CREATE_TABLE);
  }

  @Test(timeout = 30000)
  public void testKillLeaderBeforeOpenTable() throws Exception {
    doTestKillLeader(KillBefore.OPEN_TABLE);
  }

  @Test(timeout = 30000)
  public void testKillLeaderBeforeScanTable() throws Exception {
    doTestKillLeader(KillBefore.SCAN_TABLE);
  }

  private void doTestKillLeader(KillBefore killBefore) throws Exception {
    String tableName = "TestMasterFailover-killBefore=" + killBefore;
    int countMasters = harness.getMasterServers().size();
    if (countMasters < 3) {
      throw new Exception("This test requires at least 3 master servers, but only " +
          countMasters + " are specified.");
    }

    if (killBefore == KillBefore.CREATE_CLIENT) {
      harness.killLeaderMasterServer();
    }
    try (KuduClient c =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      if (killBefore == KillBefore.CREATE_TABLE) {
        harness.killLeaderMasterServer();
      }

      harness.getClient().createTable(tableName, getBasicSchema(), getBasicCreateTableOptions());

      if (killBefore == KillBefore.OPEN_TABLE) {
        harness.killLeaderMasterServer();
      }

      // Test that we can open a previously created table after killing the leader master.
      KuduTable table = harness.getClient().openTable(tableName);

      if (killBefore == KillBefore.SCAN_TABLE) {
        harness.killLeaderMasterServer();
      }
      assertEquals(0,
          countRowsInScan(harness.getAsyncClient().newScannerBuilder(table).build()));
    }
  }
}
