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

import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.junit.Rule;
import org.junit.Test;

public class TestTimeouts {

  private static final String TABLE_NAME =
      TestTimeouts.class.getName() + "-" + System.currentTimeMillis();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  /**
   * This test case tries different methods that should all timeout, while relying on the client to
   * pass down the timeouts to the session and scanner.
   * TODO(aserbin) this test is flaky; add delays on the server side to make it stable
   */
  @Test(timeout = 100000)
  public void testLowTimeouts() throws Exception {
    try (KuduClient lowTimeoutsClient =
         new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
         .defaultAdminOperationTimeoutMs(1)
         .defaultOperationTimeoutMs(1)
         .build()) {
      try {
        lowTimeoutsClient.listTabletServers();
        fail("Should have timed out");
      } catch (KuduException ex) {
        // Expected.
      }

      harness.getClient().createTable(TABLE_NAME, getBasicSchema(), getBasicCreateTableOptions());

      // openTable() may time out, nextRows() should time out.
      try {
        KuduTable table = lowTimeoutsClient.openTable(TABLE_NAME);

        KuduSession lowTimeoutSession = lowTimeoutsClient.newSession();

        OperationResponse response = lowTimeoutSession.apply(createBasicSchemaInsert(table, 1));
        assertTrue(response.hasRowError());
        assertTrue(response.getRowError().getErrorStatus().isTimedOut());

        KuduScanner lowTimeoutScanner = lowTimeoutsClient.newScannerBuilder(table).build();
        lowTimeoutScanner.nextRows();
        fail("Should have timed out");
      } catch (KuduException ex) {
        assertTrue(ex.getStatus().isTimedOut());
      }
    }
  }

  /**
   * KUDU-1868: This test checks that, even if there is no event on the channel over which an RPC
   * was sent (e.g., even if the server hangs and does not respond), RPCs will still time out.
   */
  @Test(timeout = 100000)
  @TabletServerConfig(flags = { "--scanner_inject_latency_on_each_batch_ms=200000" })
  public void testTimeoutEvenWhenServerHangs() throws Exception {
    // Set up a table with one row.
    KuduClient client = harness.getClient();
    KuduTable table = client.createTable(
        TABLE_NAME,
        getBasicSchema(),
        getBasicCreateTableOptions());
    assertFalse(client
        .newSession()
        .apply(createBasicSchemaInsert(table, 0))
        .hasRowError());

    // Create a new client with no socket read timeout (0 means do not set a read timeout).
    try (KuduClient noRecvTimeoutClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .defaultSocketReadTimeoutMs(0)
                 .build()) {
      // Propagate the timestamp to be sure we should see the row that was
      // inserted by another client.
      noRecvTimeoutClient.updateLastPropagatedTimestamp(client.getLastPropagatedTimestamp());
      KuduTable noRecvTimeoutTable = noRecvTimeoutClient.openTable(TABLE_NAME);

      // Do something besides a scan to cache table and tablet lookup.
      noRecvTimeoutClient.getTablesList();

      // Scan with a short timeout.
      KuduScanner scanner = noRecvTimeoutClient
          .newScannerBuilder(noRecvTimeoutTable)
          .scanRequestTimeout(1000)
          .build();

      // The server will not respond for the lifetime of the test, so we expect
      // the operation to time out.
      try {
        scanner.nextRows();
        fail("should not have completed nextRows");
      } catch (NonRecoverableException e) {
        assertTrue(e.getStatus().isTimedOut());
      }
    }
  }
}
