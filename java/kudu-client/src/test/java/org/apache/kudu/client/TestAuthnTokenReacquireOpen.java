/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.kudu.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test contains a special scenario to make sure the automatic authn token re-acquisition works
 * in the case when the client has established a connection to the master using secondary
 * credentials. The subtlety is that an authn token cannot be acquired using such a connection,
 * so this test verifies that the client opens a new connection using its primary credentials to
 * acquire a new authentication token and automatically retries its RPCs with the new authn token.
 */
public class TestAuthnTokenReacquireOpen extends BaseKuduTest {

  private static final String TABLE_NAME = "TestAuthnTokenReacquireOpen-table";
  private static final int TOKEN_TTL_SEC = 1;
  private static final int OP_TIMEOUT_MS = 60 * TOKEN_TTL_SEC * 1000;
  private static final int KEEPALIVE_TIME_MS = 2 * OP_TIMEOUT_MS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set appropriate TTL for authn token and connection keep-alive property, so the client could
    // keep an open connection to the master when its authn token is already expired. Inject
    // additional INVALID_AUTHENTICATION_TOKEN responses from the tablet server even for
    // not-yet-expired tokens for an extra stress on the client.
    miniClusterBuilder
        .enableKerberos()
        .addMasterFlag(String.format("--authn_token_validity_seconds=%d", TOKEN_TTL_SEC))
        .addMasterFlag(String.format("--rpc_default_keepalive_time_ms=%d", KEEPALIVE_TIME_MS))
        .addTserverFlag(String.format("--rpc_default_keepalive_time_ms=%d", KEEPALIVE_TIME_MS))
        .addTserverFlag("--rpc_inject_invalid_authn_token_ratio=0.5");

    // We want to have a cluster with a single master.
    final int NUM_MASTERS = 1;
    final int NUM_TABLET_SERVERS = 3;
    doSetup(NUM_MASTERS, NUM_TABLET_SERVERS);
  }

  private static void dropConnections() {
    for (Connection c : client.getConnectionListCopy()) {
      c.disconnect();
    }
  }

  private static void expireToken() throws InterruptedException {
    // Wait for authn token expiration.
    Thread.sleep(TOKEN_TTL_SEC * 1000);
  }

  @Test
  public void test() throws Exception {
    // Establish a connection to the cluster, get the list of tablet servers. That would fetch
    // an authn token.
    ListTabletServersResponse response = syncClient.listTabletServers();
    assertNotNull(response);
    dropConnections();

    // The connection to the master has been dropped. Make a call to the master again so the client
    // would create a new connection using authn token.
    ListTablesResponse tableList = syncClient.getTablesList(null);
    assertNotNull(tableList);
    assertTrue(tableList.getTablesList().isEmpty());

    syncClient.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    assertTrue(syncClient.tableExists(TABLE_NAME));

    expireToken();

    // Try scan table rows once the authn token has expired. This request goes to corresponding
    // tablet server, and a new connection should be negotiated. During connection negotiation,
    // the server authenticates the client using authn token, which is expired.
    KuduTable scanTable = syncClient.openTable(TABLE_NAME);
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(client, scanTable)
        .scanRequestTimeout(OP_TIMEOUT_MS)
        .build();
    assertEquals(0, countRowsInScan(scanner));

    syncClient.deleteTable(TABLE_NAME);
    assertFalse(syncClient.tableExists(TABLE_NAME));
  }
}
