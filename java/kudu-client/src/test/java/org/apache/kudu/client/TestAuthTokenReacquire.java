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
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.security.Token;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

/**
 * This test contains scenarios to verify that clients re-acquire tokens upon
 * expiration of the current one and automatically retries affected calls.
 */
public class TestAuthTokenReacquire {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuthTokenReacquire.class);

  private static final String TABLE_NAME = "TestAuthTokenReacquire-table";

  // Set a low token timeout.
  private static final int TOKEN_TTL_SEC = 1;
  private static final int OP_TIMEOUT_MS = 60 * TOKEN_TTL_SEC * 1000;

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  // Inject additional INVALID_AUTHENTICATION_TOKEN responses from both the
  // master and tablet servers, even for not-yet-expired tokens.
  private static final MiniKuduClusterBuilder clusterBuilder = KuduTestHarness.getBaseClusterBuilder()
      .enableKerberos()
      .addMasterServerFlag(String.format("--authn_token_validity_seconds=%d", TOKEN_TTL_SEC))
      .addMasterServerFlag(String.format("--authz_token_validity_seconds=%d", TOKEN_TTL_SEC))
      .addMasterServerFlag("--rpc_inject_invalid_authn_token_ratio=0.5")
      .addTabletServerFlag("--rpc_inject_invalid_authn_token_ratio=0.5")
      .addTabletServerFlag("--tserver_enforce_access_control=true")
      .addTabletServerFlag("--tserver_inject_invalid_authz_token_ratio=0.5");

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  private void dropConnections() {
    for (Connection c : asyncClient.getConnectionListCopy()) {
      c.disconnect();
    }
  }

  private void dropConnectionsAndExpireTokens() throws InterruptedException {
    // Drop all connections from the client to Kudu servers.
    dropConnections();
    // Wait for token expiration. Since we've just dropped all connections, this
    // means that we'll need to get a new authn token upon sending the next RPC.
    expireTokens();
  }

  private void expireTokens() throws InterruptedException {
    // Sleep long enough for the authn/authz tokens to expire. Wait for just
    // past the token TTL to avoid making this test flaky, e.g. in case the
    // token just misses being considered expired.
    Thread.sleep((TOKEN_TTL_SEC + 1) * 1000);
  }

  @Test
  public void testBasicMasterOperations() throws Exception {
    // To ratchet up the intensity a bit, run the scenario by several concurrent threads.
    List<Thread> threads = new ArrayList<>();
    final Map<Integer, Throwable> exceptions =
        Collections.synchronizedMap(new HashMap<Integer, Throwable>());
    for (int i = 0; i < 8; ++i) {
      final int threadIdx = i;
      Thread thread = new Thread(new Runnable() {
        @Override
        @SuppressWarnings("AssertionFailureIgnored")
        public void run() {
          final String tableName = "TestAuthTokenReacquire-table-" + threadIdx;
          try {
            ListTabletServersResponse response = client.listTabletServers();
            assertNotNull(response);
            dropConnectionsAndExpireTokens();

            ListTablesResponse tableList = client.getTablesList(tableName);
            assertNotNull(tableList);
            assertTrue(tableList.getTablesList().isEmpty());
            dropConnectionsAndExpireTokens();

            try {
              client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
            } catch (KuduException ex) {
              // Swallow "table already exists" exceptions. This might happen if the thread sends
              // a CreateTable request, the master receives it, but the connection is dropped by
              // another thread before the client receives a success response, and then the client
              // retries.
              // TODO(KUDU-1537): Remove this workaround when table creation is exactly-once.
              Status exStatus = ex.getStatus();
              if (!exStatus.isAlreadyPresent() && !exStatus.isServiceUnavailable()) {
                throw ex;
              }
            }
            dropConnectionsAndExpireTokens();

            KuduTable table = client.openTable(tableName);
            assertEquals(basicSchema.getColumnCount(), table.getSchema().getColumnCount());
            dropConnectionsAndExpireTokens();

            try {
              client.deleteTable(tableName);
            } catch (KuduException ex) {
              // See the above comment about table creation. The same idea applies to table deletion.
              // TODO(KUDU-1537): Remove this workaround when table deletion is exactly-once.
              if (!ex.getStatus().isNotFound()) {
                throw ex;
              }
            }
            assertFalse(client.tableExists(tableName));
          } catch (Throwable e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            exceptions.put(threadIdx, e);
          }
        }
      });
      thread.start();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      thread.join();
    }
    if (!exceptions.isEmpty()) {
      for (Map.Entry<Integer, Throwable> e : exceptions.entrySet()) {
        LOG.error("exception in thread {}: {}", e.getKey(), e.getValue());
      }
      fail("test failed: unexpected errors");
    }
  }

  private int countRowsInTable(KuduTable table) throws Exception {
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table)
        .scanRequestTimeout(OP_TIMEOUT_MS)
        .build();
    return countRowsInScan(scanner);
  }

  private void insertRowWithKey(KuduSession session, KuduTable table, int key) throws Exception {
    session.apply(createBasicSchemaInsert(table, key));
    session.flush();
    RowErrorsAndOverflowStatus errors = session.getPendingErrors();
    assertFalse(errors.isOverflowed());
    assertEquals(0, session.countPendingErrors());
  }

  private List<KeyRange> splitKeyRange(KuduTable table) throws Exception {
    // Note: the nulls are for key bounds; we don't really care about them.
    return table.getAsyncClient().getTableKeyRanges(table, null, null, null, null,
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP, 1, DEFAULT_SLEEP).join();
  }

  @Test
  public void testBasicWorkflow() throws Exception {
    KuduTable table = client.createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    String tableId = table.getTableId();
    int key = 0;

    // Drop all connections so the first write needs to reconnect with a new authn token.
    Token.SignedTokenPB originalToken = asyncClient.securityContext.getAuthenticationToken();
    dropConnectionsAndExpireTokens();
    KuduSession session = client.newSession();
    session.setTimeoutMillis(OP_TIMEOUT_MS);
    insertRowWithKey(session, table, ++key);

    // Verify that we got a different authn token.
    assertFalse(asyncClient.securityContext.getAuthenticationToken().equals(originalToken));

    // Now wait for the authz token to expire and do a write.
    originalToken = asyncClient.getAuthzToken(tableId);
    expireTokens();
    insertRowWithKey(session, table, ++key);

    // Verify that we got a different authz token.
    assertFalse(asyncClient.getAuthzToken(tableId).equals(originalToken));

    // Drop all connections so the first scan needs to reconnect with a new authn token.
    originalToken = asyncClient.securityContext.getAuthenticationToken();
    dropConnectionsAndExpireTokens();
    KuduTable scanTable = client.openTable(TABLE_NAME);
    assertEquals(key, countRowsInTable(scanTable));
    assertFalse(asyncClient.securityContext.getAuthenticationToken().equals(originalToken));

    // Now wait for the authz token to expire and do a scan.
    originalToken = asyncClient.getAuthzToken(tableId);
    expireTokens();
    assertEquals(key, countRowsInTable(scanTable));
    assertFalse(asyncClient.getAuthzToken(tableId).equals(originalToken));

    // Now wait for the authz token to expire and send a request to split the
    // key range. It should succeed and get a new authz token.
    originalToken = asyncClient.getAuthzToken(tableId);
    expireTokens();
    assertFalse(splitKeyRange(scanTable).isEmpty());
    assertFalse(asyncClient.getAuthzToken(tableId).equals(originalToken));

    // Force the client to get a new authn token and delete the table.
    originalToken = asyncClient.securityContext.getAuthenticationToken();
    dropConnectionsAndExpireTokens();
    client.deleteTable(TABLE_NAME);
    assertFalse(client.tableExists(TABLE_NAME));
    assertFalse(asyncClient.securityContext.getAuthenticationToken().equals(originalToken));
  }
}
