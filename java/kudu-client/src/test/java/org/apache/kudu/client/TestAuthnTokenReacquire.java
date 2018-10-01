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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.apache.kudu.util.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.util.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.util.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kudu.Schema;
import org.apache.kudu.client.MiniKuduCluster.MiniKuduClusterBuilder;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.ClientTestUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test contains scenarios to verify that client re-acquires authn token upon expiration
 * of the current one and automatically retries the call.
 */
public class TestAuthnTokenReacquire {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuthnTokenReacquire.class);

  private static final String TABLE_NAME = "TestAuthnTokenReacquire-table";
  private static final int TOKEN_TTL_SEC = 1;
  private static final int OP_TIMEOUT_MS = 60 * TOKEN_TTL_SEC * 1000;

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  // Inject additional INVALID_AUTHENTICATION_TOKEN responses from both the master and tablet
  // servers, even for not-yet-expired tokens.
  private static final MiniKuduClusterBuilder clusterBuilder = KuduTestHarness.getBaseClusterBuilder()
      .enableKerberos()
      .addMasterServerFlag(String.format("--authn_token_validity_seconds=%d", TOKEN_TTL_SEC))
      .addMasterServerFlag("--rpc_inject_invalid_authn_token_ratio=0.5")
      .addTabletServerFlag("--rpc_inject_invalid_authn_token_ratio=0.5");

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

  private void dropConnectionsAndExpireToken() throws InterruptedException {
    // Drop all connections from the client to Kudu servers.
    dropConnections();
    // Wait for authn token expiration.
    Thread.sleep(TOKEN_TTL_SEC * 1000);
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
          final String tableName = "TestAuthnTokenReacquire-table-" + threadIdx;
          try {
            ListTabletServersResponse response = client.listTabletServers();
            assertNotNull(response);
            dropConnectionsAndExpireToken();

            ListTablesResponse tableList = client.getTablesList(tableName);
            assertNotNull(tableList);
            assertTrue(tableList.getTablesList().isEmpty());
            dropConnectionsAndExpireToken();

            client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
            dropConnectionsAndExpireToken();

            KuduTable table = client.openTable(tableName);
            assertEquals(basicSchema.getColumnCount(), table.getSchema().getColumnCount());
            dropConnectionsAndExpireToken();

            client.deleteTable(tableName);
            assertFalse(client.tableExists(tableName));
          } catch (Throwable e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            exceptions.put(threadIdx, e);
          }
        }
      });
      thread.run();
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

  @Test
  public void testBasicWorkflow() throws Exception {
    KuduTable table = client.createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    dropConnectionsAndExpireToken();

    KuduSession session = client.newSession();
    session.setTimeoutMillis(OP_TIMEOUT_MS);
    session.apply(createBasicSchemaInsert(table, 1));
    session.flush();
    RowErrorsAndOverflowStatus errors = session.getPendingErrors();
    assertFalse(errors.isOverflowed());
    assertEquals(0, session.countPendingErrors());
    dropConnectionsAndExpireToken();

    KuduTable scanTable = client.openTable(TABLE_NAME);
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, scanTable)
        .scanRequestTimeout(OP_TIMEOUT_MS)
        .build();
    assertEquals(1, countRowsInScan(scanner));
    dropConnectionsAndExpireToken();

    client.deleteTable(TABLE_NAME);
    assertFalse(client.tableExists(TABLE_NAME));
  }
}
