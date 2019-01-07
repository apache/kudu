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

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.kudu.client.SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND;
import static org.apache.kudu.client.SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC;
import static org.apache.kudu.test.ClientTestUtil.*;
import static org.junit.Assert.assertEquals;

public class TestMultiMasterAuthzTokens {
  private static final MiniKuduCluster.MiniKuduClusterBuilder clusterBuilder =
      KuduTestHarness.getBaseClusterBuilder()
          .addMasterServerFlag("--authz_token_validity_seconds=1")
          .addTabletServerFlag("--tserver_enforce_access_control=true")
          // Inject invalid tokens such that operations will be forced to go
          // back to the master for an authz token.
          .addTabletServerFlag("--tserver_inject_invalid_authz_token_ratio=0.5");

  private static final String tableName = "TestMultiMasterAuthzToken-table";

  private KuduClient client;

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  /**
   * Utility to send RPCs to add rows given the specified flush mode.
   * Inserts rows with keys [startRow, endRow).
   */
  private void insertRows(KuduTable table, SessionConfiguration.FlushMode mode,
                          int startRow, int endRow) throws Exception {
    KuduSession session = client.newSession();
    session.setFlushMode(mode);
    for (int i = startRow; i < endRow; i++) {
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
    }
    session.flush();
  }

  /**
   * Utility to send RPCs to add rows given the specified flush mode.
   * Upserts rows with keys [startRow, endRow).
   */
  private void upsertRows(KuduTable table, SessionConfiguration.FlushMode mode,
                          int startRow, int endRow) throws Exception {
    KuduSession session = client.newSession();
    session.setFlushMode(mode);
    for (int i = startRow; i < endRow; i++) {
      Upsert upsert = createBasicSchemaUpsert(table, i);
      session.apply(upsert);
    }
    session.flush();
  }

  @Test
  public void testAuthzTokensDuringElection() throws Exception {
    // Test sending various requests that require authorization.
    final KuduTable table = client.createTable(tableName, getBasicSchema(),
        getBasicCreateTableOptions().setNumReplicas(1));

    // Restart the masters to trigger an election.
    harness.killAllMasterServers();
    harness.startAllMasterServers();

    final int NUM_REQS = 10;
    insertRows(table, AUTO_FLUSH_SYNC, 0, NUM_REQS);

    // Do the same for batches of inserts.
    harness.killAllMasterServers();
    harness.startAllMasterServers();
    insertRows(table, AUTO_FLUSH_BACKGROUND, NUM_REQS, 2 * NUM_REQS);

    // And for scans.
    harness.killAllMasterServers();
    harness.startAllMasterServers();
    for (int i = 0; i < NUM_REQS; i++) {
      assertEquals(2 * NUM_REQS, countRowsInTable(table));
    }
  }

  @Test
  public void testAuthzTokenExpiration() throws Exception {
    // Test a long-running concurrent workload with different types of requests
    // being sent, all the while injecting invalid tokens, with a short authz
    // token expiration time. The threads should reacquire tokens as needed
    // without surfacing token errors to the client.
    final int TEST_RUNTIME_MS = 30000;
    final KuduTable table = client.createTable(tableName, getBasicSchema(),
        getBasicCreateTableOptions().setNumReplicas(1));
    final CountDownLatch latch = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(3);
    List<Future<Exception>> exceptions = new ArrayList<>();
    exceptions.add(pool.submit(new Callable<Exception>() {
      @Override
      public Exception call() throws Exception {
        try {
          int batch = 0;
          while (latch.getCount() > 0) {
            // Send writes without batching.
            upsertRows(table, AUTO_FLUSH_SYNC, batch * 10, (++batch) * 10);
          }
        } catch (Exception e) {
          return e;
        }
        return null;
      }
    }));
    exceptions.add(pool.submit(new Callable<Exception>() {
      @Override
      public Exception call() throws Exception {
        try {
          int batch = 0;
          while (latch.getCount() > 0) {
            // Also send writes with batching.
            upsertRows(table, AUTO_FLUSH_BACKGROUND, batch * 10, (++batch) * 10);
          }
        } catch (Exception e) {
          return e;
        }
        return null;
      }
    }));
    exceptions.add(pool.submit(new Callable<Exception>() {
      @Override
      public Exception call() throws Exception {
        try {
          while (latch.getCount() > 0) {
            // We can't guarantee a row count, but catch any exceptions.
            countRowsInTable(table);
          }
        } catch (Exception e) {
          return e;
        }
        return null;
      }
    }));
    Thread.sleep(TEST_RUNTIME_MS);
    latch.countDown();
    int fails = 0;
    for (Future<Exception> future : exceptions) {
      Exception e = future.get();
      if (e != null) {
        e.printStackTrace();
        fails++;
      }
    }
    assertEquals(0, fails);
  }
}
