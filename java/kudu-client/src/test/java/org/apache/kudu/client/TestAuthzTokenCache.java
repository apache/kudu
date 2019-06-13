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

import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.security.Token;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster;

public class TestAuthzTokenCache {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuthzTokenCache.class);

  // This tests basic functionality of the authz token cache (e.g. putting
  // things in, getting stuff out).
  private static final MiniKuduCluster.MiniKuduClusterBuilder clusterBuilder =
      KuduTestHarness.getBaseClusterBuilder()
          .enableKerberos();

  private static final String tableName = "TestAuthzTokenCache-table";

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  // Retrieves a new authz token from the master (regardless of whether there is
  // already one in the authz token cache).
  public void fetchAuthzToken(KuduTable table) throws Exception {
    // Send a dummy RPC via the token cache. This will run a scan RPC
    // after retrieving a new authz token.
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table)
        .build();
    KuduRpc<AsyncKuduScanner.Response> req = scanner.getOpenRequest();
    Deferred<AsyncKuduScanner.Response> d = req.getDeferred();
    asyncClient.getAuthzTokenCache().retrieveAuthzToken(req,
        new InvalidAuthzTokenException(Status.IOError("test failure")));
    assertNotNull(d.join());
  }

  @Test
  public void testBasicAuthzTokenCache() throws Exception {
    // First, do a sanity check that we get an authz token in the first place
    // upon accessing a table.
    final KuduTable table = client.createTable(tableName, getBasicSchema(),
        getBasicCreateTableOptions());
    AuthzTokenCache tokenCache = asyncClient.getAuthzTokenCache();
    String tableId = table.getTableId();
    Token.SignedTokenPB originalToken = asyncClient.getAuthzToken(tableId);
    assertNotNull(originalToken);

    // Wait a bit so the next token we get will be different. A different token
    // will be generated every second by virtue of having a different
    // expiration, which is in seconds.
    Thread.sleep(1100);

    // Send a dummy RPC via the token cache, sending it only after getting a new
    // authz token.
    fetchAuthzToken(table);

    // Verify we actually got a new authz token.
    assertFalse(asyncClient.getAuthzToken(tableId).equals(originalToken));

    // Now put the original token directly in the cache.
    tokenCache.put(tableId, originalToken);
    assertTrue(asyncClient.getAuthzToken(tableId).equals(originalToken));
  }

  @Test
  public void testRetrieveAuthzTokensInParallel() throws Exception {
    final KuduTable table = client.createTable(tableName, getBasicSchema(),
        getBasicCreateTableOptions());
    final String tableId = table.getTableId();
    class AuthzTokenFetcher implements Callable<Exception> {
      @Override
      public Exception call() {
        try {
          fetchAuthzToken(table);
        } catch (Exception e) {
          return e;
        }
        return null;
      }
    }
    // Send a bunch of authz token requests in parallel.
    final int NUM_THREADS = 30;
    ArrayList<AuthzTokenFetcher> fetchers = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      fetchers.add(new AuthzTokenFetcher());
    }
    int fails = 0;
    final ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<Exception>> exceptions = pool.invokeAll(fetchers);
    pool.shutdown();
    for (int i = 0; i < NUM_THREADS; i++) {
      Exception e = exceptions.get(i).get();
      if (e != null) {
        fails++;
        e.printStackTrace();
      }
    }
    assertEquals(0, fails);
    // We should have gotten a token with all those retrievals, and sent a
    // number of RPCs that was lower than the number of threads.
    assertNotNull(asyncClient.getAuthzToken(tableId));
    int numRetrievals = asyncClient.getAuthzTokenCache().numRetrievalsSent();
    LOG.debug(String.format("Sent %d RPCs for %d threads", numRetrievals, NUM_THREADS));
    assertTrue(0 < numRetrievals);
    assertTrue(numRetrievals < NUM_THREADS);
  }
}
