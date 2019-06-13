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

import static org.apache.kudu.test.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.WireProtocol.AppStatusPB;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.tserver.Tserver.TabletServerErrorPB;

public class TestAsyncKuduSession {
  private static final String TABLE_NAME = TestAsyncKuduSession.class.getName();
  private static final Schema SCHEMA = getBasicSchema();
  private static final String INJECTED_TS_ERROR = "injected error for test";

  private static AsyncKuduClient client;
  private static AsyncKuduSession session;
  private static KuduTable table;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    client = harness.getAsyncClient();
    session = client.newSession();
    table = harness.getClient().createTable(TABLE_NAME, SCHEMA, getBasicCreateTableOptions());
  }

  /**
   * Test that errors in a background flush are surfaced to clients.
   * TODO(wdberkeley): Improve the method of injecting errors into batches, here and below.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testBackgroundErrors() throws Exception {
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    session.setFlushInterval(10);
    try {
      Batch.injectTabletServerErrorAndLatency(makeTabletServerError(), 0);

      OperationResponse resp = session.apply(createInsert(1)).join(DEFAULT_SLEEP);
      assertTrue(resp.hasRowError());
      assertTrue(
          resp.getRowError().getErrorStatus()
              .getMessage().contains(INJECTED_TS_ERROR));
      assertEquals(1, session.countPendingErrors());
    } finally {
      Batch.injectTabletServerErrorAndLatency(null, 0);
    }
  }

  /**
   * Regression test for a case where an error in the previous batch could cause the next
   * batch to hang in flush().
   */
  @Test(timeout = 100000)
  public void testBatchErrorCauseSessionStuck() throws Exception {
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
    session.setFlushInterval(100);
    try {
      Batch.injectTabletServerErrorAndLatency(makeTabletServerError(), 200);
      // 0ms: Insert the first row, which will be the first batch.
      Deferred<OperationResponse> resp1 = session.apply(createInsert(1));
      Thread.sleep(120);
      // 100ms: Start to send the first batch.
      // 100ms+: The first batch receives a response from the tablet leader, and
      //         will wait 200s and throw an error.
      // 120ms: Insert another row, which will be the second batch.
      Deferred<OperationResponse> resp2 = session.apply(createInsert(2));
      // 220ms: Start to send the second batch while the first batch is in flight.
      // 300ms+: The first batch completes with an error. The second batch is in flight.
      {
        OperationResponse resp = resp1.join(DEFAULT_SLEEP);
        assertTrue(resp.hasRowError());
        assertTrue(
            resp.getRowError().getErrorStatus()
                .getMessage().contains(INJECTED_TS_ERROR));
      }
      // 300ms++: The second batch completes with an error. It does not remain stuck flushing.
      {
        OperationResponse resp = resp2.join(DEFAULT_SLEEP);
        assertTrue(resp.hasRowError());
        assertTrue(
            resp.getRowError().getErrorStatus()
                .getMessage().contains(INJECTED_TS_ERROR));
      }
      assertFalse(session.hasPendingOperations());
    } finally {
      Batch.injectTabletServerErrorAndLatency(null, 0);
    }
  }

  /**
   * Regression test for a case when a tablet lookup error causes the original write RPC to hang.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testGetTableLocationsErrorCausesStuckSession() throws Exception {
    // Make sure tablet locations are cached.
    Insert insert = createInsert(1);
    session.apply(insert).join(DEFAULT_SLEEP);
    RemoteTablet rt =
        client.getTableLocationEntry(table.getTableId(), insert.partitionKey()).getTablet();
    String tabletId = rt.getTabletId();
    RpcProxy proxy = client.newRpcProxy(rt.getLeaderServerInfo());
    // Delete the table so subsequent writes fail with 'table not found'.
    client.deleteTable(TABLE_NAME).join();
    // Wait until the tablet is deleted on the TS.
    while (true) {
      ListTabletsRequest req = new ListTabletsRequest(client.getTimer(), 10000);
      Deferred<ListTabletsResponse> d = req.getDeferred();
      proxy.sendRpc(req);
      ListTabletsResponse resp = d.join();
      if (!resp.getTabletsList().contains(tabletId)) {
        break;
      }
      Thread.sleep(100);
    }

    OperationResponse response = session.apply(createInsert(1)).join(DEFAULT_SLEEP);
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isNotFound());
  }

  /** Regression test for a failure to correctly handle a timeout when flushing a batch. */
  @Test
  public void testInsertIntoUnavailableTablet() throws Exception {
    harness.killAllTabletServers();
    session.setTimeoutMillis(1);
    OperationResponse response = session.apply(createInsert(1)).join();
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isTimedOut());

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createInsert(1);
    session.apply(insert);
    List<OperationResponse> responses = session.flush().join();
    assertEquals(1, responses.size());
    assertTrue(responses.get(0).getRowError().getErrorStatus().isTimedOut());
  }

  /**
   * Regression test for a bug in which, when a tablet client is disconnected
   * and we reconnect, we were previously leaking the old RpcProxy
   * object in the client2tablets map.
   */
  @Test(timeout = 100000)
  public void testRestartBetweenWrites() throws Exception {
    // Create a non-replicated table for this test, so that
    // we're sure when we reconnect to the leader after restarting
    // the tablet servers, it's definitely the same leader we wrote
    // to before.
    KuduTable nonReplicatedTable = harness.getClient().createTable(
        "non-replicated",
        SCHEMA,
        getBasicCreateTableOptions().setNumReplicas(1));

    // Write before doing any restarts to establish a connection.
    session.setTimeoutMillis(30000);
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    session.apply(createBasicSchemaInsert(nonReplicatedTable, 1)).join();

    int numClientsBefore = client.getConnectionListCopy().size();

    // Restart all the tablet servers.
    harness.killAllTabletServers();
    harness.startAllTabletServers();

    // Perform another write, which will require reconnecting to the same
    // tablet server that we wrote to above.
    session.apply(createBasicSchemaInsert(nonReplicatedTable, 2)).join();

    // We should not have leaked an entry in the client2tablets map.
    int numClientsAfter = client.getConnectionListCopy().size();
    assertEquals(numClientsBefore, numClientsAfter);
  }

  /**
   * Regression test for KUDU-232, where, in AUTO_FLUSH_BACKGROUND mode, a call to `flush` could
   * throw instead of blocking on in-flight ops that are doing tablet lookups.
   */
  @Test(timeout = 100000)
  public void testKUDU232() throws Exception {
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);

    // Set the flush interval high enough that the operation won't flush in the background before
    // the call to `flush`.
    session.setFlushInterval(DEFAULT_SLEEP + 1000);
    session.apply(createInsert(0));

    // `flush` should not throw and should block until the row has been flushed. Ergo, the row
    // should now be readable server-side by this client.
    session.flush().join(DEFAULT_SLEEP);
    assertEquals(1, countRowsInTable(table));
  }

  /**
   * Test that changing the flush mode while ops are in flight results in an error.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testChangingFlushModeWithOpsInFlightIsAnError() throws Exception {
    // Buffer an operation in MANUAL_FLUSH mode.
    session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
    session.apply(createInsert(10));

    try {
      // `flush` was never called, so switching the flush mode is an error.
      session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_SYNC);
      fail();
    } catch (IllegalArgumentException ex) {
      // Furthermore, the flush mode should not have changed.
      assertTrue(ex.getMessage().contains("Cannot change flush mode when writes are buffered"));
      assertEquals(session.getFlushMode(), AsyncKuduSession.FlushMode.MANUAL_FLUSH);
    }
  }

  /**
   * Test the behavior of AUTO_FLUSH_SYNC mode.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testAutoFlushSync() throws Exception {
    final int kNumOps = 1000;
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_SYNC);

    // Apply a bunch of operations. There's no buffer to overflow, but the client does need to track
    // each op to know if and when it has succeeded.
    List<Deferred<OperationResponse>> opResponses = new ArrayList<>();
    for (int i = 0; i < kNumOps; i++) {
      opResponses.add(session.apply(createInsert(i)));
    }

    // Wait on all the ops. After this, all ops should be visible. No explicit flush required.
    Deferred.group(opResponses).join(DEFAULT_SLEEP);
    assertEquals(kNumOps, countRowsInTable(table));
  }

  /**
   * Test the behavior of MANUAL_FLUSH mode.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testManualFlush() throws Exception {
    final int kBufferSizeOps = 10;
    session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
    session.setMutationBufferSpace(kBufferSizeOps);

    // Fill the buffer.
    for (int i = 0; i < kBufferSizeOps; i++) {
      session.apply(createInsert(i));
    }

    // There was no call to flush, so there should be no rows in the table.
    assertEquals(0, countRowsInTable(table));

    // Attempting to buffer another op is an error.
    try {
      session.apply(createInsert(kBufferSizeOps + 1));
      fail();
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("MANUAL_FLUSH is enabled but the buffer is too big"));
    }

    // Now flush. There should be `kBufferSizeOps` rows in the end.
    session.flush().join(DEFAULT_SLEEP);
    assertEquals(kBufferSizeOps, countRowsInTable(table));

    // Applying another operation should now succeed.
    session.apply(createInsert(kBufferSizeOps + 1));
  }

  /**
   * Test the behavior of AUTO_FLUSH_BACKGROUND mode. Because this mode does a lot of work in the
   * background, possibly on different threads, it's difficult to test precisely.
   * TODO(wdberkeley): Invent better ways of testing AUTO_FLUSH_BACKGROUND edge cases.
   * @throws Exception
   */
  @Test
  public void testAutoFlushBackground() throws Exception {
    final int kBufferSizeOps = 10;
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
    session.setMutationBufferSpace(kBufferSizeOps);

    // In AUTO_FLUSH_BACKGROUND mode, the session can accept up to 2x the buffer size of ops before
    // it might be out of buffer space (depending on if a background flush finishes).
    for (int i = 0; i < 2 * kBufferSizeOps; i++) {
      session.apply(createInsert(i));
    }

    // If the client tries to buffer many more operations, it may receive a PleaseThrottleException.
    // In this case, if the client simply waits for a flush notification on the Deferred returned
    // with the exception, it can continue to buffer operations.
    final int kNumOpsMultipler = 10;
    for (int i = 2 * kBufferSizeOps; i < kNumOpsMultipler * kBufferSizeOps; i++) {
      Insert insert = createInsert(i);
      try {
        session.apply(insert);
      } catch (PleaseThrottleException ex) {
        ex.getDeferred().join(DEFAULT_SLEEP);
        session.apply(insert);
      }
    }

    // After a final call to `flush` all operations should be visible to this client.
    session.flush().join(DEFAULT_SLEEP);
    assertEquals(kNumOpsMultipler * kBufferSizeOps, countRowsInTable(table));
  }

  /**
   * Test a tablet going missing or encountering a new tablet while inserting a lot of data. This
   * code used to fail in many different ways.
   * @throws Exception
   */
  @Test(timeout = 100000)
  public void testTabletCacheInvalidatedDuringWrites() throws Exception {
    final int kNumOps = 10000;
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);

    // Insert 2 * kNumOps rows, but drop the locations cache partway through.
    for (int i = 0; i < kNumOps; i++) {
      Insert insert = createInsert(i);
      try {
        session.apply(insert);
      } catch (PleaseThrottleException ex) {
        ex.getDeferred().join(DEFAULT_SLEEP);
        session.apply(insert);
      }
    }

    client.emptyTabletsCacheForTable(table.getTableId());

    for (int i = kNumOps; i < 2 * kNumOps; i++) {
      Insert insert = createInsert(i);
      try {
        session.apply(insert);
      } catch (PleaseThrottleException ex) {
        ex.getDeferred().join(DEFAULT_SLEEP);
        session.apply(insert);
      }
    }

    session.flush().join(DEFAULT_SLEEP);
    assertEquals(2 * kNumOps, countRowsInTable(table));
  }

  // A helper just to make some lines shorter.
  private Insert createInsert(int key) {
    return createBasicSchemaInsert(table, key);
  }

  private TabletServerErrorPB makeTabletServerError() {
    return TabletServerErrorPB.newBuilder()
        .setCode(TabletServerErrorPB.Code.UNKNOWN_ERROR)
        .setStatus(AppStatusPB.newBuilder()
            .setCode(AppStatusPB.ErrorCode.UNKNOWN_ERROR)
            .setMessage(INJECTED_TS_ERROR)
            .build())
        .build();
  }
}
