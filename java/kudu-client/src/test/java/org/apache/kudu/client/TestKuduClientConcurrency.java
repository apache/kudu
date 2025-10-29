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
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.kudu.Schema;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.RandomUtils;
import org.apache.kudu.test.cluster.KuduBinaryInfo;

/**
 * Concurrency and reliability tests for KuduClient including concurrent operations,
 * read-your-writes consistency, and close behavior.
 */
public class TestKuduClientConcurrency {
  private static final String TABLE_NAME = "TestKuduClientConcurrency";

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Rule(order = Integer.MIN_VALUE)
  public TestRule watcherRule = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      System.out.println("[ TEST: STARTING  ] " + description.getMethodName());
    }

    @Override
    protected void succeeded(Description description) {
      System.out.println("[ TEST: SUCCEEDED ] " + description.getMethodName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
      System.out.println("[ TEST: FAILED    ] " + description.getMethodName());
    }
  };

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  /**
   * Creates a local client that we auto-close while buffering one row, then makes sure that after
   * closing that we can read the row.
   */
  @Test(timeout = 100000)
  public void testAutoClose() throws Exception {
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      localClient.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
      KuduTable table = localClient.openTable(TABLE_NAME);
      KuduSession session = localClient.newSession();

      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      Insert insert = createBasicSchemaInsert(table, 0);
      session.apply(insert);
    }

    KuduTable table = client.openTable(TABLE_NAME);
    AsyncKuduScanner scanner =
        new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table).build();
    assertEquals(1, countRowsInScan(scanner));
  }

  /**
   * Regression test for some log spew which occurred in short-lived client instances which
   * had outbound connections.
   */
  @Test(timeout = 100000)
  public void testCloseShortlyAfterOpen() throws Exception {
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      try (KuduClient localClient =
               new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
        // Force the client to connect to the masters.
        localClient.exportAuthenticationCredentials();
      }
      // Wait a little for exceptions to come in from threads that don't get
      // synchronously joined by client.close().
      Thread.sleep(500);
    }
    // Ensure there is no log spew due to an unexpected lost connection.
    assertFalse(cla.getAppendedText(), cla.getAppendedText().contains("Exception"));
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentInsert() throws Exception {
    KuduTable table = client.createTable(
        TABLE_NAME, createManyStringsSchema(), getBasicCreateTableOptions().setWait(false));

    // Insert a row.
    //
    // It's very likely that the tablets are still being created, but the client
    // should transparently retry the insert (and associated master lookup)
    // until the operation succeeds.
    Insert insert = table.newInsert();
    insert.getRow().addString("key", "key_0");
    insert.getRow().addString("c1", "c1_0");
    insert.getRow().addString("c2", "c2_0");
    KuduSession session = client.newSession();
    OperationResponse resp = session.apply(insert);
    assertFalse(resp.hasRowError());

    // This won't do anything useful (i.e. if the insert succeeds, we know the
    // table has been created), but it's here for additional code coverage.
    assertTrue(client.isCreateTableDone(TABLE_NAME));
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentAlter() throws Exception {
    // Kick off an asynchronous table creation.
    Deferred<KuduTable> d = asyncClient.createTable(TABLE_NAME,
        createManyStringsSchema(), getBasicCreateTableOptions());

    // Rename the table that's being created to make sure it doesn't interfere
    // with the "wait for all tablets to be created" behavior of createTable().
    //
    // We have to retry this in a loop because we might run before the table
    // actually exists.
    while (true) {
      try {
        client.alterTable(TABLE_NAME,
            new AlterTableOptions().renameTable("foo"));
        break;
      } catch (KuduException e) {
        if (!e.getStatus().isNotFound()) {
          throw e;
        }
      }
    }

    // If createTable() was disrupted by the alterTable(), this will throw.
    d.join();
  }

  // This is a test that verifies, when multiple clients run
  // simultaneously, a client can get read-your-writes and
  // read-your-reads session guarantees using READ_YOUR_WRITES
  // scan mode, from leader replica. In this test writes are
  // performed in AUTO_FLUSH_SYNC (single operation) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.CLOSEST_REPLICA);
  }

  // Similar to testReadYourWritesSyncLeaderReplica, but in this
  // test writes are performed in MANUAL_FLUSH (batches) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.CLOSEST_REPLICA);
  }

  private void readYourWrites(final SessionConfiguration.FlushMode flushMode,
                              final ReplicaSelection replicaSelection)
          throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    final int tasksNum = 4;
    List<Callable<Void>> callables = new ArrayList<>();
    for (int t = 0; t < tasksNum; t++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // Create a new client.
          AsyncKuduClient asyncKuduClient = new AsyncKuduClient
                  .AsyncKuduClientBuilder(harness.getMasterAddressesAsString())
                  .defaultAdminOperationTimeoutMs(KuduTestHarness.DEFAULT_SLEEP)
                  .build();
          // From the same client continuously performs inserts to a tablet
          // in the given flush mode.
          try (KuduClient kuduClient = asyncKuduClient.syncClient()) {
            KuduSession session = kuduClient.newSession();
            session.setFlushMode(flushMode);
            KuduTable table = kuduClient.openTable(TABLE_NAME);
            for (int i = 0; i < 3; i++) {
              for (int j = 100 * i; j < 100 * (i + 1); j++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addString("key", String.format("key_%02d", j));
                row.addString("c1", "c1_" + j);
                row.addString("c2", "c2_" + j);
                row.addString("c3", "c3_" + j);
                session.apply(insert);
              }
              session.flush();

              // Perform a bunch of READ_YOUR_WRITES scans to all the replicas
              // that count the rows. And verify that the count of the rows
              // never go down from what previously observed, to ensure subsequent
              // reads will not "go back in time" regarding writes that other
              // clients have done.
              for (int k = 0; k < 3; k++) {
                AsyncKuduScanner scanner = asyncKuduClient.newScannerBuilder(table)
                                           .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                                           .replicaSelection(replicaSelection)
                                           .build();
                KuduScanner syncScanner = new KuduScanner(scanner);
                long preTs = asyncKuduClient.getLastPropagatedTimestamp();
                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, preTs);

                long rowCount = countRowsInScan(syncScanner);
                long expectedCount = 100L * (i + 1);
                assertTrue(expectedCount <= rowCount);

                // After the scan, verify that the chosen snapshot timestamp is
                // returned from the server and it is larger than the previous
                // propagated timestamp.
                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
                assertTrue(preTs < scanner.getSnapshotTimestamp());
                syncScanner.close();
              }
            }
          }
          return null;
        }
      };
      callables.add(callable);
    }
    ExecutorService executor = Executors.newFixedThreadPool(tasksNum);
    List<Future<Void>> futures = executor.invokeAll(callables);

    // Waits for the spawn tasks to complete, and then retrieves the results.
    // Any exceptions or assertion errors in the spawn tasks will be thrown here.
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  /**
   * This is a test scenario to reproduce conditions described in KUDU-3277.
   * The scenario was failing before the fix:
   *   ** 'java.lang.AssertionError: This Deferred was already called' was
   *       encountered multiple times with the stack exactly as described in
   *       KUDU-3277
   *   ** some flusher threads were unable to join since KuduSession.flush()
   *      would hang (i.e. would not return)
   */
  @MasterServerConfig(flags = {
      "--table_locations_ttl_ms=500",
  })
  @Test(timeout = 100000)
  public void testConcurrentFlush() throws Exception {
    // This is a very intensive and stressful test scenario, so run it only
    // against Kudu binaries built without sanitizers.
    assumeTrue("this scenario is to run against non-sanitized binaries only",
        KuduBinaryInfo.getSanitizerType() == KuduBinaryInfo.SanitizerType.NONE);
    try {
      AsyncKuduSession.injectLatencyBufferFlushCb(true);

      CreateTableOptions opts = new CreateTableOptions()
          .addHashPartitions(ImmutableList.of("key"), 8)
          .setRangePartitionColumns(ImmutableList.of("key"));

      Schema schema = ClientTestUtil.getBasicSchema();
      PartialRow lowerBoundA = schema.newPartialRow();
      PartialRow upperBoundA = schema.newPartialRow();
      upperBoundA.addInt("key", 0);
      opts.addRangePartition(lowerBoundA, upperBoundA);

      PartialRow lowerBoundB = schema.newPartialRow();
      lowerBoundB.addInt("key", 0);
      PartialRow upperBoundB = schema.newPartialRow();
      opts.addRangePartition(lowerBoundB, upperBoundB);

      KuduTable table = client.createTable(TABLE_NAME, schema, opts);

      final CountDownLatch keepRunning = new CountDownLatch(1);
      final int numSessions = 50;

      List<KuduSession> sessions = new ArrayList<>(numSessions);
      for (int i = 0; i < numSessions; ++i) {
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        sessions.add(session);
      }

      List<Thread> flushers = new ArrayList<>(numSessions);
      Random random = RandomUtils.getRandom();
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          Thread flusher = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                while (!keepRunning.await(random.nextInt(250), TimeUnit.MILLISECONDS)) {
                  session.flush();
                  assertEquals(0, session.countPendingErrors());
                }
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
            }
          });
          flushers.add(flusher);
        }
      }

      final int numRowsPerSession = 10000;
      final CountDownLatch insertersCompleted = new CountDownLatch(numSessions);
      List<Thread> inserters = new ArrayList<>(numSessions);
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          final int keyStart = threadIdx * numRowsPerSession;
          Thread inserter = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                for (int key = keyStart; key < keyStart + numRowsPerSession; ++key) {
                  Insert insert = ClientTestUtil.createBasicSchemaInsert(table, key);
                  assertNull(session.apply(insert));
                }
                session.flush();
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
              insertersCompleted.countDown();
            }
          });
          inserters.add(inserter);
        }
      }

      for (Thread flusher : flushers) {
        flusher.start();
      }
      for (Thread inserter : inserters) {
        inserter.start();
      }

      // Wait for the inserter threads to finish.
      insertersCompleted.await();
      // Signal the flusher threads to stop.
      keepRunning.countDown();

      for (Thread inserter : inserters) {
        inserter.join();
      }
      for (Thread flusher : flushers) {
        flusher.join();
      }

      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(numSessions * numRowsPerSession, countRowsInScan(scanner));
    } finally {
      AsyncKuduSession.injectLatencyBufferFlushCb(false);
    }
  }

  @Test(timeout = 100000)
  public void testSessionOnceClosed() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.close();
    assertTrue(session.isClosed());

    insert = createBasicSchemaInsert(table, 1);
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      session.apply(insert);
    }
    String loggedText = cla.getAppendedText();
    assertTrue("Missing warning:\n" + loggedText,
               loggedText.contains("this is unsafe"));
  }
}
