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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kudu.util.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.util.ClientTestUtil.createBasicSchemaInsert;

/**
 * Integration test for the client. RPCs are sent to Kudu from multiple threads while processes
 * are restarted and failures are injected.
 *
 * By default this test runs for 60 seconds, but this can be changed by passing a different value
 * in "itclient.runtime.seconds". For example:
 * "mvn test -Dtest=ITClient -Ditclient.runtime.seconds=120".
 */
public class ITClient extends BaseKuduTest {

  private static final Logger LOG = LoggerFactory.getLogger(ITClient.class);

  private static final String RUNTIME_PROPERTY_NAME = "itclient.runtime.seconds";
  private static final long DEFAULT_RUNTIME_SECONDS = 60;
  // Time we'll spend waiting at the end of the test for things to settle. Also the minimum this
  // test can run for.
  private static final long TEST_MIN_RUNTIME_SECONDS = 2;
  private static final long TEST_TIMEOUT_SECONDS = 600000;

  private static final String TABLE_NAME =
      ITClient.class.getName() + "-" + System.currentTimeMillis();
  // One error and we stop the test.
  private static final CountDownLatch KEEP_RUNNING_LATCH = new CountDownLatch(1);
  // Latch used to track if an error occurred and we need to stop the test early.
  private static final CountDownLatch ERROR_LATCH = new CountDownLatch(1);

  private static KuduClient localClient;
  private static AsyncKuduClient localAsyncClient;
  private static KuduTable table;
  private static long runtimeInSeconds;

  private volatile long sharedWriteTimestamp;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    String runtimeProp = System.getProperty(RUNTIME_PROPERTY_NAME);
    runtimeInSeconds = runtimeProp == null ? DEFAULT_RUNTIME_SECONDS : Long.parseLong(runtimeProp);

    if (runtimeInSeconds < TEST_MIN_RUNTIME_SECONDS || runtimeInSeconds > TEST_TIMEOUT_SECONDS) {
      Assert.fail("This test needs to run more than " + TEST_MIN_RUNTIME_SECONDS + " seconds" +
          " and less than " + TEST_TIMEOUT_SECONDS + " seconds");
    }

    LOG.info ("Test running for {} seconds", runtimeInSeconds);

    BaseKuduTest.setUpBeforeClass();

    // Client we're using has low tolerance for read timeouts but a
    // higher overall operation timeout.
    localAsyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
        .defaultSocketReadTimeoutMs(500)
        .build();
    localClient = new KuduClient(localAsyncClient);

    CreateTableOptions builder = new CreateTableOptions().setNumReplicas(3);
    builder.setRangePartitionColumns(ImmutableList.of("key"));
    table = localClient.createTable(TABLE_NAME, basicSchema, builder);
  }

  @Test(timeout = TEST_TIMEOUT_SECONDS)
  public void test() throws Exception {

    UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler();

    ArrayList<Thread> threads = new ArrayList<>();
    Thread chaosThread = new Thread(new ChaosThread());
    Thread writerThread = new Thread(new WriterThread());
    Thread scannerThread = new Thread(new ScannerThread());

    threads.add(chaosThread);
    threads.add(writerThread);
    threads.add(scannerThread);

    for (Thread thread : threads) {
      thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
      thread.start();
    }

    // await() returns yes if the latch reaches 0, we don't want that.
    Assert.assertFalse("Look for the last ERROR line in the log that comes from ITCLient",
        ERROR_LATCH.await(runtimeInSeconds, TimeUnit.SECONDS));

    // Indicate we want to stop, then wait a little bit for it to happen.
    KEEP_RUNNING_LATCH.countDown();

    for (Thread thread : threads) {
      // Give plenty of time for threads to stop.
      thread.join(DEFAULT_SLEEP);
    }

    AsyncKuduScanner scannerBuilder = localAsyncClient.newScannerBuilder(table).build();
    int rowCount = countRowsInScan(scannerBuilder);
    Assert.assertTrue(rowCount + " should be higher than 0", rowCount > 0);
  }

  /**
   * Logs an error message and triggers the error count down latch, stopping this test.
   * @param message error message to print
   * @param exception optional exception to print
   */
  private void reportError(String message, Exception exception) {
    LOG.error(message, exception);
    ERROR_LATCH.countDown();
  }

  /**
   * Thread that introduces chaos in the cluster, one at a time.
   */
  class ChaosThread implements Runnable {

    private final Random random = new Random();

    @Override
    public void run() {
      try {
        KEEP_RUNNING_LATCH.await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        return;
      }
      while (KEEP_RUNNING_LATCH.getCount() > 0) {
        try {
          boolean shouldContinue;
          int randomInt = random.nextInt(3);
          if (randomInt == 0) {
            shouldContinue = restartTS();
          } else if (randomInt == 1) {
            shouldContinue = disconnectNode();
          } else {
            shouldContinue = restartMaster();
          }

          if (!shouldContinue) {
            return;
          }
          KEEP_RUNNING_LATCH.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          return;
        }

      }
    }

    /**
     * Failure injection. Picks a random tablet server from the client's cache and force
     * disconects it.
     * @return true if successfully completed or didn't find a server to disconnect, false it it
     * encountered a failure
     */
    private boolean disconnectNode() {
      try {
        final List<Connection> connections = localAsyncClient.getConnectionListCopy();
        if (connections.isEmpty()) {
          return true;
        }
        connections.get(random.nextInt(connections.size())).disconnect();

      } catch (Exception e) {
        if (KEEP_RUNNING_LATCH.getCount() == 0) {
          // Likely shutdown() related.
          return false;
        }
        reportError("Couldn't disconnect a TS", e);
        return false;
      }
      return true;
    }

    /**
     * Forces the restart of a random tablet server.
     * @return true if it successfully completed, false if it failed
     */
    private boolean restartTS() {
      try {
        BaseKuduTest.restartTabletServer(table);
      } catch (Exception e) {
        reportError("Couldn't restart a TS", e);
        return false;
      }
      return true;
    }

    /**
     * Forces the restart of the master.
     * @return true if it successfully completed, false if it failed
     */
    private boolean restartMaster() {
      try {
        BaseKuduTest.restartLeaderMaster();
      } catch (Exception e) {
        reportError("Couldn't restart a master", e);
        return false;
      }
      return true;
    }

  }

  /**
   * Thread that writes sequentially to the table. Every 10 rows it considers setting the flush mode
   * to MANUAL_FLUSH or AUTO_FLUSH_SYNC.
   */
  class WriterThread implements Runnable {

    private final KuduSession session = localClient.newSession();
    private final Random random = new Random();
    private int currentRowKey = 0;

    @Override
    public void run() {
      session.setExternalConsistencyMode(ExternalConsistencyMode.CLIENT_PROPAGATED);
      while (KEEP_RUNNING_LATCH.getCount() > 0) {
        try {
          OperationResponse resp = session.apply(createBasicSchemaInsert(table, currentRowKey));
          if (hasRowErrorAndReport(resp)) {
            return;
          }
          currentRowKey++;

          // Every 10 rows we flush and change the flush mode randomly.
          if (currentRowKey % 10 == 0) {

            // First flush any accumulated rows before switching.
            List<OperationResponse> responses = session.flush();
            if (responses != null) {
              for (OperationResponse batchedResp : responses) {
                if (hasRowErrorAndReport(batchedResp)) {
                  return;
                }
              }
            }

            if (random.nextBoolean()) {
              session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            } else {
              session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            }
          }
        } catch (Exception e) {
          if (KEEP_RUNNING_LATCH.getCount() == 0) {
            // Likely shutdown() related.
            return;
          }
          reportError("Got error while inserting row " + currentRowKey, e);
          return;
        }
      }
    }

    private boolean hasRowErrorAndReport(OperationResponse resp) {
      if (resp != null && resp.hasRowError()) {
        reportError("The following RPC " + resp.getOperation().getRow() +
            " returned this error: " + resp.getRowError(), null);
        return true;
      }

      if (resp == null) {
        return false;
      }

      sharedWriteTimestamp = resp.getWriteTimestampRaw();

      return false;
    }
  }

  /**
   * Thread that scans the table. Alternates randomly between random gets and full table scans.
   */
  class ScannerThread implements Runnable {

    private final Random random = new Random();

    // Updated by calling a full scan.
    private int lastRowCount = 0;

    @Override
    public void run() {
      while (KEEP_RUNNING_LATCH.getCount() > 0) {

        boolean shouldContinue;

        // First check if we've written at least one row.
        if (sharedWriteTimestamp == 0) {
          shouldContinue = true;
        } else if (lastRowCount == 0 || // Need to full scan once before random reading
            random.nextBoolean()) {
          shouldContinue = fullScan();
        } else {
          shouldContinue = randomGet();
        }

        if (!shouldContinue) {
          return;
        }

        if (lastRowCount == 0) {
          try {
            KEEP_RUNNING_LATCH.await(50, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            // Test is stopping.
            return;
          }
        }
      }
    }

    /**
     * Reads a row at random that should exist (smaller than lastRowCount).
     * @return true if the get was successful, false if there was an error
     */
    private boolean randomGet() {
      int key = random.nextInt(lastRowCount);
      KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
          table.getSchema().getColumnByIndex(0), KuduPredicate.ComparisonOp.EQUAL, key);
      KuduScanner scanner = getScannerBuilder()
          .addPredicate(predicate)
          .build();

      List<RowResult> results = new ArrayList<>();
      while (scanner.hasMoreRows()) {
        try {
          RowResultIterator ite = scanner.nextRows();
          for (RowResult row : ite) {
            results.add(row);
          }
        } catch (KuduException e) {
          return checkAndReportError("Got error while getting row " + key, e);
        }
      }

      if (results.isEmpty() || results.size() > 1) {
        reportError("Random get got 0 or many rows " + results.size() + " for key " + key, null);
        return false;
      }

      int receivedKey = results.get(0).getInt(0);
      if (receivedKey != key) {
        reportError("Tried to get key " + key + " and received " + receivedKey, null);
        return false;
      }
      return true;
    }

    /**
     * Runs a full table scan and updates the lastRowCount.
     * @return true if the full scan was successful, false if there was an error
     */
    private boolean fullScan() {
      int rowCount;
      DeadlineTracker deadlineTracker = new DeadlineTracker();
      deadlineTracker.setDeadline(DEFAULT_SLEEP);

      while (KEEP_RUNNING_LATCH.getCount() > 0 && !deadlineTracker.timedOut()) {
        KuduScanner scanner = getScannerBuilder().build();

        try {
          rowCount = countRowsInScan(scanner);
        } catch (KuduException e) {
          return checkAndReportError("Got error while row counting", e);
        }

        if (rowCount >= lastRowCount) {
          if (rowCount > lastRowCount) {
            lastRowCount = rowCount;
            LOG.info("New row count {}", lastRowCount);
          }
          return true;
        } else {
          reportError("Row count unexpectedly decreased from " + lastRowCount + "to " + rowCount,
              null);
        }

        // Due to the lack of KUDU-430, we need to loop for a while.
        try {
          KEEP_RUNNING_LATCH.await(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // No need to do anything, we'll exit the loop once we test getCount() in the condition.
        }
      }
      return !deadlineTracker.timedOut();
    }

    private KuduScanner.KuduScannerBuilder getScannerBuilder() {
      return localClient.newScannerBuilder(table)
          .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
          .snapshotTimestampRaw(sharedWriteTimestamp)
          .setFaultTolerant(true);
    }

    /**
     * Checks the passed exception contains "Scanner not found". If it does then it returns true,
     * else it reports the error and returns false.
     * We need to do this because the scans in this client aren't fault tolerant.
     * @param message message to print if the exception contains a real error
     * @param e the exception to check
     * @return true if the scanner failed on a non-FATAL error, otherwise false which will kill
     *         this test
     */
    private boolean checkAndReportError(String message, KuduException e) {
      // It's possible to get timeouts if we're unlucky. A particularly common one is
      // "could not wait for desired snapshot timestamp to be consistent" since we're using
      // READ_AT_SNAPSHOT scanners.
      // TODO revisit once KUDU-1656 is taken care of.
      if (e.getStatus().isTimedOut()) {
        LOG.warn("Received a scan timeout", e);
        return true;
      }
      // Do nasty things, expect nasty results. The scanners are a bit too happy to retry TS
      // disconnections so we might end up retrying a scanner on a node that restarted, or we might
      // get disconnected just after sending an RPC so when we reconnect to the same TS we might get
      // the "Invalid call sequence ID" message.
      if (!e.getStatus().isNotFound() &&
          !e.getStatus().getMessage().contains("Invalid call sequence ID")) {
        reportError(message, e);
        return false;
      }
      return true;
    }
  }

  private class UncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    @Override
    public void uncaughtException(Thread t, Throwable e) {
      // Only report an error if we're still running, else we'll spam the log.
      if (KEEP_RUNNING_LATCH.getCount() != 0) {
        reportError("Uncaught exception", new Exception(e));
      }
    }
  }
}