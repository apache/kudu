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

import static org.apache.kudu.client.AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.test.KuduTestHarness;

/**
 * KUDU-1260: Test Kudu scanner prefetching
 *
 */
public class TestKuduScannerPrefetching {
  private static final Logger LOG = LoggerFactory.getLogger(ITClient.class);

  private static final String RUNTIME_PROPERTY_NAME = "scannerwithprefetching.runtime.seconds";
  private static final long DEFAULT_RUNTIME_SECONDS = 60;

  // Time we'll spend waiting at the end of the test for things to settle. Also
  // the minimum this test can run for.
  private static final long TEST_MIN_RUNTIME_SECONDS = 2;

  private static final long TEST_TIMEOUT_SECONDS = 600000;

  private static final String TABLE_NAME =
      TestKuduScannerPrefetching.class.getName() + "-" + System.currentTimeMillis();

  // Tracks whether it's time for the test to end or not.
  private CountDownLatch keepRunningLatch;

  // If the test fails, will contain an exception that describes the failure.
  private Exception failureException;

  private KuduTable table;
  private long runtimeInSeconds;

  private volatile long sharedWriteTimestamp;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    // Set (or reset, in the event of a retry) test state.
    keepRunningLatch = new CountDownLatch(1);
    failureException = null;
    sharedWriteTimestamp = 0;

    // Extract and verify the test's running time.
    String runtimeProp = System.getProperty(RUNTIME_PROPERTY_NAME);
    runtimeInSeconds = runtimeProp == null ? DEFAULT_RUNTIME_SECONDS : Long.parseLong(runtimeProp);
    if (runtimeInSeconds < TEST_MIN_RUNTIME_SECONDS || runtimeInSeconds > TEST_TIMEOUT_SECONDS) {
      Assert.fail("This test needs to run more than " + TEST_MIN_RUNTIME_SECONDS + " seconds" +
              " and less than " + TEST_TIMEOUT_SECONDS + " seconds");
    }
    LOG.info("Test will run for {} seconds", runtimeInSeconds);

    // Create the test table.
    CreateTableOptions builder = new CreateTableOptions().setNumReplicas(3);
    builder.setRangePartitionColumns(ImmutableList.of("key"));
    table = harness.getClient().createTable(TABLE_NAME, getBasicSchema(), builder);
  }

  /**
   * Check the scan results for two scanners w/o prefetching
   *
   * @throws Exception
   */
  @Test(timeout = TEST_TIMEOUT_SECONDS)
  public void testWithPrefetching() throws Exception {
    List<Thread> threads = new ArrayList<>();
    TestKuduScannerPrefetching.WriterThread wt = new TestKuduScannerPrefetching.WriterThread();
    TestKuduScannerPrefetching.ScannerThread st = new TestKuduScannerPrefetching.ScannerThread();
    threads.add(new Thread(wt, "writer-test-thread"));
    threads.add(new Thread(st, "scanner-test-thread"));
    for (Thread thread : threads) {
      thread.setUncaughtExceptionHandler(new TestKuduScannerPrefetching.UncaughtExceptionHandler());
      thread.start();
    }

    // If we time out here, the test ran to completion and passed. Otherwise, a
    // count down was triggered from an error and the test failed.
    boolean failure = keepRunningLatch.await(runtimeInSeconds, TimeUnit.SECONDS);
    if (!failure) {
      // The test passed but the threads are still running; tell them to stop.
      keepRunningLatch.countDown();
    }

    for (Thread thread : threads) {
      // Give plenty of time for threads to stop.
      thread.join(DEFAULT_SLEEP);
    }

    if (failure) {
      throw failureException;
    }
    Assert.assertTrue(wt.currentRowKey + " should be higher than 0", wt.currentRowKey > 0);
    Assert.assertTrue(st.totalRowCount + " should be higher than 0", st.totalRowCount > 0);
  }

  /**
   * Logs an error message and triggers the count down latch, stopping this test.
   *
   * @param message   error message to print
   * @param exception optional exception to print
   */
  private void reportError(String message, Exception exception) {
    failureException = new Exception(message, exception);
    keepRunningLatch.countDown();
  }

  /**
   * Thread that writes sequentially to the table. Every 10 rows it considers setting the flush mode
   * to MANUAL_FLUSH or AUTO_FLUSH_SYNC.
   */
  class WriterThread implements Runnable {

    private final KuduSession session = harness.getClient().newSession();
    private int currentRowKey = 0;

    @Override
    public void run() {
      session.setExternalConsistencyMode(ExternalConsistencyMode.CLIENT_PROPAGATED);
      while (keepRunningLatch.getCount() > 0) {
        try {
          OperationResponse resp = session.apply(createBasicSchemaInsert(table, currentRowKey));
          if (hasRowErrorAndReport(resp)) {
            return;
          }
          currentRowKey++;
        } catch (Exception e) {
          if (keepRunningLatch.getCount() == 0) {
            // Likely shutdown() related.
            LOG.error("Error occurs: " + e.getMessage());
            return;
          }
          reportError("Got error while inserting row " + currentRowKey, e);
          return;
        }
      }
      LOG.info("Stop writing");
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
    // Updated by calling a full scan.
    private int totalRowCount = 0;

    @Override
    public void run() {
      while (keepRunningLatch.getCount() > 0) {
        boolean shouldContinue = true;
        if (sharedWriteTimestamp == 0) {
          shouldContinue = true;
        } else {
          shouldContinue = fullScan();
        }
        if (!shouldContinue) {
          return;
        }

        if (totalRowCount == 0) {
          try {
            keepRunningLatch.await(50, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            // Test is stopping.
            return;
          }
        }
      }
    }

    /**
     * Runs a full table scan and verify the results
     *
     * @return true if the full scan was successful, false if there was an error
     */
    private boolean fullScan() {
      int rowCount;
      int rowCount2;
      TimeoutTracker timeoutTracker = new TimeoutTracker();
      timeoutTracker.setTimeout(DEFAULT_SLEEP);

      while (keepRunningLatch.getCount() > 0 && !timeoutTracker.timedOut()) {
        long snapshot = sharedWriteTimestamp;
        KuduScanner scanner = getSnapshotScannerBuilder(snapshot).prefetching(true).build();
        KuduScanner scannerNoPrefetching =
            getSnapshotScannerBuilder(snapshot).prefetching(false).build();
        try {
          rowCount = countRowsInScan(scanner);
          rowCount2 = countRowsInScan(scannerNoPrefetching);
        } catch (KuduException e) {
          return checkAndReportError("Got error while row counting", e);
        }
        Assert.assertEquals(rowCount, rowCount2);
        totalRowCount += rowCount;
        // Due to the lack of KUDU-430, we need to loop for a while.
        try {
          keepRunningLatch.await(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // No need to do anything, we'll exit the loop once we test getCount() in the condition.
        }
      }
      return !timeoutTracker.timedOut();
    }

    private KuduScanner.KuduScannerBuilder getSnapshotScannerBuilder(long snapshot) {
      return harness.getClient().newScannerBuilder(table)
              .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
              .snapshotTimestampRaw(snapshot)
              .batchSizeBytes(128)
              .keepAlivePeriodMs(DEFAULT_KEEP_ALIVE_PERIOD_MS)
              .setFaultTolerant(false);
    }

    /**
     * Checks the passed exception contains "Scanner not found". If it does then it returns true,
     * else it reports the error and returns false.
     * We need to do this because the scans in this client aren't fault tolerant.
     *
     * @param message message to print if the exception contains a real error
     * @param e       the exception to check
     * @return true if the scanner failed on a non-FATAL error, otherwise false which will kill
     * this test
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
      if (keepRunningLatch.getCount() != 0) {
        reportError("Uncaught exception", new Exception(e));
      }
    }
  }
}
