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

import static org.apache.kudu.test.ClientTestUtil.createFourTabletsTableWithNineRows;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;

public class ITClientStress {
  private static final Logger LOG = LoggerFactory.getLogger(ITClientStress.class);

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @SuppressWarnings("FutureReturnValueIgnored")
  private void runTasks(int numThreads, int secondsToRun,
      Supplier<Callable<Void>> taskFactory) throws InterruptedException, IOException {
    // Capture any exception thrown by the tasks.
    final AtomicReference<Throwable> thrown = new AtomicReference<>(null);

    // Setup a pool with synchronous handoff.
    SynchronousQueue<Runnable> queue = new SynchronousQueue<>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(
        numThreads, numThreads, 100, TimeUnit.MILLISECONDS,
        queue, new ThreadPoolExecutor.CallerRunsPolicy());

    // Capture logs so we can check that no exceptions are logged.
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      Stopwatch s = Stopwatch.createStarted();
      while (s.elapsed(TimeUnit.SECONDS) < secondsToRun &&
          thrown.get() == null) {
        final Callable<Void> task = taskFactory.get();
        // Wrap the task so that if it throws an exception, we stop and
        // fail the test.
        Runnable wrapped = new Runnable() {
          @Override
          public void run() {
            try {
              task.call();
            } catch (Throwable t) {
              thrown.set(t);
            }
          }
        };
        pool.submit(wrapped);
      }
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    }
    if (thrown.get() != null) {
      throw new AssertionError(thrown.get());
    }
    assertFalse("log contained NPE",
        cla.getAppendedText().contains("NullPointerException"));
    assertFalse("log contained SSLException",
        cla.getAppendedText().contains("SSLException"));
    assertFalse("log contained IllegalStateException",
        cla.getAppendedText().contains("IllegalStateException"));
  }

  /**
   * Regression test for KUDU-1963. This simulates the behavior of the
   * Impala 2.8 front-end under a high-concurrency workload. Each query
   * starts a new client, fetches scan tokens, and closes the client.
   */
  @Test(timeout = 300000)
  public void testManyShortClientsGeneratingScanTokens() throws Exception {
    final String TABLE_NAME = "testManyClients";
    final int SECONDS_TO_RUN = 10;
    final int NUM_THREADS = 80;
    createFourTabletsTableWithNineRows(harness.getAsyncClient(), TABLE_NAME, DEFAULT_SLEEP);

    runTasks(NUM_THREADS, SECONDS_TO_RUN, new Supplier<Callable<Void>>() {
      @Override
      public Callable<Void> get() {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            try (AsyncKuduClient client =
                  new AsyncKuduClient.AsyncKuduClientBuilder(harness.getMasterAddressesAsString())
                  .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
                  .build()) {
              KuduTable t = client.openTable(TABLE_NAME).join();
              new KuduScanToken.KuduScanTokenBuilder(client, t).build();
            }
            return null;
          }
        };
      }
    });
  }

  /**
   * Stress test which performs upserts from many sessions on different threads
   * sharing the same KuduClient and KuduTable instance.
   */
  @Test(timeout = 300000)
  public void testMultipleSessions() throws Exception {
    final String TABLE_NAME = "testMultipleSessions";
    final int SECONDS_TO_RUN = 10;
    final int NUM_THREADS = 60;
    final KuduTable table = harness.getClient().createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    final AtomicInteger numUpserted = new AtomicInteger(0);
    runTasks(NUM_THREADS, SECONDS_TO_RUN, new Supplier<Callable<Void>>() {
      @Override
      public Callable<Void> get() {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            KuduSession s = harness.getClient().newSession();
            s.setFlushMode(FlushMode.AUTO_FLUSH_SYNC);
            try {
              for (int i = 0; i < 100; i++) {
                Upsert u = table.newUpsert();
                u.getRow().addInt(0, i);
                u.getRow().addInt(1, 12345);
                u.getRow().addInt(2, 3);
                u.getRow().setNull(3);
                u.getRow().addBoolean(4, false);
                OperationResponse apply = s.apply(u);
                if (apply.hasRowError()) {
                  throw new AssertionError(apply.getRowError().toString());
                }
                numUpserted.incrementAndGet();
              }
            } finally {
              s.close();
            }
            return null;
          }
        };
      }
    });
    LOG.info("Upserted {} rows", numUpserted.get());
  }
}
