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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;

import org.junit.Test;

import org.apache.kudu.util.CapturingLogAppender;

public class ITClientStress extends BaseKuduTest {

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
    try (Closeable c = cla.attach()){
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
      assertNull(thrown.get());
    }
    assertFalse("log contained NPE",
        cla.getAppendedText().contains("NullPointerException"));
    assertFalse("log contained SSLException",
        cla.getAppendedText().contains("SSLException"));
  }

  /**
   * Regression test for KUDU-1963. This simulates the behavior of the
   * Impala 2.8 front-end under a high-concurrency workload. Each query
   * starts a new client, fetches scan tokens, and closes the client.
   */
  @Test(timeout=60000)
  public void testManyShortClientsGeneratingScanTokens() throws Exception {
    final String TABLE_NAME = "testManyClients";
    final int SECONDS_TO_RUN = 10;
    final int NUM_THREADS = 80;
    createFourTabletsTableWithNineRows(TABLE_NAME);

    runTasks(NUM_THREADS, SECONDS_TO_RUN, new Supplier<Callable<Void>>() {
      @Override
      public Callable<Void> get() {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            try (AsyncKuduClient client =
                  new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
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
}
