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

import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;

/**
 * Integration test that inserts enough data to trigger flushes and getting multiple data
 * blocks.
 */
public class ITScannerMultiTablet {

  private static final Logger LOG = LoggerFactory.getLogger(ITScannerMultiTablet.class);
  private static final String TABLE_NAME =
      ITScannerMultiTablet.class.getName() + "-" + System.currentTimeMillis();
  protected static final int ROW_COUNT = 20000;
  protected static final int TABLET_COUNT = 3;

  private static Schema schema = getBasicSchema();
  protected KuduTable table;

  private static Random random = new Random(1234);

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    CreateTableOptions builder = new CreateTableOptions();

    builder.addHashPartitions(
        Lists.newArrayList(schema.getColumnByIndex(0).getName()),
        TABLET_COUNT);

    table = harness.getClient().createTable(TABLE_NAME, schema, builder);
    KuduSession session = harness.getClient().newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    Set<Integer> primaryKeys = new HashSet<Integer>();
    // Getting meaty rows.
    char[] chars = new char[1024];
    for (int i = 0; i < ROW_COUNT; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      int id = random.nextInt();
      while (id == Integer.MIN_VALUE || primaryKeys.contains(id)) {
        id = random.nextInt();
      }
      row.addInt(0, id);
      primaryKeys.add(id);
      row.addInt(1, i);
      row.addInt(2, i);
      row.addString(3, new String(chars));
      row.addBoolean(4, true);
      session.apply(insert);
    }
    session.flush();
    session.close();
    // Log on error details.
    if (session.countPendingErrors() > 0) {
      LOG.info("RowErrorsAndOverflowStatus: {}", session.getPendingErrors().toString());
    }
    assertEquals(0, session.countPendingErrors());
  }

  /**
   * Injecting failures (kill or restart TabletServer) while scanning, to verify:
   * fault tolerant scanner will continue scan and non-fault tolerant scanner will throw
   * {@link NonRecoverableException}.
   *
   * Also makes sure we pass all the correct information down to the server by verifying
   * we get rows in order from 3 tablets. We detect those tablet boundaries when keys suddenly
   * become smaller than what was previously seen.
   *
   * @param restart if true restarts TabletServer, otherwise kills TabletServer
   * @param isFaultTolerant if true uses fault tolerant scanner, otherwise
   *                        uses non fault-tolerant one
   * @param finishFirstScan if true injects failure before finishing first tablet scan,
   *                        otherwise in the middle of tablet scanning
   * @throws Exception
   */
  void serverFaultInjection(boolean restart, boolean isFaultTolerant,
      boolean finishFirstScan) throws Exception {
    KuduScanner scanner = harness.getClient().newScannerBuilder(table)
        .setFaultTolerant(isFaultTolerant)
        .batchSizeBytes(1)
        .setProjectedColumnIndexes(Lists.newArrayList(0)).build();

    try {
      int rowCount = 0;
      int previousRow = -1;
      int tableBoundariesCount = 0;
      if (scanner.hasMoreRows()) {
        RowResultIterator rri = scanner.nextRows();
        while (rri.hasNext()) {
          int key = rri.next().getInt(0);
          if (key < previousRow) {
            tableBoundariesCount++;
          }
          previousRow = key;
          rowCount++;
        }
      }

      if (!finishFirstScan) {
        if (restart) {
          harness.restartTabletServer(scanner.currentTablet());
        } else {
          harness.killTabletLeader(scanner.currentTablet());
        }
      }

      boolean failureInjected = false;
      while (scanner.hasMoreRows()) {
        RowResultIterator rri = scanner.nextRows();
        while (rri.hasNext()) {
          int key = rri.next().getInt(0);
          if (key < previousRow) {
            tableBoundariesCount++;
            if (finishFirstScan && !failureInjected) {
              if (restart) {
                harness.restartTabletServer(scanner.currentTablet());
              } else {
                harness.killTabletLeader(scanner.currentTablet());
              }
              failureInjected = true;
            }
          }
          previousRow = key;
          rowCount++;
        }
      }

      assertEquals(ROW_COUNT, rowCount);
      assertEquals(TABLET_COUNT, tableBoundariesCount);
    } finally {
      scanner.close();
    }
  }

  /**
   * Inject failures (kill or restart TabletServer) while scanning,
   * Inject failure (restart TabletServer) to a tablet's leader while
   * scanning after second scan request, to verify:
   * a fault tolerant scanner will continue scanning and a non-fault tolerant scanner will throw
   * {@link NonRecoverableException}.
   *
   * @throws Exception
   */
  void serverFaultInjectionRestartAfterSecondScanRequest() throws Exception {
    // In fact, the test has TABLET_COUNT, default is 3.
    // We check the rows' order, no dup rows and loss rows.
    // And In the case, we need 2 times or more scan requests,
    // so set a minimum batchSizeBytes 1.
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = harness.getClient().newScanTokenBuilder(table)
        .batchSizeBytes(1)
        .setFaultTolerant(true)
        .setProjectedColumnIndexes(Lists.newArrayList(0));

    List<KuduScanToken> tokens = tokenBuilder.build();
    assertTrue(tokens.size() == TABLET_COUNT);

    class TabletScannerTask implements Callable<Integer> {
      private KuduScanToken token;
      private boolean enableFaultInjection;

      public TabletScannerTask(KuduScanToken token, boolean enableFaultInjection) {
        this.token = token;
        this.enableFaultInjection = enableFaultInjection;
      }

      @Override
      public Integer call() {
        int rowCount = 0;
        KuduScanner scanner;
        try {
          scanner = this.token.intoScanner(harness.getClient());
        } catch (IOException e) {
          LOG.error("Generate KuduScanner error, {}", e.getMessage());
          e.printStackTrace();
          return -1;
        }
        try {
          int previousRow = Integer.MIN_VALUE;
          boolean faultInjected = !this.enableFaultInjection;
          int faultInjectionLowBound = (ROW_COUNT / TABLET_COUNT / 2);
          while (scanner.hasMoreRows()) {
            RowResultIterator rri = scanner.nextRows();
            while (rri.hasNext()) {
              int key = rri.next().getInt(0);
              if (previousRow >= key) {
                LOG.error("Impossible results, previousKey: {} >= currentKey: {}",
                          previousRow, key);
                return -1;
              }
              if (!faultInjected && rowCount > faultInjectionLowBound) {
                harness.restartTabletServer(scanner.currentTablet());
                faultInjected = true;
              }
              previousRow = key;
              rowCount++;
            }
          }
        } catch (Exception e) {
          LOG.error("Scan error, {}", e.getMessage());
          e.printStackTrace();
        } finally {
          try {
            scanner.close();
          } catch (KuduException e) {
            LOG.warn(e.getMessage());
            e.printStackTrace();
          }
        }
        return rowCount;
      }
    }

    int rowCount = 0;
    ExecutorService threadPool = Executors.newFixedThreadPool(TABLET_COUNT);
    List<TabletScannerTask> tabletScannerTasks = new ArrayList<>();
    tabletScannerTasks.add(new TabletScannerTask(tokens.get(0), true));
    for (int i = 1; i < tokens.size(); i++) {
      tabletScannerTasks.add(new TabletScannerTask(tokens.get(i), false));
    }
    List<Future<Integer>> results = threadPool.invokeAll(tabletScannerTasks);
    threadPool.shutdown();
    assertTrue(threadPool.awaitTermination(100, TimeUnit.SECONDS));
    for (Future<Integer> result : results) {
      try {
        rowCount += result.get();
      } catch (Exception e) {
        LOG.info(e.getMessage());
        assertTrue(false);
      }
    }
    assertEquals(ROW_COUNT, rowCount);
  }

  /**
   * Injecting failures (i.e. drop client connection) while scanning, to verify:
   * both non-fault tolerant scanner and fault tolerant scanner will continue scan as expected.
   *
   * @param isFaultTolerant if true use fault-tolerant scanner, otherwise use non-fault-tolerant one
   * @throws Exception
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  void clientFaultInjection(boolean isFaultTolerant) throws KuduException {
    KuduScanner scanner = harness.getClient().newScannerBuilder(table)
        .setFaultTolerant(isFaultTolerant)
        .batchSizeBytes(1)
        .build();

    try {
      int rowCount = 0;
      int loopCount = 0;
      if (scanner.hasMoreRows()) {
        loopCount++;
        RowResultIterator rri = scanner.nextRows();
        rowCount += rri.getNumRows();
      }

      // Forcefully disconnects the current connection and fails all outstanding RPCs
      // in the middle of scanning.
      harness.getAsyncClient().newRpcProxy(scanner.currentTablet().getReplicaSelectedServerInfo(
          scanner.getReplicaSelection(), /* location= */"")).getConnection().disconnect();

      while (scanner.hasMoreRows()) {
        loopCount++;
        RowResultIterator rri = scanner.nextRows();
        rowCount += rri.getNumRows();
      }

      assertTrue(loopCount > TABLET_COUNT);
      assertEquals(ROW_COUNT, rowCount);
    } finally {
      scanner.close();
    }
  }
}
