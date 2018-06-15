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

import static org.apache.kudu.util.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.BeforeClass;

import org.apache.kudu.Schema;

/**
 * Integration test that inserts enough data to trigger flushes and getting multiple data
 * blocks.
 */
public class ITScannerMultiTablet extends BaseKuduTest {

  private static final String TABLE_NAME =
      ITScannerMultiTablet.class.getName()+"-"+System.currentTimeMillis();
  protected static final int ROW_COUNT = 20000;
  protected static final int TABLET_COUNT = 3;

  private static Schema schema = getBasicSchema();
  protected static KuduTable table;

  private static Random random = new Random(1234);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();

    CreateTableOptions builder = new CreateTableOptions();

    builder.addHashPartitions(
        Lists.newArrayList(schema.getColumnByIndex(0).getName()),
        TABLET_COUNT);

    table = createTable(TABLE_NAME, schema, builder);

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    // Getting meaty rows.
    char[] chars = new char[1024];
    for (int i = 0; i < ROW_COUNT; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt(0, random.nextInt());
      row.addInt(1, i);
      row.addInt(2, i);
      row.addString(3, new String(chars));
      row.addBoolean(4, true);
      session.apply(insert);
    }
    session.flush();
    assertEquals(0, session.countPendingErrors());
  }

  @After
  public void tearDown() throws Exception {
    restartTabletServers();
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
    KuduScanner scanner = syncClient.newScannerBuilder(table)
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
          restartTabletServer(scanner.currentTablet());
        } else {
          killTabletLeader(scanner.currentTablet());
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
                restartTabletServer(scanner.currentTablet());
              } else {
                killTabletLeader(scanner.currentTablet());
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
   * Injecting failures (i.e. drop client connection) while scanning, to verify:
   * both non-fault tolerant scanner and fault tolerant scanner will continue scan as expected.
   *
   * @param isFaultTolerant if true use fault-tolerant scanner, otherwise use non-fault-tolerant one
   * @throws Exception
   */
  void clientFaultInjection(boolean isFaultTolerant) throws KuduException {
    KuduScanner scanner = syncClient.newScannerBuilder(table)
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
      client.newRpcProxy(scanner.currentTablet().getReplicaSelectedServerInfo(
          scanner.getReplicaSelection())).getConnection().disconnect();

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
