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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.kudu.Schema;

/**
 * Integration test that inserts enough data to trigger flushes and getting multiple data
 * blocks.
 */
public class ITScannerMultiTablet extends BaseKuduTest {

  private static final String TABLE_NAME =
      ITScannerMultiTablet.class.getName()+"-"+System.currentTimeMillis();
  private static final int ROW_COUNT = 20000;
  private static final int TABLET_COUNT = 3;

  private static Schema schema = getBasicSchema();
  private static KuduTable table;

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

  /**
   * Test for KUDU-1343 with a multi-batch multi-tablet scan.
   */
  @Test(timeout = 100000)
  public void testKudu1343() throws Exception {
    KuduScanner scanner = syncClient.newScannerBuilder(table)
        .batchSizeBytes(1) // Just a hint, won't actually be that small
        .build();

    int rowCount = 0;
    int loopCount = 0;
    while(scanner.hasMoreRows()) {
      loopCount++;
      RowResultIterator rri = scanner.nextRows();
      while (rri.hasNext()) {
        rri.next();
        rowCount++;
      }
    }

    assertTrue(loopCount > TABLET_COUNT);
    assertEquals(ROW_COUNT, rowCount);
  }

  /**
   * Makes sure we pass all the correct information down to the server by verifying we get rows in
   * order from 4 tablets. We detect those tablet boundaries when keys suddenly become smaller than
   * what was previously seen.
   */
  @Test(timeout = 100000)
  public void testSortResultsByPrimaryKey() throws Exception {
    KuduScanner scanner = syncClient.newScannerBuilder(table)
        .sortResultsByPrimaryKey()
        .setProjectedColumnIndexes(Lists.newArrayList(0))
        .build();

    int rowCount = 0;
    int previousRow = -1;
    int tableBoundariesCount = 0;
    while(scanner.hasMoreRows()) {
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
    assertEquals(ROW_COUNT, rowCount);
    assertEquals(TABLET_COUNT, tableBoundariesCount);
  }
}
