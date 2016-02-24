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
package org.kududb.client;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kududb.Schema;

import static org.junit.Assert.*;

/**
 * Integration test that inserts enough data to trigger flushes and getting multiple data
 * blocks.
 */
public class ITScannerMultiTablet extends BaseKuduTest {

  private static final String TABLE_NAME =
      ITScannerMultiTablet.class.getName()+"-"+System.currentTimeMillis();
  private static final int ROW_COUNT = 3000;
  private static final int TABLET_COUNT = 3;

  private static Schema schema = getBasicSchema();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();

    CreateTableOptions builder = new CreateTableOptions();

    int step = ROW_COUNT / TABLET_COUNT;
    for (int i = step; i < ROW_COUNT; i += step){
      PartialRow splitRow = getBasicSchema().newPartialRow();
      splitRow.addInt(0, i);
      builder.addSplitRow(splitRow);
    }

    table = createTable(TABLE_NAME, schema, builder);

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    // Getting meaty rows.
    char[] chars = new char[1024];
    for (int i = 0; i < ROW_COUNT; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt(0, i);
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
  public void test() throws Exception {
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
}
