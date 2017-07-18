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

import static org.apache.kudu.util.AssertHelpers.assertEventuallyTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.kudu.util.AssertHelpers.BooleanExpression;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMultipleLeaderFailover extends BaseKuduTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  private void waitUntilRowCount(final KuduTable table, final int rowCount, long timeoutMs)
      throws Exception {
    assertEventuallyTrue(String.format("Read count should be %s", rowCount),
        new BooleanExpression() {
          @Override
          public boolean get() throws Exception {
            AsyncKuduScanner scanner = client.newScannerBuilder(table).build();
            int read_count = countRowsInScan(scanner);
            return read_count == rowCount;
          }
        }, timeoutMs);
  }

  /**
   * This test writes 3 rows. Then in a loop, it kills the leader, then tries to write inner_row
   * rows, and finally restarts the tablet server it killed. Verifying with a read as it goes.
   * Finally it counts to make sure we have total_rows_to_insert of them.
   */
  @Test(timeout = 100000)
  public void testMultipleFailover() throws Exception {
    KuduTable table;
    CreateTableOptions builder = getBasicCreateTableOptions();
    String tableName =
        TestMultipleLeaderFailover.class.getName() + "-" + System.currentTimeMillis();
    createTable(tableName, basicSchema, builder);

    table = openTable(tableName);
    KuduSession session = syncClient.newSession();
    final int ROWS_PER_ITERATION = 3;
    final int NUM_ITERATIONS = 10;
    final int TOTAL_ROWS_TO_INSERT = ROWS_PER_ITERATION + NUM_ITERATIONS * ROWS_PER_ITERATION;

    for (int i = 0; i < ROWS_PER_ITERATION; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    waitUntilRowCount(table, ROWS_PER_ITERATION, DEFAULT_SLEEP);

    int currentRows = ROWS_PER_ITERATION;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
      List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
      assertEquals(1, tablets.size());
      int leaderPort = findLeaderTabletServerPort(tablets.get(0));
      miniCluster.killTabletServerOnPort(leaderPort);

      for (int j = 0; j < ROWS_PER_ITERATION; j++) {
        OperationResponse resp = session.apply(createBasicSchemaInsert(table, currentRows));
        if (resp.hasRowError()) {
          fail("Encountered a row error " + resp.getRowError());
        }
        currentRows++;
      }

      miniCluster.restartDeadTabletServerOnPort(leaderPort);
      // Read your writes hasn't been enabled, so we need to use a helper function to poll.
      waitUntilRowCount(table, currentRows, DEFAULT_SLEEP);

    }
    waitUntilRowCount(table, TOTAL_ROWS_TO_INSERT, DEFAULT_SLEEP);
  }
}