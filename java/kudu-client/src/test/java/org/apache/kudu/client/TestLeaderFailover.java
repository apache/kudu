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
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.KuduTestHarness;

public class TestLeaderFailover {

  private static final String TABLE_NAME =
      TestLeaderFailover.class.getName() + "-" + System.currentTimeMillis();
  private static KuduTable table;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    harness.getClient().createTable(TABLE_NAME, getBasicSchema(), builder);
    table = harness.getClient().openTable(TABLE_NAME);
  }

  /**
   * This test writes 3 rows, kills the leader, then tries to write another 3 rows. Finally it
   * counts to make sure we have 6 of them.
   *
   * This test won't run if we didn't start the cluster.
   */
  @Test(timeout = 100000)
  public void testFailover() throws Exception {
    KuduSession session = harness.getClient().newSession();
    for (int i = 0; i < 3; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }

    // Make sure the rows are in there before messing things up.
    AsyncKuduScanner scanner = harness.getAsyncClient().newScannerBuilder(table).build();
    assertEquals(3, countRowsInScan(scanner));

    harness.killTabletLeader(table);

    for (int i = 3; i < 6; i++) {
      OperationResponse resp = session.apply(createBasicSchemaInsert(table, i));
      if (resp.hasRowError()) {
        fail("Encountered a row error " + resp.getRowError());
      }
    }

    scanner = harness.getAsyncClient().newScannerBuilder(table).build();
    assertEquals(6, countRowsInScan(scanner));
  }
}