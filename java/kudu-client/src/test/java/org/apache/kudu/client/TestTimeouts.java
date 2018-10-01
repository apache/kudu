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

import static org.apache.kudu.util.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.util.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.util.ClientTestUtil.getBasicSchema;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.Test;

public class TestTimeouts {

  private static final String TABLE_NAME =
      TestTimeouts.class.getName() + "-" + System.currentTimeMillis();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  /**
   * This test case tries different methods that should all timeout, while relying on the client to
   * pass down the timeouts to the session and scanner.
   * TODO(aserbin) this test is flaky; add delays on the server side to make it stable
   */
  @Test(timeout = 100000)
  public void testLowTimeouts() throws Exception {
    KuduClient lowTimeoutsClient =
        new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
        .defaultAdminOperationTimeoutMs(1)
        .defaultOperationTimeoutMs(1)
        .build();

    try {
      lowTimeoutsClient.listTabletServers();
      fail("Should have timed out");
    } catch (KuduException ex) {
      // Expected.
    }

    harness.getClient().createTable(TABLE_NAME, getBasicSchema(), getBasicCreateTableOptions());
    KuduTable table = lowTimeoutsClient.openTable(TABLE_NAME);

    KuduSession lowTimeoutSession = lowTimeoutsClient.newSession();

    OperationResponse response = lowTimeoutSession.apply(createBasicSchemaInsert(table, 1));
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isTimedOut());

    KuduScanner lowTimeoutScanner = lowTimeoutsClient.newScannerBuilder(table).build();
    try {
      lowTimeoutScanner.nextRows();
      fail("Should have timed out");
    } catch (KuduException ex) {
      assertTrue(ex.getStatus().isTimedOut());
    }
  }
}
