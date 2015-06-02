// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.fail;

import com.stumbleupon.async.TimeoutException;
import org.junit.Test;

public class TestTimeouts extends BaseKuduTest {

  private static final String TABLE_NAME =
      TestTimeouts.class.getName() + "-" + System.currentTimeMillis();

  /**
   * This test case tries different methods that should all timeout, while relying on the client to
   * pass down the timeouts to the session and scanner.
   */
  @Test(timeout = 100000)
  public void testLowTimeouts() throws Exception {
    KuduClient lowTimeoutsClient = new KuduClient.KuduClientBuilder(masterHostPorts)
        .defaultAdminOperationTimeoutMs(1)
        .defaultOperationTimeoutMs(1)
        .build();

    try {
      lowTimeoutsClient.listTabletServers();
      fail("Should have timed out");
    } catch (TimeoutException ex) {
      // Expected.
    }

    createTable(TABLE_NAME, basicSchema, new CreateTableBuilder());
    KuduTable table = openTable(TABLE_NAME);

    KuduSession lowTimeoutSession = lowTimeoutsClient.newSession();

    try {
      lowTimeoutSession.apply(createBasicSchemaInsert(table, 1));
      fail("Should have timed out");
    } catch (TimeoutException ex) {
      // Expected.
    }

    KuduScanner lowTimeoutScanner = lowTimeoutsClient.newScannerBuilder(table, basicSchema).build();
    try {
      lowTimeoutScanner.nextRows();
      fail("Should have timed out");
    } catch (TimeoutException ex) {
      // Expected.
    }
  }
}
