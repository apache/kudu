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

import org.junit.Test;

/**
 * Integration test on non fault tolerant scanner that inserts enough data
 * to trigger flushes and getting multiple data blocks.
 */
public class ITNonFaultTolerantScanner extends ITScannerMultiTablet {

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
      rowCount += rri.getNumRows();
    }

    assertTrue(loopCount > TABLET_COUNT);
    assertEquals(ROW_COUNT, rowCount);
  }

  /**
   * Verifies for non fault tolerant scanner, it can proceed
   * properly even if shuts down client connection.
   */
  @Test(timeout = 100000)
  public void testNonFaultTolerantDisconnect() throws KuduException {
    clientFaultInjection(false);
  }

  /**
   * Tests non fault tolerant scanner by killing the tablet server while scanning and
   * verifies it throws {@link NonRecoverableException} as expected.
   */
  @Test(timeout = 100000, expected=NonRecoverableException.class)
  public void testNonFaultTolerantScannerKill() throws Exception {
    serverFaultInjection(false, false, false);
  }

  /**
   * Tests non fault tolerant scanner by restarting the tablet server while scanning and
   * verifies it throws {@link NonRecoverableException} as expected.
   */
  @Test(timeout = 100000, expected=NonRecoverableException.class)
  public void testNonFaultTolerantScannerRestart() throws Exception {
    serverFaultInjection(true, false, false);
  }
}
