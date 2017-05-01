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

import org.junit.Test;

/**
 * Integration test on fault tolerant scanner that inserts enough data
 * to trigger flushes and getting multiple data blocks.
 */
public class ITFaultTolerantScanner extends ITScannerMultiTablet {
  /**
   * Verifies for fault tolerant scanner, it can proceed
   * properly even if shuts down client connection.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantShutDown() throws KuduException {
    clientFaultInjection(true, true);
  }

  /**
   * Verifies for fault tolerant scanner, it can proceed
   * properly even if disconnects client connection.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantDisconnect() throws KuduException {
    clientFaultInjection(false, true);
  }

  /**
   * Tests fault tolerant scanner by restarting the tablet server in the middle
   * of tablet scanning and verifies the scan results are as expected.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantScannerRestart() throws Exception {
    serverFaultInjection(true, true, false);
  }

  /**
   * Tests fault tolerant scanner by killing the tablet server in the middle
   * of tablet scanning and verifies the scan results are as expected.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantScannerKill() throws Exception {
    serverFaultInjection(false, true, false);
  }

  /**
   * Tests fault tolerant scanner by killing the tablet server while scanning
   * (after finish scan of first tablet) and verifies the scan results are as expected.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantScannerKillFinishFirstTablet() throws Exception {
    serverFaultInjection(false, true, true);
  }

  /**
   * Tests fault tolerant scanner by restarting the tablet server while scanning
   * (after finish scan of first tablet) and verifies the scan results are as expected.
   */
  @Test(timeout = 100000)
  public void testFaultTolerantScannerRestartFinishFirstTablet() throws Exception {
    serverFaultInjection(true, true, true);
  }
}
