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

import static junit.framework.TestCase.assertTrue;

import java.io.Closeable;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.cluster.FakeDNS;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

public class TestNegotiation {
  private static final MiniKuduClusterBuilder clusterBuilder =
      new MiniKuduClusterBuilder()
          .numMasterServers(1)
          .numTabletServers(0)
          .enableKerberos();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  /**
   * Test that a non-Kerberized client will use SASL PLAIN to connect to a
   * Kerberized server which doesn't require authentication. Regression test for
   * KUDU-2121.
   */
  @Test
  @MasterServerConfig(flags = {
      "--rpc-authentication=optional",
      "--rpc-trace-negotiation",
      "--user-acl=*" })
  public void testSaslPlainFallback() throws Exception {
    FakeDNS.getInstance().install();

    CapturingLogAppender cla = new CapturingLogAppender();
    harness.kdestroy();
    harness.resetClients();
    try (Closeable c = cla.attach()) {
      assertTrue(harness.getClient().getTablesList().getTablesList().isEmpty());
    }
    assertTrue(cla.getAppendedText(),
               cla.getAppendedText().contains("Client requested to use mechanism: PLAIN"));
  }
}