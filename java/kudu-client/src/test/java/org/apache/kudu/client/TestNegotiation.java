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

import org.apache.kudu.util.CapturingLogAppender;
import org.junit.Test;

import java.io.Closeable;

import static junit.framework.TestCase.assertTrue;

public class TestNegotiation {

  /**
   * Test that a non-Kerberized client will use SASL PLAIN to connect to a
   * Kerberized server which doesn't require authentication. Regression test for
   * KUDU-2121.
   */
  @Test
  public void testSaslPlainFallback() throws Exception {
    FakeDNS.getInstance().install();
    MiniKuduCluster.MiniKuduClusterBuilder clusterBuilder =
        new MiniKuduCluster.MiniKuduClusterBuilder();

    clusterBuilder.numMasters(1)
                  .numTservers(0)
                  .enableKerberos()
                  .addMasterFlag("--rpc-authentication=optional")
                  .addMasterFlag("--rpc-trace-negotiation")
                  .addMasterFlag("--user-acl=*");

    CapturingLogAppender cla = new CapturingLogAppender();
    try (MiniKuduCluster cluster = clusterBuilder.build()) {
      cluster.kdestroy();
      try (Closeable c = cla.attach();
           KuduClient client = new KuduClient.KuduClientBuilder(cluster.getMasterAddressesAsString())
                                             .build()
      ) {
        assertTrue(client.getTablesList().getTablesList().isEmpty());
      }
    }

    assertTrue(cla.getAppendedText(),
               cla.getAppendedText().contains("Client requested to use mechanism: PLAIN"));
  }
}