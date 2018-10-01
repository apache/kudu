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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.stumbleupon.async.Deferred;
import org.apache.kudu.junit.RetryRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.util.NetUtil;

public class TestConnectionCache {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test(timeout = 50000)
  public void test() throws Exception {
    MiniKuduCluster cluster = null;
    try {
      cluster = new MiniKuduCluster.MiniKuduClusterBuilder().numMasterServers(3).build();

      final AsyncKuduClient client =
          new AsyncKuduClient.AsyncKuduClientBuilder(cluster.getMasterAddressesAsString()).build();
      // Below we ping the masters directly using RpcProxy, so if they aren't ready to process
      // RPCs we'll get an error. Here by listing the tables we make sure this won't happen since
      // it won't return until a master leader is found.
      client.getTablesList().join();

      HostAndPort masterHostPort = cluster.getMasterServers().get(0);
      ServerInfo firstMaster = new ServerInfo("fake-uuid", masterHostPort,
          NetUtil.getInetAddress(masterHostPort.getHost()));

      // 3 masters in the cluster. Connections should have been cached since we forced
      // a cluster connection above.
      // No tservers have been connected to by the client since we haven't accessed
      // any data.
      assertEquals(3, client.getConnectionListCopy().size());
      assertFalse(allConnectionsTerminated(client));

      final RpcProxy proxy = client.newRpcProxy(firstMaster);

      // Disconnect from the server.
      proxy.getConnection().disconnect().awaitUninterruptibly();
      waitForConnectionToTerminate(proxy.getConnection());
      assertTrue(proxy.getConnection().isTerminated());

      // Make sure not all the connections in the connection cache are disconnected yet. Actually,
      // only the connection to server '0' should be disconnected.
      assertFalse(allConnectionsTerminated(client));

      // For a new RpcProxy instance, a new connection to the same destination is established.
      final RpcProxy newHelper = client.newRpcProxy(firstMaster);
      final Connection newConnection = newHelper.getConnection();
      assertNotNull(newConnection);
      assertNotSame(proxy.getConnection(), newConnection);

      // The client-->server connection should not be established at this point yet. Wait a little
      // before checking the state of the connection: this is to check for the status of the
      // underlying connection _after_ the negotiation is run, if a regression happens. The
      // negotiation on the underlying connection should be run upon submitting the very first
      // RPC via the proxy object, not upon creating RpcProxy instance (see KUDU-1878).
      Thread.sleep(500);
      assertFalse(newConnection.isReady());
      pingConnection(newHelper);
      assertTrue(newConnection.isReady());

      // Test disconnecting and make sure we cleaned up all the connections.
      for (Connection c : client.getConnectionListCopy()) {
        c.disconnect().awaitUninterruptibly();
        waitForConnectionToTerminate(c);
      }
      assertTrue(allConnectionsTerminated(client));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private boolean allConnectionsTerminated(AsyncKuduClient client) {
    for (Connection c : client.getConnectionListCopy()) {
      if (!c.isTerminated()) {
        return false;
      }
    }
    return true;
  }

  private void waitForConnectionToTerminate(Connection c) throws InterruptedException {
    DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(5000);
    while (!c.isTerminated() && !deadlineTracker.timedOut()) {
      Thread.sleep(250);
    }
  }

  private void pingConnection(RpcProxy proxy) throws Exception {
    PingRequest ping = PingRequest.makeMasterPingRequest();
    Deferred<PingResponse> d = ping.getDeferred();
    proxy.sendRpc(ping);
    d.join();
  }
}
