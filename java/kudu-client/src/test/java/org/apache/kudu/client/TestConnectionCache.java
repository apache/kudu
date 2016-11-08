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
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;
import org.junit.Test;

public class TestConnectionCache {

  @Test(timeout = 50000)
  public void test() throws Exception {
    try (MiniKuduCluster cluster =
             new MiniKuduCluster.MiniKuduClusterBuilder().numMasters(3).build()) {

      AsyncKuduClient client =
          new AsyncKuduClient.AsyncKuduClientBuilder(cluster.getMasterAddresses()).build();

      List<HostAndPort> addresses = cluster.getMasterHostPorts();

      ConnectionCache cache = new ConnectionCache(client);
      int i = 0;
      for (HostAndPort hp : addresses) {
        TabletClient conn =
            cache.newClient(new ServerInfo(i + "", hp.getHostText(), hp.getPort(), false));
        // Ping the process so we go through the whole connection process.
        pingConnection(conn);
        i++;
      }
      assertEquals(3, cache.getImmutableTabletClientsList().size());
      assertFalse(cache.allConnectionsAreDead());

      TabletClient conn = cache.getClient("0");

      // Kill the connection.
      conn.shutdown().join();
      waitForConnectionToDie(conn);
      assertFalse(conn.isAlive());

      // Make sure the cache also knows it's dead, but that not all the connections are.
      assertFalse(cache.getClient("0").isAlive());
      assertFalse(cache.allConnectionsAreDead());

      // Test reconnecting with only the UUID.
      TabletClient newConn = cache.getLiveClient("0");
      assertFalse(conn == newConn);
      pingConnection(newConn);

      // Test disconnecting and make sure we cleaned up all the connections.
      cache.disconnectEverything().join();
      waitForConnectionToDie(cache.getClient("0"));
      waitForConnectionToDie(cache.getClient("1"));
      waitForConnectionToDie(cache.getClient("2"));
      assertTrue(cache.allConnectionsAreDead());
    }
  }

  private void waitForConnectionToDie(TabletClient conn) throws InterruptedException {
    DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(5000);
    while (conn.isAlive() && !deadlineTracker.timedOut()) {
      Thread.sleep(250);
    }
  }

  private void pingConnection(TabletClient conn) throws Exception {
    PingRequest ping = PingRequest.makeMasterPingRequest();
    Deferred<PingResponse> d = ping.getDeferred();
    conn.sendRpc(ping);
    d.join();
  }
}
