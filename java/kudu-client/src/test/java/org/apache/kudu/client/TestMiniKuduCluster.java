/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package org.apache.kudu.client;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.Socket;

import org.junit.Test;

public class TestMiniKuduCluster {

  private static final int NUM_TABLET_SERVERS = 3;
  private static final int DEFAULT_NUM_MASTERS = 1;

  @Test(timeout = 50000)
  public void test() throws Exception {
    try (MiniKuduCluster cluster = new MiniKuduCluster.MiniKuduClusterBuilder()
                                                      .numMasters(DEFAULT_NUM_MASTERS)
                                                      .numTservers(NUM_TABLET_SERVERS)
                                                      .build()) {
      assertTrue(cluster.waitForTabletServers(NUM_TABLET_SERVERS));
      assertEquals(DEFAULT_NUM_MASTERS, cluster.getMasterProcesses().size());
      assertEquals(NUM_TABLET_SERVERS, cluster.getTabletServerProcesses().size());

      {
        // Kill the master.
        int masterPort = cluster.getMasterProcesses().keySet().iterator().next();
        testPort(masterPort, true, 1000);
        cluster.killMasterOnPort(masterPort);

        testPort(masterPort, false, 2000);

        // Restart the master.
        cluster.restartDeadMasterOnPort(masterPort);

        // Test we can reach it.
        testPort(masterPort, true, 3000);
      }

      {
        // Kill the first TS.
        int tsPort = cluster.getTabletServerProcesses().keySet().iterator().next();
        testPort(tsPort, true, 1000);
        cluster.killTabletServerOnPort(tsPort);

        testPort(tsPort, false, 2000);

        // Restart it.
        cluster.restartDeadTabletServerOnPort(tsPort);

        testPort(tsPort, true, 3000);
      }

      assertEquals(DEFAULT_NUM_MASTERS, cluster.getMasterProcesses().size());
      assertEquals(NUM_TABLET_SERVERS, cluster.getTabletServerProcesses().size());
    }
  }

  @Test(timeout = 50000)
  public void testKerberos() throws Exception {
    try (MiniKuduCluster cluster = new MiniKuduCluster.MiniKuduClusterBuilder()
                                                      .numMasters(DEFAULT_NUM_MASTERS)
                                                      .numTservers(NUM_TABLET_SERVERS)
                                                      .enableKerberos()
                                                      .build()) {
      assertTrue(cluster.waitForTabletServers(NUM_TABLET_SERVERS));
    }
  }

  /**
   * Test without the specified is open or closed, waiting up to a certain time.
   * The longer you expect it might for the socket to become open or closed.
   * @param port the port to test
   * @param testIsOpen true if we should want it to be open, false if we want it closed
   * @param timeout how long we're willing to wait before it happens
   */
  private static void testPort(int port,
                               boolean testIsOpen,
                               long timeout) throws InterruptedException {
    DeadlineTracker tracker = new DeadlineTracker();
    while (tracker.getElapsedMillis() < timeout) {
      try {
        Socket socket = new Socket(TestUtils.getUniqueLocalhost(), port);
        socket.close();
        if (testIsOpen) {
          return;
        }
      } catch (IOException e) {
        if (!testIsOpen) {
          return;
        }
      }
      Thread.sleep(200);
    }
    fail("Port " + port + " is still " + (testIsOpen ? "closed " : "open"));
  }
}
