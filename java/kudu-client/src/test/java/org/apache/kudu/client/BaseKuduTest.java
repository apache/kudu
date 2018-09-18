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

import static org.apache.kudu.util.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.util.ClientTestUtil.getSchemaWithAllTypes;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Deferred;
import org.apache.kudu.junit.RetryRule;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Common.HostPortPB;
import org.apache.kudu.Schema;
import org.apache.kudu.client.LocatedTablet.Replica;
import org.apache.kudu.master.Master;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BaseKuduTest {

  protected static final Logger LOG = LoggerFactory.getLogger(BaseKuduTest.class);

  // Default timeout/sleep interval for various client operations, waiting for various jobs/threads
  // to complete, etc.
  protected static final int DEFAULT_SLEEP = 50000;

  private final Random randomForTSRestart = TestUtils.getRandom();

  private static final int NUM_MASTERS = 3;
  private static final int NUM_TABLET_SERVERS = 3;

  protected MiniKuduCluster miniCluster;

  // Comma separate describing the master addresses and ports.
  protected String masterAddresses;
  protected List<HostAndPort> masterHostPorts;

  // We create both versions of the client for ease of use.
  protected AsyncKuduClient client;
  protected KuduClient syncClient;
  protected static final Schema basicSchema = getBasicSchema();
  protected static final Schema allTypesSchema = getSchemaWithAllTypes();

  // Add a rule to rerun tests. We use this with Gradle because it doesn't support
  // Surefire/Failsafe rerunFailingTestsCount like Maven does.
  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void setUpBase() throws Exception {
    FakeDNS.getInstance().install();

    LOG.info("Creating a new MiniKuduCluster...");

    miniCluster = getMiniClusterBuilder().build();
    masterAddresses = miniCluster.getMasterAddresses();
    masterHostPorts = miniCluster.getMasterHostPorts();

    LOG.info("Creating a new Kudu client...");
    client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
        .build();
    syncClient = client.syncClient();
  }

  @After
  public void tearDownBase() throws Exception {
    try {
      if (client != null) {
        syncClient.shutdown();
        // No need to explicitly shutdown the async client,
        // shutting down the async client effectively does that.
      }
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  /**
   * Returns a MiniKuduClusterBuilder to use when starting the MiniKuduCluster.
   * Override this method to adjust to the MiniKuduClusterBuilder settings.
   */
  protected MiniKuduCluster.MiniKuduClusterBuilder getMiniClusterBuilder() {
    return new MiniKuduCluster.MiniKuduClusterBuilder()
        .numMasters(NUM_MASTERS)
        .numTservers(NUM_TABLET_SERVERS);
  }

  protected KuduTable createTable(String tableName, Schema schema,
                                         CreateTableOptions builder) throws KuduException {
    LOG.info("Creating table: {}", tableName);
    return client.syncClient().createTable(tableName, schema, builder);
  }

  /**
   * Helper method to open a table. It sets the default sleep time when joining on the Deferred.
   * @param name Name of the table
   * @return A KuduTable
   * @throws Exception MasterErrorException if the table doesn't exist
   */
  protected KuduTable openTable(String name) throws Exception {
    Deferred<KuduTable> d = client.openTable(name);
    return d.join(DEFAULT_SLEEP);
  }

  /**
   * Helper method to easily kill a tablet server that serves the given table's only tablet's
   * leader. The currently running test case will be failed if there's more than one tablet,
   * if the tablet has no leader after some retries, or if the tablet server was already killed.
   *
   * This method is thread-safe.
   * @param table a KuduTable which will get its single tablet's leader killed.
   * @throws Exception
   */
  protected void killTabletLeader(KuduTable table) throws Exception {
    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    if (tablets.isEmpty() || tablets.size() > 1) {
      fail("Currently only support killing leaders for tables containing 1 tablet, table " +
      table.getName() + " has " + tablets.size());
    }
    LocatedTablet tablet = tablets.get(0);
    if (tablet.getReplicas().size() == 1) {
      fail("Table " + table.getName() + " only has 1 tablet, please enable replication");
    }

    HostAndPort hp = findLeaderTabletServerHostPort(tablet);
    miniCluster.killTabletServerOnHostPort(hp);
  }

  /**
   * Helper method to kill a tablet server that serves the given tablet's
   * leader. The currently running test case will be failed if the tablet has no
   * leader after some retries, or if the tablet server was already killed.
   *
   * This method is thread-safe.
   * @param tablet a RemoteTablet which will get its leader killed
   * @throws Exception
   */
  protected void killTabletLeader(RemoteTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServerHostPort(new LocatedTablet(tablet));
    miniCluster.killTabletServerOnHostPort(hp);
  }

  /**
   * Finds the RPC port of the given tablet's leader tserver.
   * @param tablet a LocatedTablet
   * @return the host and port of the given tablet's leader tserver
   * @throws Exception if we are unable to find the leader tserver
   */
  protected HostAndPort findLeaderTabletServerHostPort(LocatedTablet tablet)
      throws Exception {
    LocatedTablet.Replica leader = null;
    DeadlineTracker deadlineTracker = new DeadlineTracker();
    deadlineTracker.setDeadline(DEFAULT_SLEEP);
    while (leader == null) {
      if (deadlineTracker.timedOut()) {
        fail("Timed out while trying to find a leader for this table");
      }

      leader = tablet.getLeaderReplica();
      if (leader == null) {
        LOG.info("Sleeping while waiting for a tablet LEADER to arise, currently slept {} ms",
            deadlineTracker.getElapsedMillis());
        Thread.sleep(50);
      }
    }
    return HostAndPort.fromParts(leader.getRpcHost(), leader.getRpcPort());
  }

  /**
   * Helper method to easily kill the leader master.
   *
   * This method is thread-safe.
   * @throws Exception if there is an error finding or killing the leader master.
   */
  protected void killMasterLeader() throws Exception {
    HostAndPort hp = findLeaderMasterHostPort();
    miniCluster.killMasterOnHostPort(hp);
  }

  /**
   * Find the host and port of the leader master.
   * @return the host and port of the leader master
   * @throws Exception if we are unable to find the leader master
   */
  protected HostAndPort findLeaderMasterHostPort() throws Exception {
    Stopwatch sw = Stopwatch.createStarted();
    while (sw.elapsed(TimeUnit.MILLISECONDS) < DEFAULT_SLEEP) {
      Deferred<Master.GetTableLocationsResponsePB> masterLocD =
          client.getMasterTableLocationsPB(null);
      Master.GetTableLocationsResponsePB r = masterLocD.join(DEFAULT_SLEEP);
      HostPortPB pb = r.getTabletLocations(0)
          .getReplicas(0)
          .getTsInfo()
          .getRpcAddresses(0);
      if (pb.getPort() != -1) {
        return HostAndPort.fromParts(pb.getHost(), pb.getPort());
      }
    }
    throw new IOException(String.format("No leader master found after %d ms", DEFAULT_SLEEP));
  }

  /**
   * Picks at random a tablet server that serves tablets from the passed table and restarts it.
   * @param table table to query for a TS to restart
   * @throws Exception
   */
  protected void restartTabletServer(KuduTable table) throws Exception {
    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    if (tablets.isEmpty()) {
      fail("Table " + table.getName() + " doesn't have any tablets");
    }

    LocatedTablet tablet = tablets.get(0);
    Replica replica = tablet.getReplicas().get(randomForTSRestart.nextInt(tablet.getReplicas().size()));
    HostAndPort hp = HostAndPort.fromParts(replica.getRpcHost(), replica.getRpcPort());
    miniCluster.killTabletServerOnHostPort(hp);
    miniCluster.restartDeadTabletServerOnHostPort(hp);
  }

  /**
   * Kills a tablet server that serves the given tablet's leader and restarts it.
   * @param tablet a RemoteTablet which will get its leader killed and restarted
   * @throws Exception
   */
  protected void restartTabletServer(RemoteTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServerHostPort(new LocatedTablet(tablet));
    miniCluster.killTabletServerOnHostPort(hp);
    miniCluster.restartDeadTabletServerOnHostPort(hp);
  }

  /**
   * Kills and restarts the leader master.
   * @throws Exception
   */
  protected void restartLeaderMaster() throws Exception {
    HostAndPort hp = findLeaderMasterHostPort();
    miniCluster.killMasterOnHostPort(hp);
    miniCluster.restartDeadMasterOnHostPort(hp);
  }

  /**
   * Return the comma-separated list of "host:port" pairs that describes the master
   * config for this cluster.
   * @return The master config string.
   */
  protected String getMasterAddresses() {
    return masterAddresses;
  }

  /**
   * Kills all tablet servers in the cluster.
   * @throws InterruptedException
   */
  protected void killTabletServers() throws IOException {
    miniCluster.killTservers();
  }

  /**
   * Restarts killed tablet servers in the cluster.
   * @throws Exception
   */
  protected void restartTabletServers() throws IOException {
    miniCluster.restartDeadTservers();
  }

  /**
   * Resets the clients so that their state is completely fresh, including meta
   * cache, connections, open tables, sessions and scanners, and propagated timestamp.
   */
  protected void resetClients() throws IOException {
    syncClient.shutdown();
    client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
                                .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
                                .build();
    syncClient = client.syncClient();
  }
}
