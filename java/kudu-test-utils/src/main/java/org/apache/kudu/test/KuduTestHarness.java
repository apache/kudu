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
package org.apache.kudu.test;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduClient.AsyncKuduClientBuilder;
import org.apache.kudu.client.DeadlineTracker;
import org.apache.kudu.client.HostAndPort;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.RemoteTablet;
import org.apache.kudu.test.cluster.MiniKuduCluster;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;
import org.apache.kudu.test.cluster.FakeDNS;
import org.apache.kudu.test.junit.RetryRule;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

/**
 * A Junit Rule that manages a Kudu cluster and clients for testing.
 * This rule also includes utility methods for the cluster
 * and clients.
 *
 * <pre>
 * public static class TestFoo {
 *
 *  &#064;Rule
 *  public KuduTestHarness harness = new KuduTestHarness();
 *
 *  ...
 * }
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class KuduTestHarness extends ExternalResource {

  private static final Logger LOG = LoggerFactory.getLogger(KuduTestHarness.class);

  private static final int NUM_MASTER_SERVERS = 3;
  private static final int NUM_TABLET_SERVERS = 3;

  // Default timeout/sleep interval for various client operations,
  // waiting for various jobs/threads to complete, etc.
  public static final int DEFAULT_SLEEP = 50000;

  private final Random randomForTSRestart = RandomUtils.getRandom();

  private MiniKuduClusterBuilder clusterBuilder;
  private MiniKuduCluster miniCluster;

  // We create both versions of the asyncClient for ease of use.
  private AsyncKuduClient asyncClient;
  private KuduClient client;

  public KuduTestHarness(final MiniKuduClusterBuilder clusterBuilder) {
    this.clusterBuilder = clusterBuilder;
  }

  public KuduTestHarness() {
    this.clusterBuilder = getBaseClusterBuilder();
  }

  /**
   * Returns the base MiniKuduClusterBuilder used when creating a
   * KuduTestHarness with the default constructor. This is useful
   * if you want to add to the default cluster setup.
   */
  public static MiniKuduClusterBuilder getBaseClusterBuilder() {
    return new MiniKuduClusterBuilder()
        .numMasterServers(NUM_MASTER_SERVERS)
        .numTabletServers(NUM_TABLET_SERVERS);
  }

  @Override
  public Statement apply(Statement base, Description description) {
    // Set any master server flags defined in the method level annotation.
    MasterServerConfig masterServerConfig = description.getAnnotation(MasterServerConfig.class);
    if (masterServerConfig != null) {
      for (String flag : masterServerConfig.flags()) {
        clusterBuilder.addMasterServerFlag(flag);
      }
    }
    // Pass through any location mapping defined in the method level annotation.
    LocationConfig locationConfig = description.getAnnotation(LocationConfig.class);
    if (locationConfig != null) {
      for (String location : locationConfig.locations()) {
        clusterBuilder.addLocation(location);
      }
    }
    // Set any tablet server flags defined in the method level annotation.
    TabletServerConfig tabletServerConfig = description.getAnnotation(TabletServerConfig.class);
    if (tabletServerConfig != null) {
      for (String flag : tabletServerConfig.flags()) {
        clusterBuilder.addTabletServerFlag(flag);
      }
    }

    // Generate the ExternalResource Statement.
    Statement statement = super.apply(base, description);
    // Wrap in the RetryRule to rerun flaky tests.
    return new RetryRule().apply(statement, description);
  }

  @Override
  public void before() throws Exception {
    FakeDNS.getInstance().install();
    LOG.info("Creating a new MiniKuduCluster...");
    miniCluster = clusterBuilder.build();
    LOG.info("Creating a new Kudu client...");
    asyncClient = new AsyncKuduClientBuilder(miniCluster.getMasterAddressesAsString())
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
        .build();
    client = asyncClient.syncClient();
  }

  @Override
  public void after() {
    try {
      if (client != null) {
        client.shutdown();
        // No need to explicitly shutdown the async client,
        // shutting down the sync client effectively does that.
      }
    } catch (KuduException e) {
      LOG.warn("Error while shutting down the test client");
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  public KuduClient getClient() {
    return client;
  }

  public AsyncKuduClient getAsyncClient() {
    return asyncClient;
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
  public void killTabletLeader(KuduTable table) throws Exception {
    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    if (tablets.isEmpty() || tablets.size() > 1) {
      fail("Currently only support killing leaders for tables containing 1 tablet, table " +
          table.getName() + " has " + tablets.size());
    }
    LocatedTablet tablet = tablets.get(0);
    if (tablet.getReplicas().size() == 1) {
      fail("Table " + table.getName() + " only has 1 tablet, please enable replication");
    }

    HostAndPort hp = findLeaderTabletServer(tablet);
    miniCluster.killTabletServer(hp);
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
  public void killTabletLeader(RemoteTablet tablet) throws Exception {
    killTabletLeader(new LocatedTablet(tablet));
  }

  /**
   * Helper method to kill a tablet server that serves the given tablet's
   * leader. The currently running test case will be failed if the tablet has no
   * leader after some retries, or if the tablet server was already killed.
   *
   * This method is thread-safe.
   * @param tablet a LocatedTablet which will get its leader killed
   * @throws Exception
   */
  public void killTabletLeader(LocatedTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServer(tablet);
    miniCluster.killTabletServer(hp);
  }

  /**
   * Finds the RPC port of the given tablet's leader tserver.
   * @param tablet a LocatedTablet
   * @return the host and port of the given tablet's leader tserver
   * @throws Exception if we are unable to find the leader tserver
   */
  public HostAndPort findLeaderTabletServer(LocatedTablet tablet)
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
    return new HostAndPort(leader.getRpcHost(), leader.getRpcPort());
  }

  /**
   * Helper method to easily kill the leader master.
   *
   * This method is thread-safe.
   * @throws Exception if there is an error finding or killing the leader master.
   */
  public void killLeaderMasterServer() throws Exception {
    HostAndPort hp = findLeaderMasterServer();
    miniCluster.killMasterServer(hp);
  }

  /**
   * Find the host and port of the leader master.
   * @return the host and port of the leader master
   * @throws Exception if we are unable to find the leader master
   */
  public HostAndPort findLeaderMasterServer() throws Exception {
    return client.findLeaderMasterServer();
  }

  /**
   * Picks at random a tablet server that serves tablets from the passed table and restarts it.
   * @param table table to query for a TS to restart
   * @throws Exception
   */
  public void restartTabletServer(KuduTable table) throws Exception {
    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    if (tablets.isEmpty()) {
      fail("Table " + table.getName() + " doesn't have any tablets");
    }

    LocatedTablet tablet = tablets.get(0);
    LocatedTablet.Replica replica =
        tablet.getReplicas().get(randomForTSRestart.nextInt(tablet.getReplicas().size()));
    HostAndPort hp = new HostAndPort(replica.getRpcHost(), replica.getRpcPort());
    miniCluster.killTabletServer(hp);
    miniCluster.startTabletServer(hp);
  }

  /**
   * Kills a tablet server that serves the given tablet's leader and restarts it.
   * @param tablet a RemoteTablet which will get its leader killed and restarted
   * @throws Exception
   */
  public void restartTabletServer(RemoteTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServer(new LocatedTablet(tablet));
    miniCluster.killTabletServer(hp);
    miniCluster.startTabletServer(hp);
  }

  /**
   * Kills and restarts the leader master.
   * @throws Exception
   */
  public void restartLeaderMaster() throws Exception {
    HostAndPort hp = findLeaderMasterServer();
    miniCluster.killMasterServer(hp);
    miniCluster.startMasterServer(hp);
  }

  /**
   * Return the comma-separated list of "host:port" pairs that describes the master
   * config for this cluster.
   * @return The master config string.
   */
  public String getMasterAddressesAsString() {
    return miniCluster.getMasterAddressesAsString();
  }

  /**
   * @return the list of master servers
   */
  public List<HostAndPort> getMasterServers() {
    return miniCluster.getMasterServers();
  }

  /**
   * @return the list of tablet servers
   */
  public List<HostAndPort> getTabletServers() {
    return miniCluster.getMasterServers();
  }

  /**
   * @return path to the mini cluster root directory
   */
  public String getClusterRoot() {
    return miniCluster.getClusterRoot();
  }

  /**
   * Kills all the master servers.
   * Does nothing to the servers that are already dead.
   *
   * @throws IOException
   */
  public void killAllMasterServers() throws IOException {
    miniCluster.killAllMasterServers();
  }

  /**
   * Starts all the master servers.
   * Does nothing to the servers that are already running.
   *
   * @throws IOException
   */
  public void startAllMasterServers() throws IOException {
    miniCluster.startAllMasterServers();
  }

  /**
   * Kills all the tablet servers.
   * Does nothing to the servers that are already dead.
   *
   * @throws IOException
   */
  public void killAllTabletServers() throws IOException {
    miniCluster.killAllTabletServers();
  }

  /**
   * Starts all the tablet servers.
   * Does nothing to the servers that are already running.
   *
   * @throws IOException
   */
  public void startAllTabletServers() throws IOException {
    miniCluster.startAllTabletServers();
  }

  /**
   * Removes all credentials for all principals from the Kerberos credential cache.
   */
  public void kdestroy() throws IOException {
    miniCluster.kdestroy();
  }

  /**
   * Re-initialize Kerberos credentials for the given username, writing them
   * into the Kerberos credential cache.
   * @param username the username to kinit as
   */
  public void kinit(String username) throws IOException {
    miniCluster.kinit(username);
  }

  /**
   * Resets the clients so that their state is completely fresh, including meta
   * cache, connections, open tables, sessions and scanners, and propagated timestamp.
   */
  public void resetClients() throws IOException {
    client.shutdown();
    asyncClient = new AsyncKuduClientBuilder(miniCluster.getMasterAddressesAsString())
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
        .build();
    client = asyncClient.syncClient();
  }

  /**
   * An annotation that can be added to each test method to
   * define additional master server flags to be used when
   * creating the test cluster.
   *
   * ex: @MasterServerConfig(flags = { "key1=valA", "key2=valB" })
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  public @interface MasterServerConfig {
    String[] flags();
  }

  /**
   * An annotation that can be added to each test method to
   * define additional tablet server flags to be used when
   * creating the test cluster.
   *
   * ex: @TabletServerConfig(flags = { "key1=valA", "key2=valB" })
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  public @interface TabletServerConfig {
    String[] flags();
  }

  /**
   * An annotation that can be added to each test method to
   * define a location mapping for the cluster. Location
   * mappings are defined as a series of 'location:number'
   * pairs.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  public @interface LocationConfig {
    String[] locations();
  }
}
