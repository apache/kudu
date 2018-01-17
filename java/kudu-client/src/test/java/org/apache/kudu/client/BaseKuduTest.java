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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Common.HostPortPB;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.LocatedTablet.Replica;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.DecimalUtil;

public class BaseKuduTest {

  protected static final Logger LOG = LoggerFactory.getLogger(BaseKuduTest.class);

  // Default timeout/sleep interval for various client operations, waiting for various jobs/threads
  // to complete, etc.
  protected static final int DEFAULT_SLEEP = 50000;

  // Currently not specifying a seed since we want a random behavior when running tests that
  // restart tablet servers. Would be nice to have the same kind of facility that C++ has that dumps
  // the seed it picks so that you can re-run tests with it.
  private static final Random randomForTSRestart = new Random();

  protected static MiniKuduCluster miniCluster;

  // Expose the MiniKuduCluster builder so that subclasses can alter the builder.
  protected static final MiniKuduCluster.MiniKuduClusterBuilder miniClusterBuilder =
      new MiniKuduCluster.MiniKuduClusterBuilder();

  // Comma separate describing the master addresses and ports.
  protected static String masterAddresses;
  protected static List<HostAndPort> masterHostPorts;

  // We create both versions of the client for ease of use.
  protected static AsyncKuduClient client;
  protected static KuduClient syncClient;
  protected static final Schema basicSchema = getBasicSchema();
  protected static final Schema allTypesSchema = getSchemaWithAllTypes();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    LOG.info("Setting up before class...");
    doSetup(Integer.getInteger("NUM_MASTERS", 3), 3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
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
   * This method is intended to be called from custom @BeforeClass method to setup Kudu mini cluster
   * with the specified parameters. The #BaseKuduTest class calls it in its @BeforeClass method
   * with the default parameters.
   *
   * @param numMasters number of masters in the cluster to start
   * @param numTabletServers number of tablet servers in the cluster to start
   * @throws Exception if something goes wrong
   */
  protected static void doSetup(int numMasters, int numTabletServers)
      throws Exception {
    FakeDNS.getInstance().install();

    miniCluster = miniClusterBuilder
        .numMasters(numMasters)
        .numTservers(numTabletServers)
        .build();
    masterAddresses = miniCluster.getMasterAddresses();
    masterHostPorts = miniCluster.getMasterHostPorts();

    LOG.info("Creating new Kudu client...");
    client = new AsyncKuduClient.AsyncKuduClientBuilder(masterAddresses)
        .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
        .build();
    syncClient = client.syncClient();
  }

  protected static KuduTable createTable(String tableName, Schema schema,
                                         CreateTableOptions builder) throws KuduException {
    LOG.info("Creating table: {}", tableName);
    return client.syncClient().createTable(tableName, schema, builder);
  }

  /**
   * Counts the rows from the {@code scanner} until exhaustion. It doesn't require the scanner to
   * be new, so it can be used to finish scanning a previously-started scan.
   */
  protected static int countRowsInScan(AsyncKuduScanner scanner)
      throws Exception {
    final AtomicInteger counter = new AtomicInteger();

    Callback<Object, RowResultIterator> cb = new Callback<Object, RowResultIterator>() {
      @Override
      public Object call(RowResultIterator arg) throws Exception {
        if (arg == null) return null;
        counter.addAndGet(arg.getNumRows());
        return null;
      }
    };

    while (scanner.hasMoreRows()) {
      Deferred<RowResultIterator> data = scanner.nextRows();
      data.addCallbacks(cb, defaultErrorCB);
      data.join(DEFAULT_SLEEP);
    }
    return counter.get();
  }

  protected static int countRowsInScan(KuduScanner scanner) throws KuduException {
    int counter = 0;
    while (scanner.hasMoreRows()) {
      counter += scanner.nextRows().getNumRows();
    }
    return counter;
  }

  /**
   * Scans the table and returns the number of rows.
   * @param table the table
   * @param predicates optional predicates to apply to the scan
   * @return the number of rows in the table matching the predicates
   */
  protected long countRowsInTable(KuduTable table, KuduPredicate... predicates)
      throws KuduException {
    KuduScanner.KuduScannerBuilder scanBuilder = syncClient.newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }
    scanBuilder.setProjectedColumnIndexes(ImmutableList.<Integer>of());
    return countRowsInScan(scanBuilder.build());
  }

  protected List<String> scanTableToStrings(KuduTable table,
                                            KuduPredicate... predicates) throws Exception {
    List<String> rowStrings = Lists.newArrayList();
    KuduScanner.KuduScannerBuilder scanBuilder = syncClient.newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }
    KuduScanner scanner = scanBuilder.build();
    while (scanner.hasMoreRows()) {
      RowResultIterator rows = scanner.nextRows();
      for (RowResult r : rows) {
        rowStrings.add(r.rowToString());
      }
    }
    Collections.sort(rowStrings);
    return rowStrings;
  }

  private static final int[] KEYS = new int[] {10, 20, 30};
  protected static KuduTable createFourTabletsTableWithNineRows(String tableName) throws
      Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    for (int i : KEYS) {
      PartialRow splitRow = basicSchema.newPartialRow();
      splitRow.addInt(0, i);
      builder.addSplitRow(splitRow);
    }
    KuduTable table = createTable(tableName, basicSchema, builder);
    AsyncKuduSession session = client.newSession();

    // create a table with on empty tablet and 3 tablets of 3 rows each
    for (int key1 : KEYS) {
      for (int key2 = 1; key2 <= 3; key2++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, key1 + key2);
        row.addInt(1, key1);
        row.addInt(2, key2);
        row.addString(3, "a string");
        row.addBoolean(4, true);
        session.apply(insert).join(DEFAULT_SLEEP);
      }
    }
    session.close().join(DEFAULT_SLEEP);
    return table;
  }

  public static Schema getSchemaWithAllTypes() {
    List<ColumnSchema> columns =
        ImmutableList.of(
            new ColumnSchema.ColumnSchemaBuilder("int8", Type.INT8).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build(),
            new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build(),
            new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build(),
            new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build(),
            new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build(),
            new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build(),
            new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary-array", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary-bytebuffer", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("null", Type.STRING).nullable(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS).build(),
            new ColumnSchema.ColumnSchemaBuilder("decimal", Type.DECIMAL)
                .typeAttributes(DecimalUtil.typeAttributes(5, 3)).build());

    return new Schema(columns);
  }

  public static CreateTableOptions getAllTypesCreateTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("int8"));
  }

  public static Schema getBasicSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column1_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column2_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column3_s", Type.STRING)
        .nullable(true)
        .desiredBlockSize(4096)
        .encoding(ColumnSchema.Encoding.DICT_ENCODING)
        .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.LZ4)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL).build());
    return new Schema(columns);
  }

  public static CreateTableOptions getBasicCreateTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"));
  }

  /**
   * Creates table options with non-covering range partitioning for a table with
   * the basic schema. Range partition key ranges fall between the following values:
   *
   * [  0,  50)
   * [ 50, 100)
   * [200, 300)
   */
  public static CreateTableOptions getBasicTableOptionsWithNonCoveredRange() {
    Schema schema = basicSchema;
    CreateTableOptions option = new CreateTableOptions();
    option.setRangePartitionColumns(ImmutableList.of("key"));

    PartialRow aLowerBound = schema.newPartialRow();
    aLowerBound.addInt("key", 0);
    PartialRow aUpperBound = schema.newPartialRow();
    aUpperBound.addInt("key", 100);
    option.addRangePartition(aLowerBound, aUpperBound);

    PartialRow bLowerBound = schema.newPartialRow();
    bLowerBound.addInt("key", 200);
    PartialRow bUpperBound = schema.newPartialRow();
    bUpperBound.addInt("key", 300);
    option.addRangePartition(bLowerBound, bUpperBound);

    PartialRow split = schema.newPartialRow();
    split.addInt("key", 50);
    option.addSplitRow(split);
    return option;
  }

  protected static Insert createBasicSchemaInsert(KuduTable table, int key) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, key);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);
    return insert;
  }

  static final Callback<Object, Object> defaultErrorCB = new Callback<Object, Object>() {
    @Override
    public Object call(Object arg) throws Exception {
      if (arg == null) {
        return null;
      }
      if (arg instanceof Exception) {
        LOG.warn("Got exception", (Exception) arg);
      } else {
        LOG.warn("Got an error response back {}", arg);
      }
      return new Exception("cannot recover from error: " + arg);
    }
  };

  /**
   * Helper method to open a table. It sets the default sleep time when joining on the Deferred.
   * @param name Name of the table
   * @return A KuduTable
   * @throws Exception MasterErrorException if the table doesn't exist
   */
  protected static KuduTable openTable(String name) throws Exception {
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
  protected static void killTabletLeader(KuduTable table) throws Exception {
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
  protected static void killTabletLeader(RemoteTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServerHostPort(new LocatedTablet(tablet));
    miniCluster.killTabletServerOnHostPort(hp);
  }

  /**
   * Finds the RPC port of the given tablet's leader tserver.
   * @param tablet a LocatedTablet
   * @return the host and port of the given tablet's leader tserver
   * @throws Exception if we are unable to find the leader tserver
   */
  protected static HostAndPort findLeaderTabletServerHostPort(LocatedTablet tablet)
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
  protected static void killMasterLeader() throws Exception {
    HostAndPort hp = findLeaderMasterHostPort();
    miniCluster.killMasterOnHostPort(hp);
  }

  /**
   * Find the host and port of the leader master.
   * @return the host and port of the leader master
   * @throws Exception if we are unable to find the leader master
   */
  protected static HostAndPort findLeaderMasterHostPort() throws Exception {
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
  protected static void restartTabletServer(KuduTable table) throws Exception {
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
  protected static void restartTabletServer(RemoteTablet tablet) throws Exception {
    HostAndPort hp = findLeaderTabletServerHostPort(new LocatedTablet(tablet));
    miniCluster.killTabletServerOnHostPort(hp);
    miniCluster.restartDeadTabletServerOnHostPort(hp);
  }

  /**
   * Kills and restarts the leader master.
   * @throws Exception
   */
  protected static void restartLeaderMaster() throws Exception {
    HostAndPort hp = findLeaderMasterHostPort();
    miniCluster.killMasterOnHostPort(hp);
    miniCluster.restartDeadMasterOnHostPort(hp);
  }

  /**
   * Return the comma-separated list of "host:port" pairs that describes the master
   * config for this cluster.
   * @return The master config string.
   */
  protected static String getMasterAddresses() {
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
