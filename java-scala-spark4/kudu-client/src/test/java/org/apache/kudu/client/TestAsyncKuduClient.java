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

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.TestCase.assertFalse;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.consensus.Metadata;
import org.apache.kudu.master.Master;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.ProtobufUtils;

public class TestAsyncKuduClient {

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  @Test(timeout = 100000)
  public void testDisconnect() throws Exception {
    // Set to 1 to always test disconnecting the right server.
    CreateTableOptions options = getBasicCreateTableOptions().setNumReplicas(1);
    KuduTable table = client.createTable(
        "testDisconnect-" + System.currentTimeMillis(),
        basicSchema,
        options);

    // Test that we can reconnect to a TS after a disconnection.
    // 1. Warm up the cache.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // 2. Disconnect the client.
    disconnectAndWait();

    // 3. Count again, it will trigger a re-connection and we should not hang or fail to scan.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // Test that we can reconnect to a TS while scanning.
    // 1. Insert enough rows to have to call next() multiple times.
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    int rowCount = 200;
    for (int i = 0; i < rowCount; i++) {
      session.apply(createBasicSchemaInsert(table, i));
    }
    session.flush();

    // 2. Start a scanner with a small max num bytes.
    AsyncKuduScanner scanner = asyncClient.newScannerBuilder(table)
        .batchSizeBytes(1)
        .build();
    Deferred<RowResultIterator> rri = scanner.nextRows();
    // 3. Register the number of rows we get back. We have no control over how many rows are
    // returned. When this test was written we were getting 100 rows back.
    int numRows = rri.join(DEFAULT_SLEEP).getNumRows();
    assertNotEquals("The TS sent all the rows back, we can't properly test disconnection",
        rowCount, numRows);

    // 4. Disconnect the client.
    disconnectAndWait();

    // 5. Make sure that we can continue scanning and that we get the remaining rows back.
    assertEquals(rowCount - numRows, countRowsInScan(scanner));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void disconnectAndWait() throws InterruptedException {
    for (Connection c : asyncClient.getConnectionListCopy()) {
      c.disconnect();
    }
    Stopwatch sw = Stopwatch.createStarted();
    boolean disconnected = false;
    while (sw.elapsed(TimeUnit.MILLISECONDS) < DEFAULT_SLEEP) {
      boolean sleep = false;
      if (!asyncClient.getConnectionListCopy().isEmpty()) {
        for (Connection c : asyncClient.getConnectionListCopy()) {
          if (!c.isTerminated()) {
            sleep = true;
            break;
          }
        }

      }
      if (sleep) {
        Thread.sleep(50);
      } else {
        disconnected = true;
        break;
      }
    }
    assertTrue(disconnected);
  }

  @Test
  public void testBadHostnames() throws Exception {
    String badHostname = "some-unknown-host-hopefully";
    final int requestBatchSize = 10;

    // Test that a bad hostname for the master makes us error out quickly.
    try (AsyncKuduClient invalidClient =
           new AsyncKuduClient.AsyncKuduClientBuilder(badHostname).build()) {
      try {
        invalidClient.listTabletServers().join(1000);
        fail("This should have failed quickly");
      } catch (Exception ex) {
        assertTrue(ex instanceof NonRecoverableException);
        assertTrue(ex.getMessage().contains(badHostname));
      }
    }

    List<Master.TabletLocationsPB> tabletLocations = new ArrayList<>();
    List<Master.TSInfoPB> tsInfos = new ArrayList<>();

    // Builder three bad locations.
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();
    for (int i = 0; i < 3; i++) {
      Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
      partition.setPartitionKeyStart(ByteString.copyFrom("a" + i, UTF_8.name()));
      partition.setPartitionKeyEnd(ByteString.copyFrom("b" + i, UTF_8.name()));
      tabletPb.setPartition(partition);
      tabletPb.setTabletId(ByteString.copyFromUtf8("some id " + i));
      tabletPb.addInternedReplicas(ProtobufUtils.getFakeTabletInternedReplicaPB(
          i, Metadata.RaftPeerPB.Role.FOLLOWER));
      tabletLocations.add(tabletPb.build());
      tsInfos.add(ProtobufUtils.getFakeTSInfoPB("uuid",badHostname + i, i).build());
    }

    // Test that a tablet full of unreachable replicas won't make us retry.
    try {
      KuduTable badTable = new KuduTable(asyncClient, "Invalid table name",
          "Invalid table ID", null, null, 3, null, null, null);
      asyncClient.discoverTablets(badTable, null, requestBatchSize,
                                  tabletLocations, tsInfos, 1000);
      fail("This should have failed quickly");
    } catch (NonRecoverableException ex) {
      assertTrue(ex.getMessage().contains(badHostname));
    }
  }

  @Test
  public void testDiscoverTabletOnBadHostname() throws Exception {
    int tserverIdx = 1; // select one tserver for testing
    final CreateTableOptions options = getBasicCreateTableOptions();
    final KuduTable table = client.createTable(
            "testDiscoverTabletOnBadHostname-" + System.currentTimeMillis(),
            basicSchema,
            options);
    // Get the tserver host_port to uuid mapping
    List<HostAndPort> tservers = harness.getTabletServers();

    // call discoverTablets
    List<Master.TabletLocationsPB> tabletLocations = new ArrayList<>();
    List<Master.TSInfoPB> tsInfos = new ArrayList<>();

    // Builder three bad locations.
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();
    for (int i = 0; i < 3; i++) {
      Common.PartitionPB.Builder partition = Common.PartitionPB.newBuilder();
      partition.setPartitionKeyStart(ByteString.copyFrom("a" + i, UTF_8.name()));
      partition.setPartitionKeyEnd(ByteString.copyFrom("b" + i, UTF_8.name()));
      tabletPb.setPartition(partition);
      tabletPb.setTabletId(ByteString.copyFromUtf8("some id " + i));
      tabletPb.addInternedReplicas(ProtobufUtils.getFakeTabletInternedReplicaPB(
              i, Metadata.RaftPeerPB.Role.FOLLOWER));
      tabletLocations.add(tabletPb.build());
      List<String> hostPort = Splitter.on(':').splitToList(tservers.get(i).toString());
      String tserverHost = hostPort.get(0);
      if (i == tserverIdx) {
        // simulate IP resolve failure by hacking the hostname
        tserverHost = tserverHost + "xxx";
      }
      tsInfos.add(ProtobufUtils.getFakeTSInfoPB("tserver",
              tserverHost, Integer.parseInt(hostPort.get(1))).build());
    }
    try {
      asyncClient.discoverTablets(table, new byte[0], 100,
              tabletLocations, tsInfos, 1000);
    } catch (Exception ex) {
      fail("discoverTablets should not complain: " + ex.getMessage());
    }
  }

  @Test
  public void testNoLeader() throws Exception {
    final int requestBatchSize = 10;
    final CreateTableOptions options = getBasicCreateTableOptions();
    final KuduTable table = client.createTable(
        "testNoLeader-" + System.currentTimeMillis(),
        basicSchema,
        options);

    // Lookup the current locations so that we can pass some valid information to discoverTablets.
    final List<LocatedTablet> tablets = asyncClient
        .locateTable(table, null, null, requestBatchSize, DEFAULT_SLEEP)
        .join(DEFAULT_SLEEP);
    final LocatedTablet tablet = tablets.get(0);
    final LocatedTablet.Replica leader = tablet.getLeaderReplica();

    // Fake a master lookup that only returns one follower for the tablet.
    final List<Master.TabletLocationsPB> tabletLocations = new ArrayList<>();
    final List<Master.TSInfoPB> tsInfos = new ArrayList<>();
    Master.TabletLocationsPB.Builder tabletPb = Master.TabletLocationsPB.newBuilder();
    tabletPb.setPartition(ProtobufUtils.getFakePartitionPB());
    tabletPb.setTabletId(ByteString.copyFrom(tablet.getTabletId()));
    tabletPb.addInternedReplicas(ProtobufUtils.getFakeTabletInternedReplicaPB(
        0, Metadata.RaftPeerPB.Role.FOLLOWER));
    tabletLocations.add(tabletPb.build());
    tsInfos.add(ProtobufUtils.getFakeTSInfoPB(
        "master", leader.getRpcHost(), leader.getRpcPort()).build());
    try {
      asyncClient.discoverTablets(table, new byte[0], requestBatchSize,
                                  tabletLocations, tsInfos, 1000);
      fail("discoverTablets should throw an exception if there's no leader");
    } catch (NoLeaderFoundException ex) {
      // Expected.
    }
  }

  @Test
  public void testConnectionRefused() throws Exception {
    CreateTableOptions options = getBasicCreateTableOptions();
    KuduTable table = client.createTable(
        "testConnectionRefused-" + System.currentTimeMillis(),
        basicSchema,
        options);

    // Warm up the caches.
    assertEquals(0, countRowsInScan(client.newScannerBuilder(table).build()));

    // Make it impossible to use Kudu.
    harness.killAllTabletServers();

    // Create a scan with a short timeout.
    KuduScanner scanner = client.newScannerBuilder(table).scanRequestTimeout(1000).build();

    // Check it fails.
    try {
      while (scanner.hasMoreRows()) {
        scanner.nextRows();
        fail("The scan should timeout");
      }
    } catch (NonRecoverableException ex) {
      assertTrue(ex.getStatus().isTimedOut());
    }

    // Try the same thing with an insert.
    KuduSession session = client.newSession();
    session.setTimeoutMillis(1000);
    OperationResponse response = session.apply(createBasicSchemaInsert(table, 1));
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isTimedOut());
  }


  /**
   * Test creating a table with out of order primary keys in the table schema .
   */
  @Test(timeout = 100000)
  public void testCreateTableOutOfOrderPrimaryKeys() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(6);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key_1", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column1_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key_2", Type.INT16).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column2_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column3_s", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL).build());
    Schema schema = new Schema(columns);
    try {
      asyncClient.createTable("testCreateTableOutOfOrderPrimaryKeys-" + System.currentTimeMillis(),
          schema,
          getBasicCreateTableOptions()).join();
      fail();
    } catch (NonRecoverableException nre) {
      assertTrue(nre.getMessage().startsWith("Got out-of-order key column"));
    }
  }

  /**
   * Test supportsIgnoreOperations() when the cluster does support them.
   */
  @Test(timeout = 100000)
  public void testSupportsIgnoreOperationsTrue() throws Exception {
    assertTrue(asyncClient.supportsIgnoreOperations().join());
  }

  /**
   * Test supportsIgnoreOperations() when the cluster does not support them.
   */
  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = { "--master_support_ignore_operations=false" })
  public void testSupportsIgnoreOperationsFalse() throws Exception {
    assertFalse(asyncClient.supportsIgnoreOperations().join());
  }
}
