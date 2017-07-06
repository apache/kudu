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

import static org.apache.kudu.Type.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.kudu.client.Client.ScanTokenPB;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;

public class TestScannerMultiTablet extends BaseKuduTest {
  // Generate a unique table name
  private static final String TABLE_NAME =
      TestScannerMultiTablet.class.getName()+"-"+System.currentTimeMillis();

  private static Schema schema = getSchema();

  /**
   * The timestamp after inserting the rows into the test table during
   * setupBeforeClass().
   */
  private static long beforeClassWriteTimestamp;
  private KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    // create a 4-tablets table for scanning
    CreateTableOptions builder =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key1", "key2"));

    for (int i = 1; i < 4; i++){
      PartialRow splitRow = schema.newPartialRow();
      splitRow.addString("key1", "" + i);
      splitRow.addString("key2", "");
      builder.addSplitRow(splitRow);
    }

    createTable(TABLE_NAME, schema, builder);

    KuduTable table = openTable(TABLE_NAME);
    AsyncKuduSession session = client.newSession();
    session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_SYNC);

    // The data layout ends up like this:
    // tablet '', '1': no rows
    // tablet '1', '2': '111', '122', '133'
    // tablet '2', '3': '211', '222', '233'
    // tablet '3', '': '311', '322', '333'
    String[] keys = new String[] {"1", "2", "3"};
    for (String key1 : keys) {
      for (String key2 : keys) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString(0, key1);
        row.addString(1, key2);
        row.addString(2, key2);
        Deferred<OperationResponse> d = session.apply(insert);
        d.join(DEFAULT_SLEEP);
      }
    }

    beforeClassWriteTimestamp = client.getLastPropagatedTimestamp();
  }

  @Before
  public void setup() throws Exception {
    // Reset the clients in order to clear the propagated timestamp, which may
    // have been set if other test cases ran before this one. This ensures
    // that all tests set their own state.
    resetClients();
    // Reopen the table using the reset client.
    table = openTable(TABLE_NAME);
  }

  // Test various combinations of start/end row keys.
  @Test(timeout = 100000)
  public void testKeyStartEnd() throws Exception {
    assertEquals(0,
        countRowsInScan(getScanner("", "", "1", ""))); // There's nothing in the 1st tablet
    assertEquals(1, countRowsInScan(getScanner("", "", "1", "2"))); // Grab the very first row
    assertEquals(3, countRowsInScan(getScanner("1", "1", "1", "4"))); // Grab the whole 2nd tablet
    assertEquals(3, countRowsInScan(getScanner("1", "1", "2", ""))); // Same, and peek at the 3rd
    assertEquals(3, countRowsInScan(getScanner("1", "1", "2", "0"))); // Same, different peek
    assertEquals(4,
        countRowsInScan(getScanner("1", "2", "2", "3"))); // Middle of 2nd to middle of 3rd
    assertEquals(3,
        countRowsInScan(getScanner("1", "4", "2", "4"))); // Peek at the 2nd then whole 3rd
    assertEquals(6, countRowsInScan(getScanner("1", "5", "3", "4"))); // Whole 3rd and 4th
    assertEquals(9, countRowsInScan(getScanner("", "", "4", ""))); // Full table scan

    assertEquals(9,
        countRowsInScan(getScanner("", "", null, null))); // Full table scan with empty upper
    assertEquals(9,
        countRowsInScan(getScanner(null, null, "4", ""))); // Full table scan with empty lower
    assertEquals(9,
        countRowsInScan(getScanner(null, null, null, null))); // Full table scan with empty bounds

    // Test that we can close a scanner while in between two tablets. We start on the second
    // tablet and our first nextRows() will get 3 rows. At that moment we want to close the scanner
    // before getting on the 3rd tablet.
    AsyncKuduScanner scanner = getScanner("1", "", null, null);
    Deferred<RowResultIterator> d = scanner.nextRows();
    RowResultIterator rri = d.join(DEFAULT_SLEEP);
    assertEquals(3, rri.getNumRows());
    d = scanner.close();
    rri = d.join(DEFAULT_SLEEP);
    assertNull(rri);
  }

  // Test mixing start/end row keys with predicates.
  @Test(timeout = 100000)
  public void testKeysAndPredicates() throws Exception {
    // First row from the 2nd tablet.
    ColumnRangePredicate predicate = new ColumnRangePredicate(schema.getColumnByIndex(2));
    predicate.setLowerBound("1");
    predicate.setUpperBound("1");
    assertEquals(1, countRowsInScan(getScanner("1", "", "2", "", predicate)));

    // All the 2nd tablet.
    predicate = new ColumnRangePredicate(schema.getColumnByIndex(2));
    predicate.setLowerBound("1");
    predicate.setUpperBound("3");
    assertEquals(3, countRowsInScan(getScanner("1", "", "2", "", predicate)));

    // Value that doesn't exist.
    predicate = new ColumnRangePredicate(schema.getColumnByIndex(2));
    predicate.setLowerBound("4");
    assertEquals(0, countRowsInScan(getScanner("1", "", "2", "", predicate)));

    // First row from every tablet.
    predicate = new ColumnRangePredicate(schema.getColumnByIndex(2));
    predicate.setLowerBound("1");
    predicate.setUpperBound("1");
    assertEquals(3, countRowsInScan(getScanner(null, null, null, null, predicate)));

    // All the rows.
    predicate = new ColumnRangePredicate(schema.getColumnByIndex(2));
    predicate.setLowerBound("1");
    assertEquals(9, countRowsInScan(getScanner(null, null, null, null, predicate)));
  }

  @Test(timeout = 100000)
  public void testProjections() throws Exception {
    // Test with column names.
    AsyncKuduScanner.AsyncKuduScannerBuilder builder = client.newScannerBuilder(table);
    builder.setProjectedColumnNames(Lists.newArrayList(schema.getColumnByIndex(0).getName(),
        schema.getColumnByIndex(1).getName()));
    buildScannerAndCheckColumnsCount(builder, 2);

    // Test with column indexes.
    builder = client.newScannerBuilder(table);
    builder.setProjectedColumnIndexes(Lists.newArrayList(0, 1));
    buildScannerAndCheckColumnsCount(builder, 2);

    // Test with column names overriding indexes.
    builder = client.newScannerBuilder(table);
    builder.setProjectedColumnIndexes(Lists.newArrayList(0, 1));
    builder.setProjectedColumnNames(Lists.newArrayList(schema.getColumnByIndex(0).getName()));
    buildScannerAndCheckColumnsCount(builder, 1);

    // Test with keys last with indexes.
    builder = client.newScannerBuilder(table);
    builder.setProjectedColumnIndexes(Lists.newArrayList(2, 1, 0));
    buildScannerAndCheckColumnsCount(builder, 3);

    // Test with keys last with column names.
    builder = client.newScannerBuilder(table);
    builder.setProjectedColumnNames(Lists.newArrayList(schema.getColumnByIndex(2).getName(),
        schema.getColumnByIndex(0).getName()));
    buildScannerAndCheckColumnsCount(builder, 2);
  }

  @Test(timeout = 100000)
  public void testReplicaSelections() throws Exception {
    AsyncKuduScanner scanner = client.newScannerBuilder(table)
        .replicaSelection(ReplicaSelection.LEADER_ONLY)
        .build();

    assertEquals(9, countRowsInScan(scanner));

    scanner = client.newScannerBuilder(table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .build();

    assertEquals(9, countRowsInScan(scanner));
  }

  @Test(timeout = 100000)
  public void testScanTokenReplicaSelections() throws Exception {
    ScanTokenPB.Builder pbBuilder = ScanTokenPB.newBuilder();
    pbBuilder.setTableName(table.getName());
    pbBuilder.setReplicaSelection(Common.ReplicaSelection.CLOSEST_REPLICA);
    Client.ScanTokenPB scanTokenPB = pbBuilder.build();
    final byte[] serializedToken = KuduScanToken.serialize(scanTokenPB);

    // Deserialize the scan token into a scanner, and make sure it is using
    // 'CLOSEST_REPLICA' selection policy.
    KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serializedToken, syncClient);
    assertEquals(ReplicaSelection.CLOSEST_REPLICA, scanner.getReplicaSelection());
    assertEquals(9, countRowsInScan(scanner));
  }

  @Test(timeout = 100000)
  public void testReadAtSnapshotNoTimestamp() throws Exception {
    // Perform scan in READ_AT_SNAPSHOT mode with no snapshot timestamp
    // specified. Verify that the scanner timestamp is set from the tablet
    // server response.
    AsyncKuduScanner scanner = client.newScannerBuilder(table)
        .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT)
        .build();
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
    KuduScanner syncScanner = new KuduScanner(scanner);
    assertEquals(scanner.getReadMode(), syncScanner.getReadMode());

    assertTrue(syncScanner.hasMoreRows());
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());

    int rowCount = syncScanner.nextRows().getNumRows();
    // At this point, the call to the first tablet server should have been
    // done already, so check the snapshot timestamp.
    final long tsRef = scanner.getSnapshotTimestamp();
    assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, tsRef);

    assertTrue(syncScanner.hasMoreRows());
    while (syncScanner.hasMoreRows()) {
      rowCount += syncScanner.nextRows().getNumRows();
      assertEquals(tsRef, scanner.getSnapshotTimestamp());
    }
    assertEquals(9, rowCount);
  }

  // Regression test for KUDU-2415.
  // Scanning a never-written-to tablet from a fresh client with no propagated
  // timestamp in "read-your-writes' mode should not fail.
  @Test(timeout = 100000)
  @Ignore("TODO(KUDU-2415)") // not fixed yet!
  public void testReadYourWritesFreshClientFreshTable() throws Exception {
    // NOTE: this test fails because the first tablet in the table
    // is empty and has never been written to.

    // Perform scan in READ_YOUR_WRITES mode. Before the scan, verify that the
    // propagated timestamp is unset, since this is a fresh client.
    AsyncKuduScanner scanner = client.newScannerBuilder(table)
                                     .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                                     .build();
    KuduScanner syncScanner = new KuduScanner(scanner);
    assertEquals(scanner.getReadMode(), syncScanner.getReadMode());
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, client.getLastPropagatedTimestamp());
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());

    assertEquals(9, countRowsInScan(syncScanner));
  }

  // Test multi tablets scan in READ_YOUR_WRITES mode for both AUTO_FLUSH_SYNC
  // (single operation) and MANUAL_FLUSH (batches) flush modes to ensure
  // client-local read-your-writes.
  @Test(timeout = 100000)
  public void testReadYourWrites() throws Exception {
    long preTs = beforeClassWriteTimestamp;

    // Update the propagated timestamp to ensure we see the rows written
    // in the constructor.
    syncClient.updateLastPropagatedTimestamp(preTs);

    // Perform scan in READ_YOUR_WRITES mode. Before the scan, verify that the
    // scanner timestamp is not yet set. It will get set only once the scan
    // is opened.
    AsyncKuduScanner scanner = client.newScannerBuilder(table)
                                     .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                                     .build();
    KuduScanner syncScanner = new KuduScanner(scanner);
    assertEquals(scanner.getReadMode(), syncScanner.getReadMode());
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());

    assertEquals(9, countRowsInScan(syncScanner));

    // After the scan, verify that the chosen snapshot timestamp is
    // returned from the server and it is larger than the previous
    // propagated timestamp.
    assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
    assertTrue(preTs < scanner.getSnapshotTimestamp());
    syncScanner.close();

    // Perform write in MANUAL_FLUSH (batch) mode.
    KuduSession session = syncClient.newSession();
    session.setFlushMode(KuduSession.FlushMode.MANUAL_FLUSH);
    String[] keys = new String[] {"11", "22", "33"};
    for (int i = 0; i < keys.length; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString(schema.getColumnByIndex(0).getName(), keys[i]);
      row.addString(schema.getColumnByIndex(1).getName(), keys[i]);
      session.apply(insert);
    }
    session.flush();
    session.close();

    scanner = client.newScannerBuilder(table)
                    .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                    .build();
    syncScanner = new KuduScanner(scanner);
    assertTrue(preTs < client.getLastPropagatedTimestamp());
    preTs = client.getLastPropagatedTimestamp();

    assertEquals(12, countRowsInScan(syncScanner));

    // After the scan, verify that the chosen snapshot timestamp is
    // returned from the server and it is larger than the previous
    // propagated timestamp.
    assertTrue(preTs < scanner.getSnapshotTimestamp());
    syncScanner.close();
  }

  @Test(timeout = 100000)
  public void testScanPropagatesLatestTimestamp() throws Exception {
    AsyncKuduScanner scanner = client.newScannerBuilder(table).build();

    // Initially, the client does not have the timestamp set.
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, client.getLastPropagatedTimestamp());
    assertEquals(KuduClient.NO_TIMESTAMP, syncClient.getLastPropagatedTimestamp());
    KuduScanner syncScanner = new KuduScanner(scanner);

    // Check that both clients return the same propagated timestamp.
    assertTrue(syncScanner.hasMoreRows());
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, client.getLastPropagatedTimestamp());
    assertEquals(KuduClient.NO_TIMESTAMP, syncClient.getLastPropagatedTimestamp());

    int rowCount = syncScanner.nextRows().getNumRows();
    // At this point, the call to the first tablet server should have been
    // done already, so the client should have received the propagated timestamp
    // in the scanner response.
    long asyncTsRef = client.getLastPropagatedTimestamp();
    long syncTsRef = syncClient.getLastPropagatedTimestamp();
    assertEquals(asyncTsRef, syncTsRef);
    assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, asyncTsRef);
    assertNotEquals(KuduClient.NO_TIMESTAMP, syncTsRef);

    assertTrue(syncScanner.hasMoreRows());
    while (syncScanner.hasMoreRows()) {
      rowCount += syncScanner.nextRows().getNumRows();
      final long asyncTs = client.getLastPropagatedTimestamp();
      final long syncTs = syncClient.getLastPropagatedTimestamp();
      // Next scan responses from tablet servers should move the propagated
      // timestamp further.
      assertEquals(syncTs, asyncTs);
      assertTrue(asyncTs > asyncTsRef);
      asyncTsRef = asyncTs;
    }
    assertNotEquals(0, rowCount);
  }

  @Test(timeout = 100000)
  public void testScanTokenPropagatesTimestamp() throws Exception {
    // Initially, the client does not have the timestamp set.
    assertEquals(AsyncKuduClient.NO_TIMESTAMP, client.getLastPropagatedTimestamp());
    assertEquals(KuduClient.NO_TIMESTAMP, syncClient.getLastPropagatedTimestamp());
    AsyncKuduScanner scanner = client.newScannerBuilder(table).build();
    KuduScanner syncScanner = new KuduScanner(scanner);

    // Let the client receive the propagated timestamp in the scanner response.
    syncScanner.nextRows().getNumRows();
    final long tsPrev = client.getLastPropagatedTimestamp();
    final long tsPropagated = tsPrev + 1000000;

    ScanTokenPB.Builder pbBuilder = ScanTokenPB.newBuilder();
    pbBuilder.setTableName(table.getName());
    pbBuilder.setPropagatedTimestamp(tsPropagated);
    Client.ScanTokenPB scanTokenPB = pbBuilder.build();
    final byte[] serializedToken = KuduScanToken.serialize(scanTokenPB);

    // Deserialize scan tokens and make sure the client's last propagated
    // timestamp is updated accordingly.
    assertEquals(tsPrev, client.getLastPropagatedTimestamp());
    KuduScanToken.deserializeIntoScanner(serializedToken, syncClient);
    assertEquals(tsPropagated, client.getLastPropagatedTimestamp());
  }

  @Test(timeout = 100000)
  public void testScanTokenReadMode() throws Exception {
    ScanTokenPB.Builder pbBuilder = ScanTokenPB.newBuilder();
    pbBuilder.setTableName(table.getName());
    pbBuilder.setReadMode(Common.ReadMode.READ_YOUR_WRITES);
    Client.ScanTokenPB scanTokenPB = pbBuilder.build();
    final byte[] serializedToken = KuduScanToken.serialize(scanTokenPB);

    // Deserialize scan tokens and make sure the read mode is updated accordingly.
    KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serializedToken, syncClient);
    assertEquals(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES, scanner.getReadMode());
  }

  private AsyncKuduScanner getScanner(String lowerBoundKeyOne,
                                      String lowerBoundKeyTwo,
                                      String exclusiveUpperBoundKeyOne,
                                      String exclusiveUpperBoundKeyTwo) {
    return getScanner(lowerBoundKeyOne, lowerBoundKeyTwo,
        exclusiveUpperBoundKeyOne, exclusiveUpperBoundKeyTwo, null);
  }

  private AsyncKuduScanner getScanner(String lowerBoundKeyOne,
                                      String lowerBoundKeyTwo,
                                      String exclusiveUpperBoundKeyOne,
                                      String exclusiveUpperBoundKeyTwo,
                                      ColumnRangePredicate predicate) {
    AsyncKuduScanner.AsyncKuduScannerBuilder builder = client.newScannerBuilder(table);

    if (lowerBoundKeyOne != null) {
      PartialRow lowerBoundRow = schema.newPartialRow();
      lowerBoundRow.addString(0, lowerBoundKeyOne);
      lowerBoundRow.addString(1, lowerBoundKeyTwo);
      builder.lowerBound(lowerBoundRow);
    }

    if (exclusiveUpperBoundKeyOne != null) {
      PartialRow upperBoundRow = schema.newPartialRow();
      upperBoundRow.addString(0, exclusiveUpperBoundKeyOne);
      upperBoundRow.addString(1, exclusiveUpperBoundKeyTwo);
      builder.exclusiveUpperBound(upperBoundRow);
    }

    if (predicate != null) {
      builder.addColumnRangePredicate(predicate);
    }

    return builder.build();
  }

  private void buildScannerAndCheckColumnsCount(AsyncKuduScanner.AsyncKuduScannerBuilder builder,
                                                int count) throws Exception {
    AsyncKuduScanner scanner = builder.build();
    scanner.nextRows().join(DEFAULT_SLEEP);
    RowResultIterator rri = scanner.nextRows().join(DEFAULT_SLEEP);
    assertEquals(count, rri.next().getSchema().getColumns().size());
  }

  private static Schema getSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key1", STRING)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key2", STRING)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("val", STRING)
        .nullable(true) // Important because we need to make sure it gets passed in projections
        .build());
    return new Schema(columns);
  }
}
