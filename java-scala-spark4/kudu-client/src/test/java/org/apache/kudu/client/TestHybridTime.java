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
import static org.apache.kudu.client.ExternalConsistencyMode.CLIENT_PROPAGATED;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.util.HybridTimeUtil.HTTimestampToPhysicalAndLogical;
import static org.apache.kudu.util.HybridTimeUtil.clockTimestampToHTTimestamp;
import static org.apache.kudu.util.HybridTimeUtil.physicalAndLogicalToHTTimestamp;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.cluster.MiniKuduCluster.MiniKuduClusterBuilder;

/**
 * Tests client propagated timestamps. All the work for commit wait is done and tested on the
 * server-side, so it is not tested here.
 */
public class TestHybridTime {
  private static final Logger LOG = LoggerFactory.getLogger(TestHybridTime.class);

  // Generate a unique table name
  private static final String TABLE_NAME =
      TestHybridTime.class.getName() + "-" + System.currentTimeMillis();

  private static final Schema schema = getSchema();
  private KuduTable table;
  private KuduClient client;

  private static final MiniKuduClusterBuilder clusterBuilder =
      KuduTestHarness.getBaseClusterBuilder();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(clusterBuilder);

  @Before
  public void setUp() throws Exception {
    client = harness.getClient();
    // Use one tablet because multiple tablets don't work: we could jump from one tablet to another
    // which could change the logical clock.
    CreateTableOptions builder =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"));
    table = client.createTable(TABLE_NAME, schema, builder);
  }

  private static Schema getSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", STRING)
        .key(true)
        .build());
    return new Schema(columns);
  }

  /**
   * Write three rows. Increment the timestamp we get back from the first write to put it in the
   * future. The remaining writes should force an update to the server's clock and only increment
   * the logical value. Check that the client propagates the timestamp correctly by scanning
   * back the appropriate rows at the appropriate snapshots.
   */
  @Test(timeout = 100000)
  public void test() throws Exception {
    KuduSession session = client.newSession();
    session.setExternalConsistencyMode(CLIENT_PROPAGATED);

    // Test timestamp propagation with AUTO_FLUSH_SYNC flush mode.
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);
    List<Long> logicalValues = new ArrayList<>();

    // Perform one write so we receive a timestamp from the server and can use it to propagate a
    // modified timestamp back to the server. Following writes should force the servers to update
    // their clocks to this value and increment the logical component of the timestamp.
    insertRow(session, "0");
    assertTrue(client.hasLastPropagatedTimestamp());
    long[] clockValues = HTTimestampToPhysicalAndLogical(client.getLastPropagatedTimestamp());
    assertEquals(clockValues[1], 0);
    long futureTs = clockValues[0] + 5000000;
    client.updateLastPropagatedTimestamp(clockTimestampToHTTimestamp(futureTs,
                                                                     TimeUnit.MICROSECONDS));

    String[] keys = new String[] {"1", "2", "3", "11", "22", "33"};
    for (int i = 0; i < keys.length; i++) {
      if (i == keys.length / 2) {
        // Switch flush mode to test timestamp propagation with MANUAL_FLUSH.
        session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
      }
      insertRow(session, keys[i]);
      assertTrue(client.hasLastPropagatedTimestamp());
      clockValues = HTTimestampToPhysicalAndLogical(client.getLastPropagatedTimestamp());
      LOG.debug("Clock value after write[%d]: %s Logical value: %d",
                i, new Date(clockValues[0] / 1000).toString(), clockValues[1]);
      assertEquals(clockValues[0], futureTs);
      logicalValues.add(clockValues[1]);
      assertTrue(Ordering.natural().isOrdered(logicalValues));
    }

    // Scan all rows with READ_LATEST (the default), which should retrieve all rows.
    assertEquals(1 + keys.length, countRowsInScan(client.newScannerBuilder(table).build()));

    // Now scan at multiple snapshots with READ_AT_SNAPSHOT. The logical timestamp from the 'i'th
    // row (counted from 0) combined with the latest physical timestamp should observe 'i + 1' rows.
    for (int i = 0; i < logicalValues.size(); i++) {
      long logicalValue = logicalValues.get(i);
      long snapshotTime = physicalAndLogicalToHTTimestamp(futureTs, logicalValues.get(i));
      int expected = i + 1;
      assertEquals(
          String.format("wrong number of rows for write %d at logical timestamp %d",
              i, logicalValue),
          expected, scanAtSnapshot(snapshotTime));
    }

    // The last snapshots needs to be one into the future w.r.t. the last write's timestamp
    // to get all rows, but the snapshot timestamp can't be bigger than the propagated
    // timestamp. Ergo increase the propagated timestamp first.
    long latestLogicalValue = logicalValues.get(logicalValues.size() - 1);
    client.updateLastPropagatedTimestamp(client.getLastPropagatedTimestamp() + 1);
    long snapshotTime = physicalAndLogicalToHTTimestamp(futureTs, latestLogicalValue + 1);
    assertEquals(1 + keys.length, scanAtSnapshot(snapshotTime));
  }

  private int scanAtSnapshot(long time) throws Exception {
    AsyncKuduScanner.AsyncKuduScannerBuilder builder =
        harness.getAsyncClient().newScannerBuilder(table)
        .snapshotTimestampRaw(time)
        .readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
    return countRowsInScan(builder.build());
  }

  private void insertRow(KuduSession session, String key) throws KuduException {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString(0, key);
    session.apply(insert);
    session.flush();
  }
}
