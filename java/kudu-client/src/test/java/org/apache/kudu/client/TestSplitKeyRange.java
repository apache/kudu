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

import static org.apache.kudu.test.ClientTestUtil.createTableWithOneThousandRows;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class TestSplitKeyRange {
  // Generate a unique table name
  private static final String TABLE_NAME =
      TestSplitKeyRange.class.getName()+"-"+System.currentTimeMillis();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test
  @TabletServerConfig(flags = {
      "--flush_threshold_mb=1",
      "--flush_threshold_secs=1",
      // Disable rowset compact to prevent DRSs being merged because they are too small.
      "--enable_rowset_compaction=false"
  })
  public void testSplitKeyRange() throws Exception {
    final KuduTable table = createTableWithOneThousandRows(
        harness.getAsyncClient(), TABLE_NAME, 32 * 1024, DEFAULT_SLEEP);

    // Wait for mrs flushed
    Thread.sleep(5 * 1000);

    Schema schema = table.getSchema();

    // 1. Don't split tablet's key range
    // 1.1 Get all key range for table
    List<KeyRange> keyRanges = table.getAsyncClient().getTableKeyRanges(
        table,null, null, null, null,
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP, -1, DEFAULT_SLEEP).join();
    assertEquals(4, keyRanges.size());
    LocatedTablet tablet0 = keyRanges.get(0).getTablet();
    LocatedTablet tablet1 = keyRanges.get(1).getTablet();
    LocatedTablet tablet2 = keyRanges.get(2).getTablet();
    LocatedTablet tablet3 = keyRanges.get(3).getTablet();
    // 1.2 Get all key range for specified tablet
    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, null, null,
        tablet1.getPartition().getPartitionKeyStart(),
        tablet1.getPartition().getPartitionKeyEnd(),
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        -1, DEFAULT_SLEEP).join();
    assertEquals(1, keyRanges.size());
    assertEquals(tablet1.toString(), keyRanges.get(0).getTablet().toString());
    assertEquals(null, keyRanges.get(0).getPrimaryKeyStart());
    assertEquals(null, keyRanges.get(0).getPrimaryKeyEnd());

    // 2. Don't set primary key range, and splitSizeBytes > tablet's size
    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, null, null,
        tablet1.getPartition().getPartitionKeyStart(),
        tablet1.getPartition().getPartitionKeyEnd(),
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        1024 * 1024 * 1024, DEFAULT_SLEEP).join();
    assertEquals(1, keyRanges.size());
    assertEquals(tablet1.toString(), keyRanges.get(0).getTablet().toString());

    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, null, null,
        tablet1.getPartition().getPartitionKeyStart(),
        tablet2.getPartition().getPartitionKeyEnd(),
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        1024 * 1024 * 1024, DEFAULT_SLEEP).join();
    assertEquals(2, keyRanges.size());
    assertEquals(tablet1.toString(), keyRanges.get(0).getTablet().toString());
    assertEquals(tablet2.toString(), keyRanges.get(1).getTablet().toString());

    // 3. Set primary key range, and splitSizeBytes > tablet's size
    // 3.1 Non-coverage
    PartialRow partialRowStart = schema.newPartialRow();
    partialRowStart.addInt(0, 1500);
    PartialRow partialRowEnd = schema.newPartialRow();
    partialRowEnd.addInt(0, 2000);
    byte[] primaryKeyStart = KeyEncoder.encodePrimaryKey(partialRowStart);
    byte[] primaryKeyEnd = KeyEncoder.encodePrimaryKey(partialRowEnd);
    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, primaryKeyStart, primaryKeyEnd,
        tablet1.getPartition().getPartitionKeyStart(),
        tablet1.getPartition().getPartitionKeyEnd(),
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        1024 * 1024 * 1024, DEFAULT_SLEEP).join();
    // TODO: Response should be return empty. But this does not affect scan result.
    assertEquals(1, keyRanges.size());
    assertEquals(tablet1.toString(), keyRanges.get(0).getTablet().toString());
    assertEquals(partialRowStart.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(0).getPrimaryKeyStart()).toString());
    assertEquals(partialRowEnd.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(0).getPrimaryKeyEnd()).toString());

    // 3.2 Coverage, but no data. RPC return a key range for tablet's MRS, because
    // the data that in MRS may be in the [partialRowStart, partialRowEnd). But in this
    // test case, the MRS is empty.
    primaryKeyStart = KeyEncoder.encodePrimaryKey(partialRowStart);
    primaryKeyEnd = KeyEncoder.encodePrimaryKey(partialRowEnd);
    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, primaryKeyStart, primaryKeyEnd, null, null,
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        1024 * 1024 * 1024, DEFAULT_SLEEP).join();
    assertEquals(4, keyRanges.size());
    assertEquals(tablet0.toString(), keyRanges.get(0).getTablet().toString());
    assertEquals(partialRowStart.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(0).getPrimaryKeyStart()).toString());
    assertEquals(partialRowEnd.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(0).getPrimaryKeyEnd()).toString());
    assertEquals(tablet1.toString(), keyRanges.get(1).getTablet().toString());
    assertEquals(partialRowStart.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(1).getPrimaryKeyStart()).toString());
    assertEquals(partialRowEnd.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(1).getPrimaryKeyEnd()).toString());
    assertEquals(tablet2.toString(), keyRanges.get(2).getTablet().toString());
    assertEquals(partialRowStart.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(2).getPrimaryKeyStart()).toString());
    assertEquals(partialRowEnd.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(2).getPrimaryKeyEnd()).toString());
    assertEquals(tablet3.toString(), keyRanges.get(3).getTablet().toString());
    assertEquals(partialRowStart.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(3).getPrimaryKeyStart()).toString());
    assertEquals(partialRowEnd.toString(),
        KeyEncoder.decodePrimaryKey(schema, keyRanges.get(3).getPrimaryKeyEnd()).toString());

    // 4. Set primary key range, and splitSizeBytes < tablet's size
    partialRowStart = schema.newPartialRow();
    partialRowStart.addInt(0, 200);
    partialRowEnd = schema.newPartialRow();
    partialRowEnd.addInt(0, 800);
    primaryKeyStart = KeyEncoder.encodePrimaryKey(partialRowStart);
    primaryKeyEnd = KeyEncoder.encodePrimaryKey(partialRowEnd);
    keyRanges = table.getAsyncClient().getTableKeyRanges(
        table, primaryKeyStart, primaryKeyEnd, null, null,
        AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP,
        1024, DEFAULT_SLEEP).join();
    assertTrue(keyRanges.size() > 4);
    for (KeyRange keyRange : keyRanges) {
      int startKey = KeyEncoder.decodePrimaryKey(schema, keyRange.getPrimaryKeyStart()).getInt(0);
      int endKey = KeyEncoder.decodePrimaryKey(schema, keyRange.getPrimaryKeyEnd()).getInt(0);
      assertTrue(200 <= startKey);
      assertTrue(startKey <= endKey);
      assertTrue(800 >= endKey);
      assertTrue(0 < keyRange.getDataSizeBytes());
    }
  }
}
