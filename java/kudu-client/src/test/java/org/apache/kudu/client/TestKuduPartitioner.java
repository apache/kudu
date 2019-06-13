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

import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduPartitioner {

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Test
  public void testPartitioner() throws Exception {
    // Create a table with the following 9 partitions:
    //
    //             hash bucket
    //   key     0      1     2
    //         -----------------
    //  <3333    x      x     x
    // 3333-6666 x      x     x
    //  >=6666   x      x     x
    Schema basicSchema = getBasicSchema();
    int numRanges = 3;
    int numHashPartitions = 3;
    String tableName = "TestPartitioner";

    List<PartialRow> splitRows = new ArrayList<>();
    for (int split : Arrays.asList(3333, 6666)) {
      PartialRow row = basicSchema.newPartialRow();
      row.addInt("key", split);
      splitRows.add(row);
    }

    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(Collections.singletonList("key"), numHashPartitions);
    createOptions.setRangePartitionColumns(Collections.singletonList("key"));
    for (PartialRow row : splitRows) {
      createOptions.addSplitRow(row);
    }

    KuduTable table = client.createTable(tableName, basicSchema, createOptions);
    Schema schema = table.getSchema();
    KuduPartitioner part = new KuduPartitioner.KuduPartitionerBuilder(table).build();

    assertEquals(numRanges * numHashPartitions, part.numPartitions());

    // Partition a bunch of rows, counting how many fall into each partition.
    int numRowsToPartition = 10000;
    int[] countsByPartition = new int[part.numPartitions()];
    Arrays.fill(countsByPartition, 0);
    for (int i = 0; i < numRowsToPartition; i++) {
      PartialRow row = schema.newPartialRow();
      row.addInt("key", i);
      int partIndex = part.partitionRow(row);
      countsByPartition[partIndex]++;
    }

    // We don't expect a completely even division of rows into partitions, but
    // we should be within 10% of that.
    int expectedPerPartition = numRowsToPartition / part.numPartitions();
    int fuzziness = expectedPerPartition / 10;
    int minPerPartition = expectedPerPartition - fuzziness;
    int maxPerPartition = expectedPerPartition + fuzziness;
    for (int i = 0; i < part.numPartitions(); i++) {
      assertTrue(minPerPartition <= countsByPartition[i]);
      assertTrue(maxPerPartition >= countsByPartition[i]);
    }

    // Drop the first and third range partition.
    AlterTableOptions alterOptions = new AlterTableOptions();
    alterOptions.dropRangePartition(basicSchema.newPartialRow(), splitRows.get(0));
    alterOptions.dropRangePartition(splitRows.get(1), basicSchema.newPartialRow());
    client.alterTable(tableName, alterOptions);

    // The existing partitioner should still return results based on the table
    // state at the time it was created, and successfully return partitions
    // for rows in the now-dropped range.
    assertEquals(numRanges * numHashPartitions, part.numPartitions());
    PartialRow row = schema.newPartialRow();
    row.addInt("key", 1000);
    assertEquals(0, part.partitionRow(row));

    // If we recreate the partitioner, it should get the new partitioning info.
    part = new KuduPartitioner.KuduPartitionerBuilder(table).build();
    numRanges = 1;
    assertEquals(numRanges * numHashPartitions, part.numPartitions());
  }

  @Test
  public void testPartitionerNonCoveredRange() throws Exception {
    Schema basicSchema = getBasicSchema();
    int numHashPartitions = 3;
    String tableName = "TestPartitionerNonCoveredRange";

    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(Collections.singletonList("key"), numHashPartitions);
    createOptions.setRangePartitionColumns(Collections.singletonList("key"));
    // Cover a range where 1000 <= key < 2000
    PartialRow lower = basicSchema.newPartialRow();
    lower.addInt("key", 1000);
    PartialRow upper = basicSchema.newPartialRow();
    upper.addInt("key", 2000);
    createOptions.addRangePartition(lower, upper);

    KuduTable table = client.createTable(tableName, basicSchema, createOptions);
    Schema schema = table.getSchema();
    KuduPartitioner part = new KuduPartitioner.KuduPartitionerBuilder(table).build();

    try {
      PartialRow under = schema.newPartialRow();
      under.addInt("key", 999);
      part.partitionRow(under);
      fail("partitionRow did not throw a NonCoveredRangeException");
    } catch (NonCoveredRangeException ex) {
      // Expected
    }

    try {
      PartialRow over = schema.newPartialRow();
      over.addInt("key", 999);
      part.partitionRow(over);
      fail("partitionRow did not throw a NonCoveredRangeException");
    } catch (NonCoveredRangeException ex) {
      // Expected
    }
  }

  @Test
  public void testBuildTimeout() throws Exception {
    Schema basicSchema = getBasicSchema();
    String tableName = "TestBuildTimeout";
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(Collections.singletonList("key"), 3);
    createOptions.setRangePartitionColumns(Collections.singletonList("key"));
    KuduTable table = client.createTable(tableName, basicSchema, createOptions);

    // Ensure the table information can't be found to force a timeout.
    harness.killAllMasterServers();

    int timeoutMs = 2000;
    long now = System.currentTimeMillis();
    try {
      new KuduPartitioner.KuduPartitionerBuilder(table).buildTimeout(timeoutMs).build();
      fail("No NonRecoverableException was thrown");
    } catch (NonRecoverableException ex) {
      assertTrue(ex.getMessage().startsWith("cannot complete before timeout"));
    }
    long elapsed = System.currentTimeMillis() - now;
    assertTrue(elapsed <= timeoutMs * 1.1); // Add 10% to avoid flakiness.
  }

  @Test
  public void testTableCache() throws Exception {
    String tableName = "TestTableCache";
    KuduTable table =
        client.createTable(tableName, getBasicSchema(), getBasicTableOptionsWithNonCoveredRange());

    // Populate the table cache by building the partitioner once.
    KuduPartitioner partitioner = new KuduPartitioner.KuduPartitionerBuilder(table).build();

    // Ensure the remote table information can't be found.
    harness.killAllMasterServers();

    // This partitioner should build correctly because the table cache holds the partitions
    // from the previous partitioner.
    KuduPartitioner partitionerFromCache = new KuduPartitioner.KuduPartitionerBuilder(table).build();

    assertEquals(partitioner.numPartitions(), partitionerFromCache.numPartitions());
  }
}
