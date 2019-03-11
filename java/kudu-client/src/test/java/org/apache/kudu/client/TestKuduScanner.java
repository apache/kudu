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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.RandomUtils;
import org.apache.kudu.util.DataGenerator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestKuduScanner {
  private static final String tableName = "TestKuduScanner";

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Test(timeout = 100000)
  public void testIterable() throws Exception {
    KuduTable table = client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    DataGenerator generator = new DataGenerator.DataGeneratorBuilder()
        .random(RandomUtils.getRandom())
        .build();
    KuduSession session = client.newSession();
    List<Integer> insertKeys = new ArrayList<>();
    int numRows = 10;
    for (int i = 0; i < numRows; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      generator.randomizeRow(row);
      insertKeys.add(row.getInt(0));
      session.apply(insert);
    }

    // Ensure that when an enhanced for-loop is used, there's no sharing of RowResult objects.
    KuduScanner scanner = client.newScannerBuilder(table).build();
    Set<RowResult> results = new HashSet<>();
    Set<Integer> resultKeys = new HashSet<>();
    for (RowResult rowResult : scanner) {
      results.add(rowResult);
      resultKeys.add(rowResult.getInt(0));
    }
    assertEquals(numRows, results.size());
    assertTrue(resultKeys.containsAll(insertKeys));

    // Ensure that when the reuseRowResult optimization is set, only a single RowResult is used.
    KuduScanner reuseScanner = client.newScannerBuilder(table).build();
    reuseScanner.setReuseRowResult(true);
    Set<RowResult> reuseResult = new HashSet<>();
    for (RowResult rowResult : reuseScanner) {
      reuseResult.add(rowResult);
    }
    // Ensure the same RowResult object is reused.
    assertEquals(1, reuseResult.size());
  }

  @Test(timeout = 100000)
  @KuduTestHarness.TabletServerConfig(flags = {
      "--scanner_ttl_ms=5000",
      "--scanner_gc_check_interval_us=500000"}) // 10% of the TTL.
  public void testKeepAlive() throws Exception {
    int rowCount = 500;
    int shortScannerTtlMs = 5000;

    // Create a simple table with a single partition.
    Schema tableSchema = new Schema(Collections.singletonList(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build()
    ));

    CreateTableOptions tableOptions = new CreateTableOptions()
        .setRangePartitionColumns(Collections.singletonList("key"))
        .setNumReplicas(1);
    KuduTable table = client.createTable(tableName, tableSchema, tableOptions);

    KuduSession session = client.newSession();
    for (int i = 0; i < rowCount; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt(0, i);
      session.apply(insert);
    }

    // Test that a keepAlivePeriodMs less than the scanner ttl is successful.
    KuduScanner goodScanner = client.newScannerBuilder(table)
        .batchSizeBytes(100) // Set a small batch size so the first scan doesn't read all the rows.
        .keepAlivePeriodMs(shortScannerTtlMs / 4)
        .build();
    processKeepAliveScanner(goodScanner, shortScannerTtlMs);

    // Test that a keepAlivePeriodMs greater than the scanner ttl fails.
    KuduScanner badScanner = client.newScannerBuilder(table)
        .batchSizeBytes(100) // Set a small batch size so the first scan doesn't read all the rows.
        .keepAlivePeriodMs(shortScannerTtlMs * 2)
        .build();
    try {
      processKeepAliveScanner(badScanner, shortScannerTtlMs);
      fail("Should throw a scanner not found exception");
    } catch (RuntimeException ex) {
      assertTrue(ex.getMessage().matches(".*Scanner .* not found.*"));
    }
  }

  private void processKeepAliveScanner(KuduScanner scanner, int scannerTtlMs) throws Exception {
    int i = 0;
    // Ensure reading takes longer than the scanner ttl.
    for (RowResult rowResult : scanner) {
      // Sleep for half the ttl for the first few rows. This ensures
      // we are on the same tablet and will go past the ttl without
      // a new scan request. It also ensures a single row doesn't go
      // longer than the ttl.
      if (i < 5) {
        Thread.sleep(scannerTtlMs / 2); // Sleep for half the ttl.
        i++;
      }
    }
  }
}
