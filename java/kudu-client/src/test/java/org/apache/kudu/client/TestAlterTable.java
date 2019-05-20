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

import static org.apache.kudu.test.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.Pair;

public class TestAlterTable {
  private String tableName;
  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    tableName = TestKuduClient.class.getName() + "-" + System.currentTimeMillis();
  }

  /**
   * Creates a new table with two int columns, c0 and c1. c0 is the primary key.
   * The table is hash partitioned on c0 into two buckets, and range partitioned
   * with the provided bounds.
   */
  private KuduTable createTable(List<Pair<Integer, Integer>> bounds) throws KuduException {
    // Create initial table with single range partition covering the entire key
    // space, and two hash buckets.
    ArrayList<ColumnSchema> columns = new ArrayList<>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
                                .nullable(false)
                                .key(true)
                                .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
                                .nullable(false)
                                .build());
    Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("c0"))
                                .setNumReplicas(1)
                                .addHashPartitions(ImmutableList.of("c0"), 2);

    for (Pair<Integer, Integer> bound : bounds) {
      PartialRow lower = schema.newPartialRow();
      PartialRow upper = schema.newPartialRow();
      lower.addInt("c0", bound.getFirst());
      upper.addInt("c0", bound.getSecond());
      createOptions.addRangePartition(lower, upper);
    }

    return client.createTable(tableName, schema, createOptions);
  }

  /**
   * Insert rows into the provided table. The table's columns must be ints, and
   * must have a primary key in the first column.
   * @param table the table
   * @param start the inclusive start key
   * @param end the exclusive end key
   */
  private void insertRows(KuduTable table, int start, int end) throws KuduException {
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    for (int i = start; i < end; i++) {
      Insert insert = table.newInsert();
      for (int idx = 0; idx < table.getSchema().getColumnCount(); idx++) {
        insert.getRow().addInt(idx, i);
      }
      session.apply(insert);
    }
    session.flush();
    RowError[] rowErrors = session.getPendingErrors().getRowErrors();
    assertEquals(String.format("row errors: %s", Arrays.toString(rowErrors)), 0, rowErrors.length);
  }

  @Test
  public void testAlterAddColumns() throws Exception {
    KuduTable table = createTable(ImmutableList.<Pair<Integer,Integer>>of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    client.alterTable(tableName, new AlterTableOptions()
        .addColumn("addNonNull", Type.INT32, 100)
        .addNullableColumn("addNullable", Type.INT32)
        .addNullableColumn("addNullableDef", Type.INT32, 200));

    // Reopen table for the new schema.
    table = client.openTable(tableName);
    assertEquals(5, table.getSchema().getColumnCount());

    // Add a row with addNullableDef=null
    KuduSession session = client.newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("c0", 101);
    row.addInt("c1", 101);
    row.addInt("addNonNull", 101);
    row.addInt("addNullable", 101);
    row.setNull("addNullableDef");
    session.apply(insert);
    session.flush();
    RowError[] rowErrors = session.getPendingErrors().getRowErrors();
    assertEquals(String.format("row errors: %s", Arrays.toString(rowErrors)), 0, rowErrors.length);

    // Check defaults applied, and that row key=101
    List<String> actual = scanTableToStrings(table);
    List<String> expected = new ArrayList<>(101);
    for (int i = 0; i < 100; i++) {
      expected.add(i, String.format("INT32 c0=%d, INT32 c1=%d, INT32 addNonNull=100" +
          ", INT32 addNullable=NULL, INT32 addNullableDef=200", i, i));
    }
    expected.add("INT32 c0=101, INT32 c1=101, INT32 addNonNull=101" +
        ", INT32 addNullable=101, INT32 addNullableDef=NULL");
    Collections.sort(expected);
    assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
  }

  @Test
  public void testAlterModifyColumns() throws Exception {
    KuduTable table = createTable(ImmutableList.<Pair<Integer,Integer>>of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // Check for expected defaults.
    ColumnSchema col = table.getSchema().getColumns().get(1);
    assertEquals(CompressionAlgorithm.DEFAULT_COMPRESSION, col.getCompressionAlgorithm());
    assertEquals(Encoding.AUTO_ENCODING, col.getEncoding());
    assertEquals(null, col.getDefaultValue());

    // Alter the table.
    client.alterTable(tableName, new AlterTableOptions()
        .changeCompressionAlgorithm(col.getName(), CompressionAlgorithm.SNAPPY)
        .changeEncoding(col.getName(), Encoding.RLE)
        .changeDefault(col.getName(), 0));

    // Check for new values.
    table = client.openTable(tableName);
    col = table.getSchema().getColumns().get(1);
    assertEquals(CompressionAlgorithm.SNAPPY, col.getCompressionAlgorithm());
    assertEquals(Encoding.RLE, col.getEncoding());
    assertEquals(0, col.getDefaultValue());
  }

  @Test
  public void testRenameKeyColumn() throws Exception {
    KuduTable table = createTable(ImmutableList.<Pair<Integer,Integer>>of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    client.alterTable(tableName, new AlterTableOptions()
            .renameColumn("c0", "c0Key"));

    // scanning with the old schema
    try {
      KuduScanner scanner = client.newScannerBuilder(table)
              .setProjectedColumnNames(Lists.newArrayList("c0", "c1")).build();
      while (scanner.hasMoreRows()) {
        scanner.nextRows();
      }
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
              "Some columns are not present in the current schema: c0"));
    }

    // Reopen table for the new schema.
    table = client.openTable(tableName);
    assertEquals("c0Key", table.getSchema().getPrimaryKeyColumns().get(0).getName());
    assertEquals(2, table.getSchema().getColumnCount());

    // Add a row
    KuduSession session = client.newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("c0Key", 101);
    row.addInt("c1", 101);
    session.apply(insert);
    session.flush();
    RowError[] rowErrors = session.getPendingErrors().getRowErrors();
    assertEquals(String.format("row errors: %s", Arrays.toString(rowErrors)), 0, rowErrors.length);

    KuduScanner scanner = client.newScannerBuilder(table)
            .setProjectedColumnNames(Lists.newArrayList("c0Key", "c1")).build();
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      assertTrue(it.hasNext());
      RowResult rr = it.next();
      assertEquals(rr.getInt(0), rr.getInt(1));
    }
  }

  @Test
  public void testAlterRangePartitioning() throws Exception {
    KuduTable table = createTable(ImmutableList.<Pair<Integer,Integer>>of());
    Schema schema = table.getSchema();

    // Insert some rows, and then drop the partition and ensure that the table is empty.
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));
    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    client.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper));
    assertEquals(0, countRowsInTable(table));

    // Add new range partition and insert rows.
    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // Replace the range partition with a different one.
    AlterTableOptions options = new AlterTableOptions();
    options.dropRangePartition(lower, upper);
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    options.addRangePartition(lower, upper);
    client.alterTable(tableName, options);
    assertEquals(0, countRowsInTable(table));
    insertRows(table, 50, 125);
    assertEquals(75, countRowsInTable(table));

    // Replace the range partition with the same one.
    client.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper)
                                                            .addRangePartition(lower, upper));
    assertEquals(0, countRowsInTable(table));
    insertRows(table, 50, 125);
    assertEquals(75, countRowsInTable(table));

    // Alter table partitioning + alter table schema
    lower.addInt("c0", 200);
    upper.addInt("c0", 300);
    client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper)
                                                            .renameTable(tableName + "-renamed")
                                                            .addNullableColumn("c2", Type.INT32));
    tableName = tableName + "-renamed";
    insertRows(table, 200, 300);
    assertEquals(175, countRowsInTable(table));
    assertEquals(3, client.openTable(tableName).getSchema().getColumnCount());

    // Drop all range partitions + alter table schema. This also serves to test
    // specifying range bounds with a subset schema (since a column was
    // previously added).
    options = new AlterTableOptions();
    options.dropRangePartition(lower, upper);
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    options.dropRangePartition(lower, upper);
    options.dropColumn("c2");
    client.alterTable(tableName, options);
    assertEquals(0, countRowsInTable(table));
    assertEquals(2, client.openTable(tableName).getSchema().getColumnCount());
  }

  /**
   * Test creating and altering a table with range partitions with exclusive
   * lower bounds and inclusive upper bounds.
   */
  @Test
  public void testAlterRangePartitioningExclusiveInclusive() throws Exception {
    // Create initial table with single range partition covering (-1, 99].
    ArrayList<ColumnSchema> columns = new ArrayList<>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
                    .nullable(false)
                    .key(true)
                    .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
                    .nullable(false)
                    .build());
    Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("c0"))
                                .setNumReplicas(1);

    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    lower.addInt("c0", -1);
    upper.addInt("c0", 99);
    createOptions.addRangePartition(lower, upper,
                                    RangePartitionBound.EXCLUSIVE_BOUND,
                                    RangePartitionBound.INCLUSIVE_BOUND);

    KuduTable table = client.createTable(tableName, schema, createOptions);

    lower.addInt("c0", 199);
    upper.addInt("c0", 299);
    client.alterTable(tableName, new AlterTableOptions().addRangePartition(
        lower, upper, RangePartitionBound.EXCLUSIVE_BOUND, RangePartitionBound.INCLUSIVE_BOUND));

    // Insert some rows, and then drop the partition and ensure that the table is empty.
    insertRows(table, 0, 100);
    insertRows(table, 200, 300);
    assertEquals(200, countRowsInTable(table));

    AlterTableOptions alter = new AlterTableOptions();
    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    alter.dropRangePartition(lower, upper,
                             RangePartitionBound.INCLUSIVE_BOUND,
                             RangePartitionBound.EXCLUSIVE_BOUND);
    lower.addInt("c0", 199);
    upper.addInt("c0", 299);
    alter.dropRangePartition(lower, upper,
                             RangePartitionBound.EXCLUSIVE_BOUND,
                             RangePartitionBound.INCLUSIVE_BOUND);
    client.alterTable(tableName, alter);

    assertEquals(0, countRowsInTable(table));
  }

  @Test
  public void testAlterRangeParitioningInvalid() throws KuduException {
    // Create initial table with single range partition covering [0, 100).
    KuduTable table = createTable(ImmutableList.of(new Pair<>(0, 100)));
    Schema schema = table.getSchema();
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // ADD [0, 100) <- illegal (duplicate)
    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    try {
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "New range partition conflicts with existing range partition"));
    }
    assertEquals(100, countRowsInTable(table));

    // ADD [50, 150) <- illegal (overlap)
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    try {
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "New range partition conflicts with existing range partition"));
    }
    assertEquals(100, countRowsInTable(table));

    // ADD [-50, 50) <- illegal (overlap)
    lower.addInt("c0", -50);
    upper.addInt("c0", 50);
    try {
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "New range partition conflicts with existing range partition"));
    }
    assertEquals(100, countRowsInTable(table));

    // ADD [200, 300)
    // ADD [-50, 150) <- illegal (overlap)
    lower.addInt("c0", 200);
    upper.addInt("c0", 300);
    AlterTableOptions options = new AlterTableOptions();
    options.addRangePartition(lower, upper);
    lower.addInt("c0", -50);
    upper.addInt("c0", 150);
    options.addRangePartition(lower, upper);
    try {
      client.alterTable(tableName, options);
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "New range partition conflicts with existing range partition"));
    }
    assertEquals(100, countRowsInTable(table));

    // DROP [<start>, <end>)
    try {
      client.alterTable(tableName,
                            new AlterTableOptions().dropRangePartition(schema.newPartialRow(),
                                                                       schema.newPartialRow()));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage(), e.getStatus().getMessage().contains(
          "No range partition found for drop range partition step"));
    }
    assertEquals(100, countRowsInTable(table));

    // DROP [50, 150)
    // RENAME foo
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    try {
      client.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper)
                                                              .renameTable("foo"));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "No range partition found for drop range partition step"));
    }
    assertEquals(100, countRowsInTable(table));
    assertFalse(client.tableExists("foo"));

    // DROP [0, 100)
    // ADD  [100, 200)
    // DROP [100, 200)
    // ADD  [150, 250)
    // DROP [0, 10)    <- illegal
    options = new AlterTableOptions();

    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    options.dropRangePartition(lower, upper);

    lower.addInt("c0", 100);
    upper.addInt("c0", 200);
    options.addRangePartition(lower, upper);
    options.dropRangePartition(lower, upper);

    lower.addInt("c0", 150);
    upper.addInt("c0", 250);
    options.addRangePartition(lower, upper);

    lower.addInt("c0", 0);
    upper.addInt("c0", 10);
    options.dropRangePartition(lower, upper);
    try {
      client.alterTable(tableName, options);
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "No range partition found for drop range partition step"));
    }
    assertEquals(100, countRowsInTable(table));
  }

  @Test
  public void testAlterExtraConfigs() throws Exception {
    KuduTable table = createTable(ImmutableList.<Pair<Integer,Integer>>of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // 1. Check for expected defaults.
    table = client.openTable(tableName);
    Map<String, String> extraConfigs = table.getExtraConfig();
    assertFalse(extraConfigs.containsKey("kudu.table.history_max_age_sec"));

    // 2. Alter history max age second to 3600
    Map<String, String> alterExtraConfigs = new HashMap<>();
    alterExtraConfigs.put("kudu.table.history_max_age_sec", "3600");
    client.alterTable(tableName, new AlterTableOptions().alterExtraConfigs(alterExtraConfigs));

    table = client.openTable(tableName);
    extraConfigs = table.getExtraConfig();
    assertTrue(extraConfigs.containsKey("kudu.table.history_max_age_sec"));
    assertEquals("3600", extraConfigs.get("kudu.table.history_max_age_sec"));

    // 3. Alter history max age second to 7200
    alterExtraConfigs = new HashMap<>();
    alterExtraConfigs.put("kudu.table.history_max_age_sec", "7200");
    client.alterTable(tableName, new AlterTableOptions().alterExtraConfigs(alterExtraConfigs));

    table = client.openTable(tableName);
    extraConfigs = table.getExtraConfig();
    assertTrue(extraConfigs.containsKey("kudu.table.history_max_age_sec"));
    assertEquals("7200", extraConfigs.get("kudu.table.history_max_age_sec"));

    // 4. Reset history max age second to default
    alterExtraConfigs = new HashMap<>();
    alterExtraConfigs.put("kudu.table.history_max_age_sec", "");
    client.alterTable(tableName, new AlterTableOptions().alterExtraConfigs(alterExtraConfigs));

    table = client.openTable(tableName);
    assertTrue(table.getExtraConfig().isEmpty());
  }
}
