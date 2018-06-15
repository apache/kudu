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

import static org.apache.kudu.util.ClientTestUtil.countRowsInTable;
import static org.apache.kudu.util.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.Pair;

public class TestAlterTable extends BaseKuduTest {
  private String tableName;

  @Before
  public void setTableName() {
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

    return BaseKuduTest.createTable(tableName, schema, createOptions);
  }

  /**
   * Insert rows into the provided table. The table's columns must be ints, and
   * must have a primary key in the first column.
   * @param table the table
   * @param start the inclusive start key
   * @param end the exclusive end key
   */
  private void insertRows(KuduTable table, int start, int end) throws KuduException {
    KuduSession session = syncClient.newSession();
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

    syncClient.alterTable(tableName, new AlterTableOptions()
        .addColumn("addNonNull", Type.INT32, 100)
        .addNullableColumn("addNullable", Type.INT32)
        .addNullableColumn("addNullableDef", Type.INT32, 200));

    // Reopen table for the new schema.
    table = syncClient.openTable(tableName);
    assertEquals(5, table.getSchema().getColumnCount());

    // Add a row with addNullableDef=null
    KuduSession session = syncClient.newSession();
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
    syncClient.alterTable(tableName, new AlterTableOptions()
        .changeCompressionAlgorithm(col.getName(), CompressionAlgorithm.SNAPPY)
        .changeEncoding(col.getName(), Encoding.RLE)
        .changeDefault(col.getName(), 0));

    // Check for new values.
    table = syncClient.openTable(tableName);
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

    syncClient.alterTable(tableName, new AlterTableOptions()
            .renameColumn("c0", "c0Key"));

    // scanning with the old schema
    try {
      KuduScanner scanner = syncClient.newScannerBuilder(table)
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
    table = syncClient.openTable(tableName);
    assertEquals("c0Key", table.getSchema().getPrimaryKeyColumns().get(0).getName());
    assertEquals(2, table.getSchema().getColumnCount());

    // Add a row
    KuduSession session = syncClient.newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("c0Key", 101);
    row.addInt("c1", 101);
    session.apply(insert);
    session.flush();
    RowError[] rowErrors = session.getPendingErrors().getRowErrors();
    assertEquals(String.format("row errors: %s", Arrays.toString(rowErrors)), 0, rowErrors.length);

    KuduScanner scanner = syncClient.newScannerBuilder(table)
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
    syncClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper));
    assertEquals(0, countRowsInTable(table));

    // Add new range partition and insert rows.
    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // Replace the range partition with a different one.
    AlterTableOptions options = new AlterTableOptions();
    options.dropRangePartition(lower, upper);
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    options.addRangePartition(lower, upper);
    syncClient.alterTable(tableName, options);
    assertEquals(0, countRowsInTable(table));
    insertRows(table, 50, 125);
    assertEquals(75, countRowsInTable(table));

    // Replace the range partition with the same one.
    syncClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper)
                                                            .addRangePartition(lower, upper));
    assertEquals(0, countRowsInTable(table));
    insertRows(table, 50, 125);
    assertEquals(75, countRowsInTable(table));

    // Alter table partitioning + alter table schema
    lower.addInt("c0", 200);
    upper.addInt("c0", 300);
    syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper)
                                                            .renameTable(tableName + "-renamed")
                                                            .addNullableColumn("c2", Type.INT32));
    tableName = tableName + "-renamed";
    insertRows(table, 200, 300);
    assertEquals(175, countRowsInTable(table));
    assertEquals(3, openTable(tableName).getSchema().getColumnCount());

    // Drop all range partitions + alter table schema. This also serves to test
    // specifying range bounds with a subset schema (since a column was
    // previously added).
    options = new AlterTableOptions();
    options.dropRangePartition(lower, upper);
    lower.addInt("c0", 50);
    upper.addInt("c0", 150);
    options.dropRangePartition(lower, upper);
    options.dropColumn("c2");
    syncClient.alterTable(tableName, options);
    assertEquals(0, countRowsInTable(table));
    assertEquals(2, openTable(tableName).getSchema().getColumnCount());
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

    KuduTable table = BaseKuduTest.createTable(tableName, schema, createOptions);

    lower.addInt("c0", 199);
    upper.addInt("c0", 299);
    syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(
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
    syncClient.alterTable(tableName, alter);

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
      syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
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
      syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
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
      syncClient.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
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
      syncClient.alterTable(tableName, options);
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "New range partition conflicts with existing range partition"));
    }
    assertEquals(100, countRowsInTable(table));

    // DROP [<start>, <end>)
    try {
      syncClient.alterTable(tableName,
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
      syncClient.alterTable(tableName, new AlterTableOptions().dropRangePartition(lower, upper)
                                                              .renameTable("foo"));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "No range partition found for drop range partition step"));
    }
    assertEquals(100, countRowsInTable(table));
    assertFalse(syncClient.tableExists("foo"));

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
      syncClient.alterTable(tableName, options);
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isInvalidArgument());
      assertTrue(e.getStatus().getMessage().contains(
          "No range partition found for drop range partition step"));
    }
    assertEquals(100, countRowsInTable(table));
  }
}
