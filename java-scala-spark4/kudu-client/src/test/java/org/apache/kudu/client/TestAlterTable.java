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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
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
    return createTable(bounds, null, 2);
  }

  /**
   * Creates a new table with two int columns, c0 and c1. c0 is the primary key.
   * The table is hash partitioned on c0 into two buckets, and range partitioned
   * with the provided bounds and the specified owner.
   */
  private KuduTable createTable(List<Pair<Integer, Integer>> bounds, String owner,
                                int buckets)
      throws KuduException {
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
                                .setNumReplicas(1);
    if (buckets > 1) {
      createOptions = createOptions.addHashPartitions(ImmutableList.of("c0"), buckets);
    }

    for (Pair<Integer, Integer> bound : bounds) {
      PartialRow lower = schema.newPartialRow();
      PartialRow upper = schema.newPartialRow();
      lower.addInt("c0", bound.getFirst());
      upper.addInt("c0", bound.getSecond());
      createOptions.addRangePartition(lower, upper);
    }

    if (owner != null) {
      createOptions.setOwner(owner);
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

  private int countRows(KuduTable table) throws KuduException {
    KuduScanner scanner = client.newScannerBuilder(table).build();
    int rowCount = 0;
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      rowCount += it.getNumRows();
    }
    return rowCount;
  }

  // This unit test is used to verify the problem KUDU-3483. Without the fix,
  // this unit test will throw an out of index exception.
  @Test
  public void testInsertDataWithChangedSchema() throws Exception {
    // Create a table with single partition in order to make all operations
    // fall into the same tablet.
    KuduTable table = createTable(ImmutableList.of(), null, 1);
    final KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);

    // Test case with the same table schema.
    {
      Insert insert = table.newInsert();
      PartialRow row1 = insert.getRow();
      row1.addInt("c0", 101);
      row1.addInt("c1", 101);
      session.apply(insert);

      Upsert upsert = table.newUpsert();
      PartialRow row2 = upsert.getRow();
      row2.addInt("c0", 102);
      row2.addInt("c1", 102);
      session.apply(upsert);
      List<OperationResponse> responses = session.flush();
      assertEquals(responses.size(), 2);

      RowError[] rowErrors = session.getPendingErrors().getRowErrors();
      assertEquals(String.format("row errors: %s",
          Arrays.toString(rowErrors)), 0, rowErrors.length);
      assertEquals(2, countRows(table));
    }

    // Test case with adding columns.
    {
      // Upsert a row with the old schema.
      Upsert upsert1 = table.newUpsert();
      PartialRow row1 = upsert1.getRow();
      row1.addInt("c0", 103);
      row1.addInt("c1", 103);
      session.apply(upsert1);

      // Add one new column.
      client.alterTable(tableName, new AlterTableOptions()
          .addColumn("addNonNull", Type.INT32, 100));

      // Reopen the table with new schema.
      table = client.openTable(tableName);
      assertEquals(3, table.getSchema().getColumnCount());

      // Upsert a row with the new schema.
      Upsert upsert2 = table.newUpsert();
      PartialRow row2 = upsert2.getRow();
      row2.addInt("c0", 104);
      row2.addInt("c1", 104);
      row2.addInt("addNonNull", 101);

      session.apply(upsert2);
      List<OperationResponse> responses = session.flush();
      assertEquals(responses.size(), 2);

      RowError[] rowErrors = session.getPendingErrors().getRowErrors();
      assertEquals(String.format("row errors: %s",
          Arrays.toString(rowErrors)), 0, rowErrors.length);

      // Read the data. It contains 4 rows.
      assertEquals(4, countRows(table));
    }

    // Test case with renamed columns.
    {
      table = client.openTable(tableName);
      // Upsert a row.
      Upsert upsert1 = table.newUpsert();
      PartialRow row1 = upsert1.getRow();
      row1.addInt("c0", 105);
      row1.addInt("c1", 105);
      row1.addInt("addNonNull", 101);
      session.apply(upsert1);

      // Rename one column.
      client.alterTable(tableName, new AlterTableOptions()
          .renameColumn("addNonNull", "newAddNonNull"));

      // Reopen the table with the new schema.
      table = client.openTable(tableName);
      assertEquals(3, table.getSchema().getColumnCount());

      // Upsert a row with the new schema.
      Upsert upsert2 = table.newUpsert();
      PartialRow row2 = upsert2.getRow();
      row2.addInt("c0", 106);
      row2.addInt("c1", 106);
      row2.addInt("newAddNonNull", 101);
      session.apply(upsert2);
      List<OperationResponse> responses = session.flush();
      assertEquals(responses.size(), 2);

      RowError[] rowErrors = session.getPendingErrors().getRowErrors();
      assertEquals(String.format("row errors: %s",
          Arrays.toString(rowErrors)), 1, rowErrors.length);
      assertTrue(Arrays.toString(rowErrors)
          .contains("Client provided column addNonNull INT32 NOT NULL not present in tablet"));

      // Read the data. It contains 5 rows, one row failed to insert.
      assertEquals(5, countRows(table));
    }

    // Test case with drop columns.
    {
      // Upsert a row.
      Upsert upsert1 = table.newUpsert();
      PartialRow row1 = upsert1.getRow();
      row1.addInt("c0", 107);
      row1.addInt("c1", 107);
      row1.addInt("newAddNonNull", 101);
      session.apply(upsert1);

      // Drop one column.
      client.alterTable(tableName, new AlterTableOptions()
          .dropColumn("newAddNonNull"));

      // Reopen the table with the new schema.
      table = client.openTable(tableName);
      assertEquals(2, table.getSchema().getColumnCount());

      // Upsert a row with the new schema.
      Upsert upsert2 = table.newUpsert();
      PartialRow row2 = upsert2.getRow();
      row2.addInt("c0", 108);
      row2.addInt("c1", 108);
      session.apply(upsert2);
      List<OperationResponse> responses = session.flush();
      assertEquals(responses.size(), 2);

      RowError[] rowErrors = session.getPendingErrors().getRowErrors();
      assertEquals(String.format("row errors: %s",
          Arrays.toString(rowErrors)), 1, rowErrors.length);
      assertTrue(Arrays.toString(rowErrors)
          .contains("Client provided column newAddNonNull INT32 NOT NULL not present in tablet"));

      // Read the data. It contains 6 rows, one row failed to insert.
      assertEquals(6, countRows(table));
    }
  }

  @Test
  public void testAlterAddColumns() throws Exception {
    KuduTable table = createTable(ImmutableList.of());
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
    final KuduSession session = client.newSession();
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
    final List<String> actual = scanTableToStrings(table);
    final List<String> expected = new ArrayList<>(101);
    for (int i = 0; i < 100; i++) {
      expected.add(i, String.format("INT32 c0=%d, INT32 c1=%d, INT32 addNonNull=100" +
          ", INT32 addNullable=NULL, INT32 addNullableDef=200", i, i));
    }
    expected.add("INT32 c0=101, INT32 c1=101, INT32 addNonNull=101" +
        ", INT32 addNullable=101, INT32 addNullableDef=NULL");
    Collections.sort(expected);
    assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));

    NonRecoverableException thrown =
            Assert.assertThrows(NonRecoverableException.class, new ThrowingRunnable() {
              @Override
              public void run() throws Exception {
                // Add duplicate column
                client.alterTable(tableName, new AlterTableOptions()
                        .addNullableColumn("addNullable", Type.INT32));
              }
            });
    Assert.assertTrue(thrown.getStatus().isAlreadyPresent());
    Assert.assertTrue(thrown.getMessage().contains("The column already exists"));
  }

  @Test
  public void testAlterModifyColumns() throws Exception {
    KuduTable table = createTable(ImmutableList.of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // Check for expected defaults.
    ColumnSchema col = table.getSchema().getColumns().get(1);
    assertEquals(CompressionAlgorithm.DEFAULT_COMPRESSION, col.getCompressionAlgorithm());
    assertEquals(Encoding.AUTO_ENCODING, col.getEncoding());
    assertNull(col.getDefaultValue());

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
    KuduTable table = createTable(ImmutableList.of());
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
    int rowCount = 0;
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      while (it.hasNext()) {
        RowResult rr = it.next();
        assertEquals(rr.getInt(0), rr.getInt(1));
        ++rowCount;
      }
    }
    assertEquals(101, rowCount);
  }

  @Test
  public void testAlterRangePartitioning() throws Exception {
    KuduTable table = createTable(ImmutableList.of());
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

    final KuduTable table = client.createTable(tableName, schema, createOptions);

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
  public void testAlterRangePartitioningInvalid() throws KuduException {
    // Create initial table with single range partition covering [0, 100).
    KuduTable table = createTable(ImmutableList.of(new Pair<>(0, 100)));
    Schema schema = table.getSchema();
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    // ADD [0, 100) <- already present (duplicate)
    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    lower.addInt("c0", 0);
    upper.addInt("c0", 100);
    try {
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(lower, upper));
      fail();
    } catch (KuduException e) {
      assertTrue(e.getStatus().isAlreadyPresent());
      assertTrue(e.getStatus().getMessage().contains(
          "range partition already exists"));
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
          "new range partition conflicts with existing one"));
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
          "new range partition conflicts with existing one"));
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
          "new range partition conflicts with existing one"));
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
          "no range partition to drop"));
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
          "no range partition to drop"));
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
          "no range partition to drop"));
    }
    assertEquals(100, countRowsInTable(table));
  }

  /**
   * Test altering a table, adding range partitions with custom hash schema
   * per range.
   */
  @Test(timeout = 100000)
  public void testAlterAddRangeWithCustomHashSchema() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
        .nullable(false)
        .build());
    final Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("c0"))
            .addHashPartitions(ImmutableList.of("c0"), 2, 0)
            .setNumReplicas(1);

    {
      // Add range partition with the table-wide hash schema (to be added upon
      // creating the new table).
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      createOptions.addRangePartition(lower, upper);
    }

    client.createTable(tableName, schema, createOptions);

    // Alter the table: add a range partition with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 200);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }

    KuduTable table = client.openTable(tableName);

    // Insert some rows and then drop partitions, ensuring the row count comes
    // as expected.
    insertRows(table, -100, 100);
    assertEquals(200, countRowsInTable(table));
    insertRows(table, 100, 200);
    assertEquals(300, countRowsInTable(table));

    {
      AlterTableOptions alter = new AlterTableOptions();
      alter.setWait(true);
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
      assertEquals(100, countRowsInTable(table));
    }

    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 200);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
      assertEquals(0, countRowsInTable(table));
    }

    // Make sure it's possible to delete/drop the table after adding and then
    // dropping a range with custom hash schema.
    client.deleteTable(tableName);
  }

  /**
   * Test altering a table, adding unbounded range partitions
   * with custom hash schema.
   */
  @Test(timeout = 100000)
  public void testAlterAddUnboundedRangeWithCustomHashSchema() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
        .nullable(false)
        .build());
    final Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("c0"))
            .addHashPartitions(ImmutableList.of("c0"), 2, 0)
            .setNumReplicas(1);
    // Add range partition [-100, 100) with the table-wide hash schema
    // (to be added upon creating the new table below).
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      createOptions.addRangePartition(lower, upper);
    }
    // Add unbounded range partition [100, +inf) with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 5, 0);
      createOptions.addRangePartition(range);
    }

    client.createTable(tableName, schema, createOptions);

    // Alter the table: add unbounded range partition [-inf, -100) with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -100);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }

    KuduTable table = client.openTable(tableName);

    // Insert some rows and then drop partitions, ensuring the row count comes
    // out as expected.
    insertRows(table, -250, -200);
    assertEquals(50, countRowsInTable(table));
    insertRows(table, -200, -50);
    assertEquals(200, countRowsInTable(table));
    insertRows(table, -50, 50);
    assertEquals(300, countRowsInTable(table));
    insertRows(table, 50, 200);
    assertEquals(450, countRowsInTable(table));
    insertRows(table, 200, 250);
    assertEquals(500, countRowsInTable(table));

    {
      AlterTableOptions alter = new AlterTableOptions();
      alter.setWait(true);
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
      assertEquals(300, countRowsInTable(table));
    }
    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -100);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
      assertEquals(150, countRowsInTable(table));
    }
    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
      assertEquals(0, countRowsInTable(table));
    }

    client.deleteTable(tableName);
  }

  /**
   * Test altering a table, adding range partitions with custom hash schema
   * per range and dropping partition in the middle, resulting in non-covered
   * ranges between partition with the table-wide and custom hash schemas.
   */
  @Test(timeout = 100000)
  public void testAlterAddRangeWithCustomHashSchemaNonCoveredRange() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
        .nullable(false)
        .build());
    final Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("c0"))
            .addHashPartitions(ImmutableList.of("c0"), 2, 0)
            .setNumReplicas(1);

    // Add 3 range partitions with the table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -300);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -200);
      createOptions.addRangePartition(lower, upper);
    }
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      createOptions.addRangePartition(lower, upper);
    }
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 200);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 300);
      createOptions.addRangePartition(lower, upper);
    }

    client.createTable(tableName, schema, createOptions);

    // Add range partitions with custom hash schemas, interlaced with the
    // partitions having the table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -400);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -300);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -200);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -100);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 4, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 200);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 5, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 300);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 400);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 6, 0);
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
    }

    KuduTable table = client.openTable(tableName);

    // Insert some rows and then drop partitions, ensuring the row count comes
    // as expected.
    insertRows(table, -400, 0);
    assertEquals(400, countRowsInTable(table));

    insertRows(table, 0, 400);
    assertEquals(800, countRowsInTable(table));

    // Drop one range with table-wide hash schema in the very middle of the
    // covered ranges.
    {
      AlterTableOptions alter = new AlterTableOptions();
      alter.setWait(true);
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
    }
    assertEquals(600, countRowsInTable(table));

    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -400);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -300);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
    }
    assertEquals(500, countRowsInTable(table));

    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 200);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
    }
    assertEquals(400, countRowsInTable(table));

    {
      AlterTableOptions alter = new AlterTableOptions();
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -200);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", -100);
      alter.dropRangePartition(lower, upper);
      client.alterTable(tableName, alter);
    }
    assertEquals(300, countRowsInTable(table));

    // Make sure it's possible to delete/drop the table after adding and then
    // dropping a range with custom hash schema.
    client.deleteTable(tableName);
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=false",
  })
  public void testAlterTryAddRangeWithCustomHashSchema() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
        .nullable(false)
        .build());
    final Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("c0"))
            .addHashPartitions(ImmutableList.of("c0"), 2, 0)
            .setNumReplicas(1);

    client.createTable(tableName, schema, createOptions);

    // Try adding a range partition with custom hash schema when server side
    // doesn't support the RANGE_SPECIFIC_HASH_SCHEMA feature.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 0);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      try {
        client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
        fail("shouldn't be able to add a range with custom hash schema " +
                "in a table when server side doesn't support required " +
                "RANGE_SPECIFIC_HASH_SCHEMA feature");
      } catch (KuduException ex) {
        final String errmsg = ex.getMessage();
        assertTrue(errmsg, ex.getStatus().isRemoteError());
        assertTrue(errmsg, errmsg.matches(
            ".* server sent error unsupported feature flags"));
      }
    }
  }

  @Test(timeout = 100000)
  public void testAlterTryAddRangeWithCustomHashSchemaDuplicateColumns()
      throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
        .nullable(false)
        .key(true)
        .build());
    final Schema schema = new Schema(columns);

    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of("c0"))
            .addHashPartitions(ImmutableList.of("c0"), 2, 0)
            .addHashPartitions(ImmutableList.of("c1"), 3, 0)
            .setNumReplicas(1);

    // Add range partition with table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 0);
      createOptions.addRangePartition(lower, upper);
    }

    client.createTable(tableName, schema, createOptions);

    // Try adding a range partition with custom hash schema having multiple
    // hash dimensions and conflicting on columns used for hash function:
    // different dimensions should not intersect on the set of columns
    // used for hashing.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 0);
      PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      range.addHashPartitions(ImmutableList.of("c0"), 3, 0);
      try {
        client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
        fail("shouldn't be able to add a range with custom hash schema " +
            "having duplicate hash columns across different dimensions");
      } catch (KuduException ex) {
        final String errmsg = ex.getMessage();
        assertTrue(errmsg, ex.getStatus().isInvalidArgument());
        assertTrue(errmsg, errmsg.matches(
            "hash bucket schema components must not contain columns in common"));
      }
    }
  }

  @Test
  public void testAlterExtraConfigs() throws Exception {
    KuduTable table = createTable(ImmutableList.of());
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

  @Test
  @KuduTestHarness.MasterServerConfig(flags = { "--max_num_columns=10" })
  public void testAlterExceedsColumnLimit() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      columns.add(new ColumnSchema.ColumnSchemaBuilder(Integer.toString(i), Type.INT32)
              .key(i == 0)
              .build());
    }
    Schema schema = new Schema(columns);
    CreateTableOptions createOptions =
            new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("0"));
    client.createTable(tableName, schema, createOptions);

    NonRecoverableException thrown =
            Assert.assertThrows(NonRecoverableException.class, new ThrowingRunnable() {
              @Override
              public void run() throws Exception {
                client.alterTable(tableName,
                        new AlterTableOptions().addNullableColumn("11", Type.INT32));
              }
            });
    Assert.assertTrue(thrown.getStatus().isInvalidArgument());
    Assert.assertTrue(thrown.getMessage()
            .contains("number of columns 11 is greater than the permitted maximum 10"));
  }

  @Test
  public void testAlterChangeOwner() throws Exception {
    String originalOwner = "alice";
    KuduTable table = createTable(ImmutableList.of(), originalOwner, 2);
    assertEquals(originalOwner, table.getOwner());

    String newOwner = "bob";
    client.alterTable(table.getName(), new AlterTableOptions().setOwner(newOwner));
    table = client.openTable(table.getName());
    assertEquals(newOwner, table.getOwner());
  }

  @Test
  public void testAlterChangeComment() throws Exception {
    String originalComment = "original comment";
    ArrayList<ColumnSchema> columns = new ArrayList<>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("val", Type.INT32).build());
    Schema schema = new Schema(columns);
    CreateTableOptions createOptions = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("key"))
        .setComment(originalComment);
    KuduTable table = client.createTable(tableName, schema, createOptions);
    assertEquals(originalComment, table.getComment());

    String newComment = "new comment";
    client.alterTable(table.getName(), new AlterTableOptions().setComment(newComment));
    table = client.openTable(table.getName());
    assertEquals(newComment, table.getComment());
  }

  @Test
  public void testAlterAddAndRemoveImmutableAttribute() throws Exception {
    KuduTable table = createTable(ImmutableList.of());
    insertRows(table, 0, 100);
    assertEquals(100, countRowsInTable(table));

    client.alterTable(tableName, new AlterTableOptions()
            .changeImmutable("c1", true));
    table = client.openTable(table.getName());
    assertTrue(table.getSchema().getColumn("c1").isImmutable());

    insertRows(table, 100, 200);
    assertEquals(200, countRowsInTable(table));

    client.alterTable(tableName, new AlterTableOptions()
            .changeImmutable("c1", false));
    table = client.openTable(table.getName());
    assertFalse(table.getSchema().getColumn("c1").isImmutable());

    insertRows(table, 200, 300);
    assertEquals(300, countRowsInTable(table));

    final ColumnSchema immu_col = new ColumnSchema.ColumnSchemaBuilder("immu_col", Type.INT32)
        .nullable(true).immutable(true).build();
    client.alterTable(tableName, new AlterTableOptions().addColumn(immu_col));
    table = client.openTable(table.getName());
    assertTrue(table.getSchema().getColumn("immu_col").isImmutable());

    insertRows(table, 300, 400);
    assertEquals(400, countRowsInTable(table));
  }
}
