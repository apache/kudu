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

import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithArrayColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithNonUniqueKey;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getSchemaWithArrayTypes;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.TimestampUtil;

/**
 * Advanced KuduClient tests for features like auto-incrementing columns,
 * array types, and custom partitioning schemas.
 */
public class TestKuduClientAdvanced {
  private static final String TABLE_NAME = "TestKuduClientAdvanced";

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Rule(order = Integer.MIN_VALUE)
  public TestRule watcherRule = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      System.out.println("[ TEST: STARTING  ] " + description.getMethodName());
    }

    @Override
    protected void succeeded(Description description) {
      System.out.println("[ TEST: SUCCEEDED ] " + description.getMethodName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
      System.out.println("[ TEST: FAILED    ] " + description.getMethodName());
    }
  };

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  /**
   * Test creating a table with non unique primary key in the table schema.
   */
  @Test(timeout = 100000)
  public void testCreateTableWithNonUniquePrimaryKeys() throws Exception {
    // Create a schema with non unique primary key column
    Schema schema = createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    // Verify auto-incrementing column is in the schema
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    // Create a table
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    // Verify that the primary key is not unique, and an auto-incrementing column is
    // added as key column in the position after all key columns.
    schema = table.getSchema();
    assertFalse(schema.isPrimaryKeyUnique());
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    assertTrue(schema.getColumn(Schema.getAutoIncrementingColumnName()).isKey());
    assertTrue(schema.getColumn(
        Schema.getAutoIncrementingColumnName()).isAutoIncrementing());

    // Insert rows into the table without assigning values for the auto-incrementing
    // column.
    for (int i = 0; i < 3; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", i);
      row.addInt("c1", i * 10);
      session.apply(insert);
    }
    session.flush();

    // Scan all the rows in the table with all columns.
    // Verify that the auto-incrementing column is included in the rows.
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
          i, Schema.getAutoIncrementingColumnName(), i + 1, i * 10));
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    // Update "c1" column of the first row with "key" and auto-incrementing columns.
    Update update = table.newUpdate();
    PartialRow row = update.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    row.addInt(schema.getColumnByIndex(2).getName(), 100);
    session.apply(update);
    session.flush();

    // Scan all the rows in the table without the auto-incrementing column.
    // Verify that "c1" column of the first row is updated.
    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
    KuduScanner scanner =
        scanBuilder.setProjectedColumnNames(Lists.newArrayList("key", "c1")).build();
    rowStrings.clear();
    for (RowResult r : scanner) {
      rowStrings.add(r.rowToString());
    }
    Collections.sort(rowStrings);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      if (i == 0) {
        expectedRow.append(String.format("INT32 key=0, INT32 c1=100"));
      } else {
        expectedRow.append(String.format("INT32 key=%d, INT32 c1=%d", i, i * 10));
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    // Upsert rows into the table after assigning values for the auto-incrementing
    // column. The first three rows will be applied as updates and the next three as
    // inserts.
    for (int i = 0; i < 6; i++) {
      Upsert upsert = table.newUpsert();
      row = upsert.getRow();
      row.addInt("key", i);
      row.addLong(Schema.getAutoIncrementingColumnName(), i + 1);
      row.addInt("c1", i * 20);
      session.apply(upsert);
    }
    session.flush();

    // Scan all the rows in the table with all columns.
    // Verify that the auto-incrementing column is included in the rows.
    rowStrings = scanTableToStrings(table);
    assertEquals(6, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      String expectedRow = String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
              i, Schema.getAutoIncrementingColumnName(), i + 1, i * 20);
      assertEquals(expectedRow, rowStrings.get(i));
    }

    // Delete the first row with "key" and auto-incrementing columns.
    // Verify that number of rows is decreased by 1.
    Delete delete = table.newDelete();
    row = delete.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    session.apply(delete);
    session.flush();
    assertEquals(5, countRowsInScan(client.newScannerBuilder(table).build()));

    // Check that we can delete the table.
    client.deleteTable(TABLE_NAME);
  }

  /**
   * Test operations for table with auto-incrementing column.
   */
  @Test(timeout = 100000)
  public void testTableWithAutoIncrementingColumn() throws Exception {
    // Create a schema with non unique primary key column
    Schema schema = createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    // Verify auto-incrementing column is in the schema
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    // Create a table
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    final KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    schema = table.getSchema();
    assertTrue(schema.hasAutoIncrementingColumn());

    // Verify that UPSERT is allowed for table with auto-incrementing column
    Upsert upsert = table.newUpsert();
    PartialRow rowUpsert = upsert.getRow();
    rowUpsert.addInt("key", 0);
    rowUpsert.addLong(Schema.getAutoIncrementingColumnName(), 1);
    rowUpsert.addInt("c1", 10);
    session.apply(upsert);

    // Verify that UPSERT_IGNORE is allowed for table with auto-incrementing column
    UpsertIgnore upsertIgnore = table.newUpsertIgnore();
    PartialRow rowUpsertIgnore = upsertIgnore.getRow();
    rowUpsertIgnore.addInt("key", 1);
    rowUpsertIgnore.addLong(Schema.getAutoIncrementingColumnName(), 2);
    rowUpsertIgnore.addInt("c1", 20);
    session.apply(upsertIgnore);

    // Change desired block size for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeDesiredBlockSize(
        Schema.getAutoIncrementingColumnName(), 1));
    // Change encoding for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeEncoding(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.Encoding.PLAIN_ENCODING));
    // Change compression algorithm for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeCompressionAlgorithm(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.CompressionAlgorithm.NO_COMPRESSION));
    session.flush();

    // Verify that auto-incrementing column value cannot be specified in an INSERT operation.
    try {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insert);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }

    // Verify that auto-incrementing column value cannot be specified in an INSERT_IGNORE operation.
    try {
      InsertIgnore insertIgnore = table.newInsertIgnore();
      PartialRow row = insertIgnore.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insertIgnore);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }
    // Verify that auto-incrementing column cannot be added
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          Schema.getAutoIncrementingColumnName(), Schema.getAutoIncrementingColumnType(), 0));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          new ColumnSchema.AutoIncrementingColumnSchemaBuilder().build()));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }

    // Verify that auto-incrementing column cannot be removed
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().dropColumn(
          Schema.getAutoIncrementingColumnName()));
      fail("Drop auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot remove auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be renamed
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().renameColumn(
          Schema.getAutoIncrementingColumnName(), "new_auto_incrementing"));
      fail("Rename auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot rename auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be changed by removing default
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().removeDefault(
          Schema.getAutoIncrementingColumnName()));
      fail("Remove default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Auto-incrementing column " +
          Schema.getAutoIncrementingColumnName() + " does not have default value"));
    }

    // Verify that auto-incrementing column cannot be changed with default value
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeDefault(
          Schema.getAutoIncrementingColumnName(), 0));
      fail("Change default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot set default value for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be changed for its immutable
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeImmutable(
          Schema.getAutoIncrementingColumnName(), true));
      fail("Change immutable for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot change immutable for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    client.deleteTable(TABLE_NAME);
  }

  /**
   * Test creating a table schema with an array type column.
   */
  @Test(timeout = 100000)
  public void testArrayTypeColumnCreationAndAlter() throws Exception {
    Schema schema = createSchemaWithArrayColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduTable table = client.openTable(TABLE_NAME);
    Schema returnedSchema = table.getSchema();

    // Verify initial columns (as in your original test)
    assertEquals(5, returnedSchema.getColumnCount());
    assertTrue(returnedSchema.getColumn("int_arr").isArray());
    assertFalse(returnedSchema.getColumn("int_arr").isKey());
    assertTrue(returnedSchema.getColumn("nullable_int_arr").isArray());
    assertTrue(returnedSchema.getColumn("dec_arr").isArray());
    assertTrue(returnedSchema.getColumn("str_arr").isArray());

    // ---- Sub-scenario: Alter table ----
    AlterTableOptions alter = new AlterTableOptions();

    // Add new array columns
    ColumnSchema newFloatArray =
            new ColumnSchema.ColumnSchemaBuilder("string_arr", Type.STRING)
                    .array(true)
                    .nullable(true)
                    .build();
    alter.addColumn(newFloatArray);

    ColumnSchema newIntCol =
            new ColumnSchema.ColumnSchemaBuilder("int_col", Type.INT32)
                    .array(false)
                    .nullable(true)
                    .build();
    alter.addColumn(newIntCol);

    // Drop existing array column
    alter.dropColumn("nullable_int_arr");

    client.alterTable(TABLE_NAME, alter);

    // Validate altered schema
    table = client.openTable(TABLE_NAME);
    returnedSchema = table.getSchema();

    // Column count should stay 6 (added two, dropped one)
    assertEquals(6, returnedSchema.getColumnCount());

    // Check the new columns
    ColumnSchema floatArr = returnedSchema.getColumn("string_arr");
    assertTrue(floatArr.isArray());
    assertTrue(floatArr.isNullable());
    assertEquals(Type.NESTED, floatArr.getType());
    assertEquals(Type.STRING,
            floatArr.getNestedTypeDescriptor().getArrayDescriptor().getElemType());
    ColumnSchema intCol = returnedSchema.getColumn("int_col");
    assertFalse(intCol.isArray());
    assertTrue(intCol.isNullable());
    assertNotEquals(Type.NESTED, intCol.getType());
    assertEquals(Type.INT32, intCol.getType());

    // Check dropped column is gone
    assertFalse(returnedSchema.hasColumn("nullable_int_arr"));

    client.deleteTable(TABLE_NAME);
  }

  /**
   * Test inserting and retrieving rows from a table that has a range partition
   * with custom hash schema.
   */
  @Test(timeout = 100000)
  public void testRangeWithCustomHashSchema() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT64).key(true).build());
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32).nullable(true).build());
    Schema schema = new Schema(cols);

    CreateTableOptions options = new CreateTableOptions();
    options.setRangePartitionColumns(ImmutableList.of("c0"));
    options.addHashPartitions(ImmutableList.of("c0"), 2);

    // Add range partition with table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 100);
      options.addRangePartition(lower, upper);
    }

    // Add a partition with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("c0"), 5, 0);
      options.addRangePartition(rangePartition);
    }

    client.createTable(TABLE_NAME, schema, options);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    KuduTable table = client.openTable(TABLE_NAME);

    // Check the range with the table-wide hash schema.
    {
      for (int i = 0; i < 10; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 1000 * i);
        session.apply(insert);
      }

      // Scan all the rows in the table.
      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(10, rowStringsAll.size());

      // Now scan the rows that are in the range with the table-wide hash schema.
      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 0),
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), LESS, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d", i, 1000 * i));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
    }

    // Check the range with the custom hash schema.
    {
      for (int i = 100; i < 110; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 2 * i);
        session.apply(insert);
      }

      // Scan all the rows in the table.
      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(20, rowStringsAll.size());

      // Now scan the rows that are in the range with the custom hash schema.
      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d",
            i + 100, 2 * (i + 100)));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
    }
  }

  /**
   * Test an attempt to create a new table with array type column
   * when the cluster doesn't support tables with 1D array columns.
   */
  @MasterServerConfig(flags = {
      "--master_support_1d_array_columns=false",
  })
  @Test(timeout = 100000)
  public void testCreateTableWithArrayColumnUnsupportedFeatureFlags() throws Exception {
    try {
      Schema schema = createSchemaWithArrayColumns();
      client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
      fail("should have failed with 'unsupported feature flags' server-side error");
    } catch (RpcRemoteException e) {
      assertTrue(e.getMessage().contains("server sent error unsupported feature flags"));
    }
  }

  /**
   * Test an attempt to add a new array column into an existing table
   * when the cluster doesn't support tables with 1D array columns.
   */
  @MasterServerConfig(flags = {
      "--master_support_1d_array_columns=false",
  })
  @Test(timeout = 100000)
  public void testAddArrayColumnUnsupportedFeatureFlags() throws Exception {
    try {
      AlterTableOptions alter = new AlterTableOptions();
      ColumnSchema col =
          new ColumnSchema.ColumnSchemaBuilder("int32_arr", Type.INT32)
              .array(true)
              .nullable(true)
              .build();
      alter.addColumn(col);
      client.alterTable(TABLE_NAME, alter);
      fail("should have failed with 'unsupported feature flags' server-side error");
    } catch (RpcRemoteException e) {
      assertTrue(e.getMessage().contains("server sent error unsupported feature flags"));
    }
  }

  /**
   * Test inserting and retrieving array columns.
   */
  @Test
  public void testArrayColumns() throws Exception {
    Schema schema = getSchemaWithArrayTypes();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < 10; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();

      row.addInt("key", i);

      // INT8 array (nullable elements)
      byte[] int8Values = new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2)};
      boolean[] int8Validity = new boolean[]{true, i % 2 == 0, true};
      row.addArrayInt8("int8_arr", int8Values, int8Validity);

      // BOOL array (nullable elements)
      boolean[] boolValues = new boolean[]{true, i % 2 == 0, false};
      boolean[] boolValidity = new boolean[]{true, false, true};
      row.addArrayBool("bool_arr", boolValues, boolValidity);

      // BINARY array (nullable elements)
      byte[][] binValues = new byte[][]{
          ("bin_" + i).getBytes(StandardCharsets.UTF_8),
          null,
          ("bin_last_" + i).getBytes(StandardCharsets.UTF_8)
      };
      row.addArrayBinary("binary_arr", binValues, null);

      // INT32 array with some nulls
      if (i % 3 == 0) {
        row.setNull("int32_arr");
      } else {
        int[] values = new int[]{i, i + 1, i + 2};
        boolean[] validity = new boolean[]{true, i % 2 == 0, true};
        row.addArrayInt32("int32_arr", values, validity);
      }

      // STRING array
      row.addArrayString("string_arr", new String[]{
          "row" + i,
          null,
          "long_string_" + i
      });

      // DECIMAL array (scale = 2 for testing)
      row.addArrayDecimal("decimal_arr", new BigDecimal[]{
          BigDecimal.valueOf(123, 2),
          null,
          BigDecimal.valueOf(i * 100L, 2)
      }, null);

      // DATE array
      row.addArrayDate("date_arr", new java.sql.Date[]{
          DateUtil.epochDaysToSqlDate(i),
          null,
          DateUtil.epochDaysToSqlDate(i + 5)
      });

      // TIMESTAMP array
      row.addArrayTimestamp("ts_arr", new Timestamp[]{
          TimestampUtil.microsToTimestamp(i * 1000L),
          null,
          TimestampUtil.microsToTimestamp((i + 1) * 1000L)
      });

      // VARCHAR array
      row.addArrayVarchar("varchar_arr", new String[]{
          "row" + i,
          null,
          "varchar" + i
      });

      session.apply(insert);
    }
    session.flush();

    // Scan back stringified form and verify logical output
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(10, rowStrings.size());

    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append("INT32 key=").append(i);

      // INT8[] column
      expectedRow.append(", INT8[] int8_arr=[")
          .append(i).append(", ")
          .append(i % 2 == 0 ? i + 1 : "NULL").append(", ")
          .append(i + 2).append("]");

      // BOOL[] column
      expectedRow.append(", BOOL[] bool_arr=[true, NULL, false]");

      // BINARY[] column
      expectedRow.append(", BINARY[] binary_arr=[bin_").append(i)
          .append(", null, bin_last_").append(i).append("]");


      // INT32[] column
      expectedRow.append(", INT32[] int32_arr=");
      if (i % 3 == 0) {
        expectedRow.append("NULL");
      } else {
        expectedRow.append("[");
        expectedRow.append(i); // always valid
        expectedRow.append(", ");
        if (i % 2 == 0) {
          expectedRow.append(i + 1);
        } else {
          expectedRow.append("NULL");
        }
        expectedRow.append(", ");
        expectedRow.append(i + 2);
        expectedRow.append("]");
      }

      // STRING[] column
      expectedRow.append(", STRING[] string_arr=[row").append(i)
          .append(", null, long_string_").append(i).append("]");

      // DECIMAL[] column
      expectedRow.append(", DECIMAL[] decimal_arr(5, 2)=[1.23, null, ")
          .append(BigDecimal.valueOf(i * 100L, 2)).append("]");

      // DATE[] column
      expectedRow.append(", DATE[] date_arr=[");
      expectedRow.append(DateUtil.epochDaysToSqlDate(i));
      expectedRow.append(", null, ");
      expectedRow.append(DateUtil.epochDaysToSqlDate(i + 5));
      expectedRow.append("]");

      // TIMESTAMP[] column
      expectedRow.append(", UNIXTIME_MICROS[] ts_arr=[");
      expectedRow.append(TimestampUtil.timestampToString(
          TimestampUtil.microsToTimestamp(i * 1000L)));
      expectedRow.append(", NULL, ");
      expectedRow.append(TimestampUtil.timestampToString(
          TimestampUtil.microsToTimestamp((i + 1) * 1000L)));
      expectedRow.append("]");

      // VARCHAR[] column
      expectedRow.append(", VARCHAR[] varchar_arr(10)=[row").append(i)
          .append(", null, varchar").append(i).append("]");

      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    // Verify actual array decoding by logical type
    KuduScanner scanner = client.newScannerBuilder(table).build();
    while (scanner.hasMoreRows()) {
      for (RowResult rr : scanner.nextRows()) {
        int key = rr.getInt("key");

        // INT8[]
        Object int8Obj = rr.getArrayData("int8_arr");
        assertTrue(int8Obj instanceof Byte[]);
        Byte[] int8Vals = (Byte[]) int8Obj;
        assertEquals(3, int8Vals.length);
        assertEquals(Byte.valueOf((byte) key), int8Vals[0]);
        if (key % 2 == 0) {
          assertEquals(Byte.valueOf((byte) (key + 1)), int8Vals[1]);
        } else {
          assertNull(int8Vals[1]);
        }
        assertEquals(Byte.valueOf((byte) (key + 2)), int8Vals[2]);

        // BOOL[]
        Object boolObj = rr.getArrayData("bool_arr");
        assertTrue(boolObj instanceof Boolean[]);
        Boolean[] boolVals = (Boolean[]) boolObj;
        assertEquals(3, boolVals.length);
        assertEquals(Boolean.TRUE, boolVals[0]);
        assertNull(boolVals[1]);
        assertEquals(Boolean.FALSE, boolVals[2]);

        // BINARY[]
        Object binObj = rr.getArrayData("binary_arr");
        assertTrue(binObj instanceof byte[][]);
        byte[][] binVals = (byte[][]) binObj;
        assertEquals(3, binVals.length);
        assertEquals(("bin_" + key), new String(binVals[0], StandardCharsets.UTF_8));
        assertNull(binVals[1]);
        assertEquals(("bin_last_" + key), new String(binVals[2], StandardCharsets.UTF_8));

        // INT32[] -> Integer[]
        Object intObj = rr.getArrayData("int32_arr");
        if (key % 3 == 0) {
          // Entire array was intentionally NULL
          assertNull(intObj);
        } else {
          assertTrue(intObj instanceof Integer[]);
          Integer[] intVals = (Integer[]) intObj;
          assertEquals(3, intVals.length);
          assertEquals(Integer.valueOf(key), intVals[0]);
          if (key % 2 == 0) {
            assertEquals(Integer.valueOf(key + 1), intVals[1]);
          } else {
            assertNull(intVals[1]);
          }
          assertEquals(Integer.valueOf(key + 2), intVals[2]);
        }

        // STRING[] -> String[]
        Object strObj = rr.getArrayData("string_arr");
        assertTrue(strObj instanceof String[]);
        String[] strVals = (String[]) strObj;
        assertEquals(3, strVals.length);
        assertEquals("row" + key, strVals[0]);
        assertNull(strVals[1]);
        assertEquals("long_string_" + key, strVals[2]);

        // DECIMAL[] -> BigDecimal[]
        Object decObj = rr.getArrayData("decimal_arr");
        assertTrue(decObj instanceof BigDecimal[]);
        BigDecimal[] decVals = (BigDecimal[]) decObj;
        assertEquals(3, decVals.length);
        assertEquals(new BigDecimal("1.23"), decVals[0]);
        assertNull(decVals[1]);
        assertEquals(BigDecimal.valueOf(key * 100L, 2), decVals[2]);

        // DATE[] -> java.sql.Date[]
        Object dateObj = rr.getArrayData("date_arr");
        assertTrue(dateObj instanceof java.sql.Date[]);
        java.sql.Date[] dateVals = (java.sql.Date[]) dateObj;
        assertEquals(3, dateVals.length);
        assertEquals(DateUtil.epochDaysToSqlDate(key), dateVals[0]);
        assertNull(dateVals[1]);
        assertEquals(DateUtil.epochDaysToSqlDate(key + 5), dateVals[2]);

        // TIMESTAMP[] -> java.sql.Timestamp[]
        Object tsObj = rr.getArrayData("ts_arr");
        assertTrue(tsObj instanceof java.sql.Timestamp[]);
        java.sql.Timestamp[] tsVals = (java.sql.Timestamp[]) tsObj;
        assertEquals(3, tsVals.length);
        assertEquals(TimestampUtil.microsToTimestamp(key * 1000L), tsVals[0]);
        assertNull(tsVals[1]);
        assertEquals(TimestampUtil.microsToTimestamp((key + 1) * 1000L), tsVals[2]);

        // VARCHAR[] -> String[]
        Object vchObj = rr.getArrayData("varchar_arr");
        assertTrue(vchObj instanceof String[]);
        String[] vchVals = (String[]) vchObj;
        assertEquals(3, vchVals.length);
        assertEquals("row" + key, vchVals[0]);
        assertNull(vchVals[1]);
        assertEquals("varchar" + key, vchVals[2]);
      }

    }
  }

  @Test
  public void testArrayEmptyValidityOptimization() throws Exception {
    Schema schema = getSchemaWithArrayTypes();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    final KuduSession session = client.newSession();

    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("key", 100);

    // INT8[]
    row.addArrayInt8("int8_arr", new byte[]{1, 2, 3}, new boolean[0]);

    // DECIMAL[]
    row.addArrayDecimal("decimal_arr",
        new BigDecimal[]{new BigDecimal("1.23"), new BigDecimal("4.56"), new BigDecimal("7.89")},
        new boolean[0]);  // all valid

    // DATE[]
    row.addArrayDate("date_arr", new Date[]{
        DateUtil.epochDaysToSqlDate(1),
        DateUtil.epochDaysToSqlDate(2),
        DateUtil.epochDaysToSqlDate(3)
    }, new boolean[0]);

    // TIMESTAMP[]
    row.addArrayTimestamp("ts_arr", new Timestamp[]{
        TimestampUtil.microsToTimestamp(1000L),
        TimestampUtil.microsToTimestamp(2000L),
        TimestampUtil.microsToTimestamp(3000L)
    }, new boolean[0]);

    // VARCHAR[]
    row.addArrayVarchar("varchar_arr", new String[]{"x", "y", "z"}, new boolean[0]);

    // Optional nullable ones can be left null
    row.setNull("bool_arr");
    row.setNull("binary_arr");
    row.setNull("int32_arr");
    row.setNull("string_arr");

    session.apply(insert);

    // Insert second row with null validity vector
    Insert insert2 = table.newInsert();
    PartialRow row2 = insert2.getRow();
    row2.addInt("key", 200);
    row2.addArrayInt8("int8_arr", new byte[]{1, 2, 3});
    row2.addArrayDecimal("decimal_arr",
        new BigDecimal[]{new BigDecimal("1.23"), null, new BigDecimal("7.89")},
        null);
    row2.addArrayDate("date_arr", new Date[]{
        DateUtil.epochDaysToSqlDate(10),
        DateUtil.epochDaysToSqlDate(11),
        DateUtil.epochDaysToSqlDate(12)
    });
    row2.addArrayTimestamp("ts_arr", new Timestamp[]{
        TimestampUtil.microsToTimestamp(10_000L),
        TimestampUtil.microsToTimestamp(20_000L),
        TimestampUtil.microsToTimestamp(30_000L)
    });
    row2.addArrayVarchar("varchar_arr", new String[]{"a", null, "c"});
    row2.setNull("bool_arr");
    row2.setNull("binary_arr");
    row2.setNull("int32_arr");
    row2.setNull("string_arr");

    session.apply(insert2);
    session.flush();

    // --- Verify readback ---
    KuduScanner scanner = client.newScannerBuilder(table).build();
    List<RowResult> rows = new ArrayList<>();
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      while (it.hasNext()) {
        rows.add(it.next());
      }
    }
    assertEquals(2, rows.size());
    RowResult r1 = rows.get(0);

    // INT8[]
    Byte[] int8Vals = (Byte[]) r1.getArrayData("int8_arr");
    assertArrayEquals(new Byte[]{1, 2, 3}, int8Vals);

    // DECIMAL[]
    BigDecimal[] decVals = (BigDecimal[]) r1.getArrayData("decimal_arr");
    assertArrayEquals(
        new BigDecimal[]{new BigDecimal("1.23"), new BigDecimal("4.56"), new BigDecimal("7.89")},
        decVals);

    // DATE[]
    Date[] dateVals = (Date[]) r1.getArrayData("date_arr");
    assertEquals(DateUtil.epochDaysToSqlDate(1), dateVals[0]);
    assertEquals(DateUtil.epochDaysToSqlDate(3), dateVals[2]);

    // TIMESTAMP[]
    Timestamp[] tsVals = (Timestamp[]) r1.getArrayData("ts_arr");
    assertEquals(TimestampUtil.microsToTimestamp(1000L), tsVals[0]);
    assertEquals(TimestampUtil.microsToTimestamp(3000L), tsVals[2]);

    // VARCHAR[]
    String[] vcharVals = (String[]) r1.getArrayData("varchar_arr");
    assertArrayEquals(new String[]{"x", "y", "z"}, vcharVals);

    RowResult r2 = rows.get(1);

    // INT8[]
    Byte[] int8Vals2 = (Byte[]) r2.getArrayData("int8_arr");
    assertArrayEquals(new Byte[]{1, 2, 3}, int8Vals2);

    // DECIMAL[]
    BigDecimal[] decVals2 = (BigDecimal[]) r2.getArrayData("decimal_arr");
    assertArrayEquals(
        new BigDecimal[]{new BigDecimal("1.23"), null, new BigDecimal("7.89")},
        decVals2);

    // DATE[]
    Date[] dateVals2 = (Date[]) r2.getArrayData("date_arr");
    assertEquals(DateUtil.epochDaysToSqlDate(10), dateVals2[0]);
    assertEquals(DateUtil.epochDaysToSqlDate(12), dateVals2[2]);

    // TIMESTAMP[]
    Timestamp[] tsVals2 = (Timestamp[]) r2.getArrayData("ts_arr");
    assertEquals(TimestampUtil.microsToTimestamp(10_000L), tsVals2[0]);
    assertEquals(TimestampUtil.microsToTimestamp(30_000L), tsVals2[2]);

    // VARCHAR[]
    String[] vcharVals2 = (String[]) r2.getArrayData("varchar_arr");
    assertArrayEquals(new String[]{"a", null, "c"}, vcharVals2);
  }
}
