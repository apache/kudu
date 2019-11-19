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

import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.apache.kudu.test.KuduTestHarness.DEFAULT_SLEEP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;

public class TestKuduTable {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTable.class);

  private static final Schema BASIC_SCHEMA = getBasicSchema();
  private static final String tableName = "TestKuduTable";

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
  public void testAlterColumn() throws Exception {
    // Used a simplified schema because BASIC_SCHEMA has extra columns that make the asserts
    // verbose.
    List<ColumnSchema> columns = ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
            .nullable(true)
            .desiredBlockSize(4096)
            .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
            .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION)
            .build());
    KuduTable table = client.createTable(tableName, new Schema(columns), getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    // Insert a row before a default is defined and check the value is NULL.
    insertDefaultRow(table, session, 0);
    //ClientTestUtil.scanTa
    List<String> rows = scanTableToStrings(table);
    assertEquals("wrong number of rows", 1, rows.size());
    assertEquals("wrong row", "INT32 key=0, STRING value=NULL", rows.get(0));

    // Add a default, checking new rows see the new default and old rows remain the same.
    client.alterTable(tableName, new AlterTableOptions().changeDefault("value", "pizza"));

    insertDefaultRow(table, session, 1);
    rows = scanTableToStrings(table);
    assertEquals("wrong number of rows", 2, rows.size());
    assertEquals("wrong row", "INT32 key=0, STRING value=NULL", rows.get(0));
    assertEquals("wrong row", "INT32 key=1, STRING value=pizza", rows.get(1));

    // Change the default, checking new rows see the new default and old rows remain the same.
    client.alterTable(tableName, new AlterTableOptions().changeDefault("value", "taco"));

    insertDefaultRow(table, session, 2);

    rows = scanTableToStrings(table);
    assertEquals("wrong number of rows", 3, rows.size());
    assertEquals("wrong row", "INT32 key=0, STRING value=NULL", rows.get(0));
    assertEquals("wrong row", "INT32 key=1, STRING value=pizza", rows.get(1));
    assertEquals("wrong row", "INT32 key=2, STRING value=taco", rows.get(2));

    // Remove the default, checking that new rows default to NULL and old rows remain the same.
    client.alterTable(tableName, new AlterTableOptions().removeDefault("value"));

    insertDefaultRow(table, session, 3);

    rows = scanTableToStrings(table);
    assertEquals("wrong number of rows", 4, rows.size());
    assertEquals("wrong row", "INT32 key=0, STRING value=NULL", rows.get(0));
    assertEquals("wrong row", "INT32 key=1, STRING value=pizza", rows.get(1));
    assertEquals("wrong row", "INT32 key=2, STRING value=taco", rows.get(2));
    assertEquals("wrong row", "INT32 key=3, STRING value=NULL", rows.get(3));

    // Change the column storage attributes.
    assertEquals("wrong block size",
        4096,
        table.getSchema().getColumn("value").getDesiredBlockSize());
    assertEquals("wrong encoding",
        ColumnSchema.Encoding.PLAIN_ENCODING,
        table.getSchema().getColumn("value").getEncoding());
    assertEquals("wrong compression algorithm",
        ColumnSchema.CompressionAlgorithm.NO_COMPRESSION,
        table.getSchema().getColumn("value").getCompressionAlgorithm());

    client.alterTable(tableName, new AlterTableOptions()
        .changeDesiredBlockSize("value", 8192)
        .changeEncoding("value", ColumnSchema.Encoding.DICT_ENCODING)
        .changeCompressionAlgorithm("value", ColumnSchema.CompressionAlgorithm.SNAPPY));

    KuduTable reopenedTable = client.openTable(tableName);
    assertEquals("wrong block size post alter",
        8192,
        reopenedTable.getSchema().getColumn("value").getDesiredBlockSize());
    assertEquals("wrong encoding post alter",
        ColumnSchema.Encoding.DICT_ENCODING,
        reopenedTable.getSchema().getColumn("value").getEncoding());
    assertEquals("wrong compression algorithm post alter",
        ColumnSchema.CompressionAlgorithm.SNAPPY,
        reopenedTable.getSchema().getColumn("value").getCompressionAlgorithm());
  }

  private void insertDefaultRow(KuduTable table, KuduSession session, int key)
      throws Exception {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt("key", key);
    // Omit value.
    session.apply(insert);
  }

  @Test(timeout = 100000)
  public void testAlterTable() throws Exception {
    client.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    try {

      // Add a col.
      client.alterTable(tableName,
          new AlterTableOptions().addColumn("testaddint", Type.INT32, 4));

      // Rename that col.
      client.alterTable(tableName,
          new AlterTableOptions().renameColumn("testaddint", "newtestaddint"));

      // Delete it.
      client.alterTable(tableName, new AlterTableOptions().dropColumn("newtestaddint"));

      String newTableName = tableName +"new";

      // Rename our table.
      client.alterTable(tableName, new AlterTableOptions().renameTable(newTableName));

      // Rename it back.
      client.alterTable(newTableName, new AlterTableOptions().renameTable(tableName));

      // Add 3 columns, where one has default value, nullable and Timestamp with default value
      client.alterTable(tableName, new AlterTableOptions()
          .addColumn("testaddmulticolnotnull", Type.INT32, 4)
          .addNullableColumn("testaddmulticolnull", Type.STRING)
          .addColumn("testaddmulticolTimestampcol", Type.UNIXTIME_MICROS,
              (System.currentTimeMillis() * 1000)));

      // Try altering a table that doesn't exist.
      String nonExistingTableName = "table_does_not_exist";
      try {
        client.alterTable(nonExistingTableName, new AlterTableOptions());
        fail("Shouldn't be able to alter a table that doesn't exist");
      } catch (KuduException ex) {
        assertTrue(ex.getStatus().isNotFound());
      }

      try {
        client.isAlterTableDone(nonExistingTableName);
        fail("Shouldn't be able to query if an alter table is done here");
      } catch (KuduException ex) {
        assertTrue(ex.getStatus().isNotFound());
      }
    } finally {
      // Normally Java tests accumulate tables without issue, deleting them all
      // when shutting down the mini cluster at the end of every test class.
      // However, testGetLocations below expects a certain table count, so
      // we'll delete our table to ensure there's no interaction between them.
      client.deleteTable(tableName);
    }
  }

  /**
   * Test creating tables of different sizes and see that we get the correct number of tablets back
   * @throws Exception
   */
  @Test
  public void testGetLocations() throws Exception {
    int initialTableCount = asyncClient.getTablesList().join(DEFAULT_SLEEP).getTablesList().size();

    final String NON_EXISTENT_TABLE = "NON_EXISTENT_TABLE";

    // Test a non-existing table
    try {
      client.openTable(NON_EXISTENT_TABLE);
      fail("Should receive an exception since the table doesn't exist");
    } catch (Exception ex) {
      // expected
    }
    // Test with defaults
    String tableWithDefault = tableName + "-WithDefault";
    CreateTableOptions builder = getBasicCreateTableOptions();
    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(BASIC_SCHEMA.getColumnCount());
    int defaultInt = 30;
    String defaultString = "data";
    for (ColumnSchema columnSchema : BASIC_SCHEMA.getColumns()) {

      Object defaultValue;

      if (columnSchema.getType() == Type.INT32) {
        defaultValue = defaultInt;
      } else if (columnSchema.getType() == Type.BOOL) {
        defaultValue = true;
      } else {
        defaultValue = defaultString;
      }
      columns.add(
          new ColumnSchema.ColumnSchemaBuilder(columnSchema.getName(), columnSchema.getType())
              .key(columnSchema.isKey())
              .nullable(columnSchema.isNullable())
              .defaultValue(defaultValue).build());
    }
    Schema schemaWithDefault = new Schema(columns);
    KuduTable kuduTable = client.createTable(tableWithDefault, schemaWithDefault, builder);
    assertEquals(defaultInt, kuduTable.getSchema().getColumnByIndex(0).getDefaultValue());
    assertEquals(defaultString,
        kuduTable.getSchema().getColumnByIndex(columns.size() - 2).getDefaultValue());
    assertEquals(true,
            kuduTable.getSchema().getColumnByIndex(columns.size() - 1).getDefaultValue());

    // Make sure the table's schema includes column IDs.
    assertTrue(kuduTable.getSchema().hasColumnIds());

    // Test we can open a table that was already created.
    client.openTable(tableWithDefault);

    String splitTablePrefix = tableName + "-Splits";
    // Test splitting and reading those splits
    KuduTable kuduTableWithoutDefaults = createTableWithSplitsAndTest(splitTablePrefix, 0);
    // finish testing read defaults
    assertNull(kuduTableWithoutDefaults.getSchema().getColumnByIndex(0).getDefaultValue());
    createTableWithSplitsAndTest(splitTablePrefix, 3);
    createTableWithSplitsAndTest(splitTablePrefix, 10);

    KuduTable table = createTableWithSplitsAndTest(splitTablePrefix, 30);

    List<LocatedTablet>tablets = table.getTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9, table.asyncGetTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(0), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9, table.asyncGetTabletsLocations(getKeyInBytes(0), getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(4, tablets.size());
    assertEquals(4, table.asyncGetTabletsLocations(getKeyInBytes(5), getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(14), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9, table.asyncGetTabletsLocations(getKeyInBytes(5), getKeyInBytes(14), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(31), DEFAULT_SLEEP);
    assertEquals(26, tablets.size());
    assertEquals(26, table.asyncGetTabletsLocations(getKeyInBytes(5), getKeyInBytes(31), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), null, DEFAULT_SLEEP);
    assertEquals(26, tablets.size());
    assertEquals(26, table.asyncGetTabletsLocations(getKeyInBytes(5), null, DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(null, getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(31, tablets.size());
    assertEquals(31, table.asyncGetTabletsLocations(null, getKeyInBytes(10000), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(20), getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(11, tablets.size());
    assertEquals(11, table.asyncGetTabletsLocations(getKeyInBytes(20), getKeyInBytes(10000), DEFAULT_SLEEP).join().size());

    // Test listing tables.
    assertEquals(0, asyncClient.getTablesList(NON_EXISTENT_TABLE)
        .join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(1, asyncClient.getTablesList(tableWithDefault)
        .join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(initialTableCount + 5,
        asyncClient.getTablesList().join(DEFAULT_SLEEP).getTablesList().size());
    assertFalse(asyncClient.getTablesList(tableWithDefault).
        join(DEFAULT_SLEEP).getTablesList().isEmpty());

    assertFalse(asyncClient.tableExists(NON_EXISTENT_TABLE).join(DEFAULT_SLEEP));
    assertTrue(asyncClient.tableExists(tableWithDefault).join(DEFAULT_SLEEP));
  }

  @Test(timeout = 100000)
  public void testLocateTableNonCoveringRange() throws Exception {
    client.createTable(tableName, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = client.openTable(tableName);

    List<LocatedTablet> tablets;

    // all tablets
    tablets = table.getTabletsLocations(null, null, 100000);
    assertEquals(3, tablets.size());
    assertArrayEquals(getKeyInBytes(0), tablets.get(0).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(50), tablets.get(0).getPartition().getPartitionKeyEnd());
    assertArrayEquals(getKeyInBytes(50), tablets.get(1).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(100), tablets.get(1).getPartition().getPartitionKeyEnd());
    assertArrayEquals(getKeyInBytes(200), tablets.get(2).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(300), tablets.get(2).getPartition().getPartitionKeyEnd());

    // key < 50
    tablets = table.getTabletsLocations(null, getKeyInBytes(50), 100000);
    assertEquals(1, tablets.size());
    assertArrayEquals(getKeyInBytes(0), tablets.get(0).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(50), tablets.get(0).getPartition().getPartitionKeyEnd());

    // key >= 300
    tablets = table.getTabletsLocations(getKeyInBytes(300), null, 100000);
    assertEquals(0, tablets.size());

    // key >= 299
    tablets = table.getTabletsLocations(getKeyInBytes(299), null, 100000);
    assertEquals(1, tablets.size());
    assertArrayEquals(getKeyInBytes(200), tablets.get(0).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(300), tablets.get(0).getPartition().getPartitionKeyEnd());

    // key >= 150 && key < 250
    tablets = table.getTabletsLocations(getKeyInBytes(150), getKeyInBytes(250), 100000);
    assertEquals(1, tablets.size());
    assertArrayEquals(getKeyInBytes(200), tablets.get(0).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(300), tablets.get(0).getPartition().getPartitionKeyEnd());
  }

  public byte[] getKeyInBytes(int i) {
    PartialRow row = BASIC_SCHEMA.newPartialRow();
    row.addInt(0, i);
    return row.encodePrimaryKey();
  }

  @Test(timeout = 100000)
  public void testAlterTableNonCoveringRange() throws Exception {
    client.createTable(tableName, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = client.openTable(tableName);
    KuduSession session = client.newSession();

    AlterTableOptions ato = new AlterTableOptions();
    PartialRow bLowerBound = BASIC_SCHEMA.newPartialRow();
    bLowerBound.addInt("key", 300);
    PartialRow bUpperBound = BASIC_SCHEMA.newPartialRow();
    bUpperBound.addInt("key", 400);
    ato.addRangePartition(bLowerBound, bUpperBound);
    client.alterTable(tableName, ato);

    Insert insert = createBasicSchemaInsert(table, 301);
    session.apply(insert);

    List<LocatedTablet> tablets;

    // all tablets
    tablets = table.getTabletsLocations(getKeyInBytes(300), null, 100000);
    assertEquals(1, tablets.size());
    assertArrayEquals(getKeyInBytes(300), tablets.get(0).getPartition().getPartitionKeyStart());
    assertArrayEquals(getKeyInBytes(400), tablets.get(0).getPartition().getPartitionKeyEnd());

    insert = createBasicSchemaInsert(table, 201);
    session.apply(insert);

    ato = new AlterTableOptions();
    bLowerBound = BASIC_SCHEMA.newPartialRow();
    bLowerBound.addInt("key", 200);
    bUpperBound = BASIC_SCHEMA.newPartialRow();
    bUpperBound.addInt("key", 300);
    ato.dropRangePartition(bLowerBound, bUpperBound);
    client.alterTable(tableName, ato);

    insert = createBasicSchemaInsert(table, 202);
    OperationResponse response = session.apply(insert);
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isNotFound());
  }

  @Test(timeout = 100000)
  public void testFormatRangePartitions() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    List<String> expected = Lists.newArrayList();

    {
      expected.add("VALUES < -300");
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, -300);
      builder.addRangePartition(basicSchema.newPartialRow(), upper);
    }
    {
      expected.add("-100 <= VALUES < 0");
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, -100);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 0);
      builder.addRangePartition(lower, upper);
    }
    {
      expected.add("0 <= VALUES < 100");
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, -1);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 99);
      builder.addRangePartition(lower, upper,
                                RangePartitionBound.EXCLUSIVE_BOUND,
                                RangePartitionBound.INCLUSIVE_BOUND);
    }
    {
      expected.add("VALUE = 300");
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 300);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 300);
      builder.addRangePartition(lower, upper,
                                RangePartitionBound.INCLUSIVE_BOUND,
                                RangePartitionBound.INCLUSIVE_BOUND);
    }
    {
      expected.add("VALUES >= 400");
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 400);
      builder.addRangePartition(lower, basicSchema.newPartialRow());
    }

    client.createTable(tableName, basicSchema, builder);
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));
  }

  @Test(timeout = 100000)
  public void testFormatRangePartitionsCompoundColumns() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build());
    Schema schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions();
    builder.addHashPartitions(ImmutableList.of("a"), 2);
    builder.addHashPartitions(ImmutableList.of("b"), 2);
    builder.setRangePartitionColumns(ImmutableList.of("a", "b"));
    List<String> expected = Lists.newArrayList();

    {
      expected.add("VALUES < (\"\", -100)");
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "");
      upper.addByte(1, (byte) -100);
      builder.addRangePartition(schema.newPartialRow(), upper);
    }
    {
      expected.add("VALUE = (\"abc\", 0)");
      PartialRow lower = schema.newPartialRow();
      lower.addString(0, "abc");
      lower.addByte(1, (byte) 0);
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "abc");
      upper.addByte(1, (byte) 1);
      builder.addRangePartition(lower, upper);
    }
    {
      expected.add("(\"def\", 0) <= VALUES < (\"ghi\", 100)");
      PartialRow lower = schema.newPartialRow();
      lower.addString(0, "def");
      lower.addByte(1, (byte) -1);
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "ghi");
      upper.addByte(1, (byte) 99);
      builder.addRangePartition(lower, upper,
                                RangePartitionBound.EXCLUSIVE_BOUND,
                                RangePartitionBound.INCLUSIVE_BOUND);
    }

    client.createTable(tableName, schema, builder);
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));
  }

  @Test(timeout = 100000)
  public void testFormatRangePartitionsStringColumn() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).key(true).build());
    Schema schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions();
    builder.setRangePartitionColumns(ImmutableList.of("a"));
    List<String> expected = Lists.newArrayList();

    {
      expected.add("VALUES < \"\\0\"");
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "\0");
      builder.addRangePartition(schema.newPartialRow(), upper);
    }
    {
      expected.add("VALUE = \"abc\"");
      PartialRow lower = schema.newPartialRow();
      lower.addString(0, "abc");
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "abc\0");
      builder.addRangePartition(lower, upper);
    }
    {
      expected.add("\"def\" <= VALUES < \"ghi\"");
      PartialRow lower = schema.newPartialRow();
      lower.addString(0, "def");
      PartialRow upper = schema.newPartialRow();
      upper.addString(0, "ghi");
      builder.addRangePartition(lower, upper);
    }
    {
      expected.add("VALUES >= \"z\"");
      PartialRow lower = schema.newPartialRow();
      lower.addString(0, "z");
      builder.addRangePartition(lower, schema.newPartialRow());
    }

    client.createTable(tableName, schema, builder);
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));
  }

  @Test(timeout = 100000)
  public void testFormatRangePartitionsUnbounded() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    client.createTable(tableName, basicSchema, builder);

    assertEquals(
        ImmutableList.of("UNBOUNDED"),
        client.openTable(tableName).getFormattedRangePartitions(10000));
  }

  private KuduTable createTableWithSplitsAndTest(String tableNamePrefix, int splitsCount)
      throws Exception {
    String newTableName = tableNamePrefix + "-" + splitsCount;
    CreateTableOptions builder = getBasicCreateTableOptions();

    if (splitsCount != 0) {
      for (int i = 1; i <= splitsCount; i++) {
        PartialRow row = BASIC_SCHEMA.newPartialRow();
        row.addInt(0, i);
        builder.addSplitRow(row);
      }
    }
    KuduTable table = client.createTable(newTableName, BASIC_SCHEMA, builder);

    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(splitsCount + 1, tablets.size());
    assertEquals(splitsCount + 1, table.asyncGetTabletsLocations(DEFAULT_SLEEP).join().size());
    for (LocatedTablet tablet : tablets) {
      assertEquals(3, tablet.getReplicas().size());
    }
    return table;
  }

  @Test(timeout = 100000)
  public void testGetRangePartitions() throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.INT8).key(true).build());
    Schema schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions();
    builder.addHashPartitions(ImmutableList.of("a"), 2);
    builder.addHashPartitions(ImmutableList.of("b"), 2);
    builder.setRangePartitionColumns(ImmutableList.of("a", "b"));

    PartialRow bottom = schema.newPartialRow();
    PartialRow middle = schema.newPartialRow();
    middle.addString("a", "");
    middle.addByte("b", (byte) -100);
    PartialRow upper = schema.newPartialRow();

    builder.addRangePartition(bottom, middle);
    builder.addRangePartition(middle, upper);

    KuduTable table = client.createTable(tableName, schema, builder);

    List<Partition> rangePartitions =
        table.getRangePartitions(client.getDefaultOperationTimeoutMs());
    assertEquals(rangePartitions.size(), 2);

    Partition lowerPartition = rangePartitions.get(0);
    assertEquals(0, lowerPartition.getRangeKeyStart().length);
    assertTrue(lowerPartition.getRangeKeyEnd().length > 0);
    PartialRow decodedLower = lowerPartition.getDecodedRangeKeyEnd(table);
    assertEquals("", decodedLower.getString("a"));
    assertEquals((byte) -100, decodedLower.getByte("b"));

    Partition upperPartition = rangePartitions.get(1);
    assertTrue(upperPartition.getRangeKeyStart().length > 0);
    assertEquals(0, upperPartition.getRangeKeyEnd().length);
    PartialRow decodedUpper = upperPartition.getDecodedRangeKeyStart(table);
    assertEquals("", decodedUpper.getString("a"));
    assertEquals((byte) -100, decodedUpper.getByte("b"));
  }

  @Test(timeout = 100000)
  public void testGetRangePartitionsUnbounded() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();
    KuduTable table = client.createTable(tableName, BASIC_SCHEMA, builder);

    List<Partition> rangePartitions =
        table.getRangePartitions(client.getDefaultOperationTimeoutMs());
    assertEquals(rangePartitions.size(), 1);
    Partition partition = rangePartitions.get(0);
    assertEquals(0, partition.getRangeKeyStart().length);
    assertEquals(0, partition.getRangeKeyEnd().length);
  }

  @Test(timeout = 100000)
  public void testAlterNoWait() throws Exception {
    client.createTable(tableName, basicSchema, getBasicCreateTableOptions());

    String oldName = "column2_i";
    for (int i = 0; i < 10; i++) {
      String newName = String.format("foo%d", i);
      client.alterTable(tableName, new AlterTableOptions()
          .renameColumn(oldName, newName)
          .setWait(false));

      // We didn't wait for the column rename to finish, so we should be able
      // to still see 'oldName' and not yet see 'newName'. However, this is
      // timing dependent: if the alter finishes before we reload the schema,
      // loop and try again.
      KuduTable table = client.openTable(tableName);
      try {
        table.getSchema().getColumn(oldName);
      } catch (IllegalArgumentException e) {
        LOG.info("Alter finished too quickly (old column name {} is already " +
            "gone), trying again", oldName);
        oldName = newName;
        continue;
      }
      try {
        table.getSchema().getColumn(newName);
        fail(String.format("New column name %s should not yet be visible", newName));
      } catch (IllegalArgumentException e) {}

      // After waiting for the alter to finish and reloading the schema,
      // 'newName' should be visible and 'oldName' should be gone.
      assertTrue(client.isAlterTableDone(tableName));
      table = client.openTable(tableName);
      try {
        table.getSchema().getColumn(oldName);
        fail(String.format("Old column name %s should not be visible", oldName));
      } catch (IllegalArgumentException e) {}
      table.getSchema().getColumn(newName);
      LOG.info("Test passed on attempt {}", i + 1);
      return;
    }
    fail("Could not run test even after multiple attempts");
  }

  @Test(timeout = 100000)
  public void testNumReplicas() throws Exception {
    for (int i = 1; i <= 3; i++) {
      // Ignore even numbers.
      if (i % 2 != 0) {
        String tableName = "testNumReplicas" + "-" + i;
        CreateTableOptions options = getBasicCreateTableOptions();
        options.setNumReplicas(i);
        client.createTable(tableName, basicSchema, options);
        KuduTable table = client.openTable(tableName);
        assertEquals(i, table.getNumReplicas());
      }
    }
  }

  @Test(timeout = 100000)
  public void testAlterColumnComment() throws Exception {
    // Schema with comments.
    List<ColumnSchema> columns = ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
            .key(true).comment("keytest").build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
            .comment("valuetest").build());
    // Create a table.
    KuduTable table = client.createTable(tableName,
        new Schema(columns), getBasicCreateTableOptions());

    // Verify the comments after creating.
    assertEquals("wrong key comment", "keytest",
        table.getSchema().getColumn("key").getComment());
    assertEquals("wrong value comment", "valuetest",
        table.getSchema().getColumn("value").getComment());

    // Change the comments.
    client.alterTable(tableName,
        new AlterTableOptions().changeComment("key", "keycomment"));
    client.alterTable(tableName,
        new AlterTableOptions().changeComment("value", "valuecomment"));

    // Verify the comments after the first change.
    KuduTable table1 = client.openTable(tableName);
    assertEquals("wrong key comment post alter",
        "keycomment", table1.getSchema().getColumn("key").getComment());
    assertEquals("wrong value comment post alter",
        "valuecomment", table1.getSchema().getColumn("value").getComment());

    // Delete the comments.
    client.alterTable(tableName,
        new AlterTableOptions().changeComment("key", ""));
    client.alterTable(tableName,
        new AlterTableOptions().changeComment("value", ""));

    // Verify the comments after the second change.
    KuduTable table2 = client.openTable(tableName);
    assertEquals("wrong key comment post alter", "",
        table2.getSchema().getColumn("key").getComment());
    assertEquals("wrong value comment post alter", "",
        table2.getSchema().getColumn("value").getComment());
  }

  @Test(timeout = 100000)
  public void testDimensionLabel() throws Exception {
    // Create a table with dimension label.
    KuduTable table = client.createTable(tableName, basicSchema,
        getBasicTableOptionsWithNonCoveredRange().setDimensionLabel("labelA"));

    // Add a range partition to the table with dimension label.
    AlterTableOptions ato = new AlterTableOptions();
    PartialRow bLowerBound = BASIC_SCHEMA.newPartialRow();
    bLowerBound.addInt("key", 300);
    PartialRow bUpperBound = BASIC_SCHEMA.newPartialRow();
    bUpperBound.addInt("key", 400);
    ato.addRangePartition(bLowerBound, bUpperBound, "labelB",
                          RangePartitionBound.INCLUSIVE_BOUND,
                          RangePartitionBound.EXCLUSIVE_BOUND);
    client.alterTable(tableName, ato);

    Map<String, Integer> dimensionMap = new HashMap<>();
    for (LocatedTablet tablet : table.getTabletsLocations(DEFAULT_SLEEP)) {
      for (LocatedTablet.Replica replica : tablet.getReplicas()) {
        Integer number = dimensionMap.get(replica.getDimensionLabel());
        if (number == null) {
          number = 0;
        }
        dimensionMap.put(replica.getDimensionLabel(), number + 1);
      }
    }
    assertEquals(9, dimensionMap.get("labelA").intValue());
    assertEquals(3, dimensionMap.get("labelB").intValue());
  }

  @Test(timeout = 100000)
  @KuduTestHarness.TabletServerConfig(flags = {
      "--update_tablet_stats_interval_ms=200",
      "--heartbeat_interval_ms=100",
  })
  public void testGetTableStatistics() throws Exception {
    // Create a table.
    CreateTableOptions builder = getBasicCreateTableOptions();
    KuduTable table = client.createTable(tableName, BASIC_SCHEMA, builder);

    // Insert some rows and test the statistics.
    KuduTableStatistics prevStatistics = new KuduTableStatistics(-1, -1);
    KuduTableStatistics currentStatistics;
    KuduSession session = client.newSession();
    int num = 100;
    for (int i = 0; i < num; ++i) {
      // Get current table statistics.
      currentStatistics = table.getTableStatistics();
      assertTrue(currentStatistics.getOnDiskSize() >= prevStatistics.getOnDiskSize());
      assertTrue(currentStatistics.getLiveRowCount() >= prevStatistics.getLiveRowCount());
      assertTrue(currentStatistics.getLiveRowCount() <= i + 1);
      prevStatistics = currentStatistics;
      // Insert row.
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
      List<String> rows = scanTableToStrings(table);
      assertEquals("wrong number of rows", i + 1, rows.size());
    }

    // Final accuracy test.
    // Wait for master to aggregate table statistics.
    Thread.sleep(200 * 6);
    currentStatistics = table.getTableStatistics();
    assertTrue(currentStatistics.getOnDiskSize() >= prevStatistics.getOnDiskSize());
    assertTrue(currentStatistics.getLiveRowCount() >= prevStatistics.getLiveRowCount());
    assertTrue(currentStatistics.getLiveRowCount() == num);
  }
}
