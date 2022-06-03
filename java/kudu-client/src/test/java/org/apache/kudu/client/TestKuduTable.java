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

import static org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    KuduTable table =
        client.createTable(tableName, new Schema(columns), getBasicCreateTableOptions());
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

      String newTableName = tableName + "new";

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
   * Test creating tables of different sizes and see that we get the correct number of tablets back.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testGetLocations() throws Exception {
    final int initialTableCount =
        asyncClient.getTablesList().join(DEFAULT_SLEEP).getTablesList().size();

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
    List<ColumnSchema> columns = new ArrayList<>(BASIC_SCHEMA.getColumnCount());
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

    List<LocatedTablet> tablets =
        table.getTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9,
        table.asyncGetTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(0), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9,
        table.asyncGetTabletsLocations(getKeyInBytes(0),
            getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(4, tablets.size());
    assertEquals(4,
        table.asyncGetTabletsLocations(getKeyInBytes(5),
            getKeyInBytes(9), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(14), DEFAULT_SLEEP);
    assertEquals(9, tablets.size());
    assertEquals(9,
        table.asyncGetTabletsLocations(getKeyInBytes(5),
            getKeyInBytes(14), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(31), DEFAULT_SLEEP);
    assertEquals(26, tablets.size());
    assertEquals(26,
        table.asyncGetTabletsLocations(getKeyInBytes(5),
            getKeyInBytes(31), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), null, DEFAULT_SLEEP);
    assertEquals(26, tablets.size());
    assertEquals(26,
        table.asyncGetTabletsLocations(getKeyInBytes(5), null, DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(null, getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(31, tablets.size());
    assertEquals(31,
        table.asyncGetTabletsLocations(null,
            getKeyInBytes(10000), DEFAULT_SLEEP).join().size());

    tablets = table.getTabletsLocations(getKeyInBytes(20), getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(11, tablets.size());
    assertEquals(11,
        table.asyncGetTabletsLocations(getKeyInBytes(20),
            getKeyInBytes(10000), DEFAULT_SLEEP).join().size());

    // Test listing tables.
    assertEquals(0, asyncClient.getTablesList(NON_EXISTENT_TABLE)
        .join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(1, asyncClient.getTablesList(tableWithDefault)
        .join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(initialTableCount + 5,
        asyncClient.getTablesList().join(DEFAULT_SLEEP).getTablesList().size());
    assertFalse(asyncClient.getTablesList(tableWithDefault)
        .join(DEFAULT_SLEEP).getTablesList().isEmpty());

    assertFalse(asyncClient.tableExists(NON_EXISTENT_TABLE).join(DEFAULT_SLEEP));
    assertTrue(asyncClient.tableExists(tableWithDefault).join(DEFAULT_SLEEP));
  }

  @Test(timeout = 100000)
  @SuppressWarnings("deprecation")
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
  @SuppressWarnings("deprecation")
  public void testAlterTableNonCoveringRange() throws Exception {
    client.createTable(tableName, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    final KuduTable table = client.openTable(tableName);
    final KuduSession session = client.newSession();

    AlterTableOptions ato = new AlterTableOptions();
    PartialRow lowerBound = BASIC_SCHEMA.newPartialRow();
    lowerBound.addInt("key", 300);
    PartialRow upperBound = BASIC_SCHEMA.newPartialRow();
    upperBound.addInt("key", 400);
    ato.addRangePartition(lowerBound, upperBound);
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
    lowerBound = BASIC_SCHEMA.newPartialRow();
    lowerBound.addInt("key", 200);
    upperBound = BASIC_SCHEMA.newPartialRow();
    upperBound.addInt("key", 300);
    ato.dropRangePartition(lowerBound, upperBound);
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
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testCreateTablePartitionWithEmptyCustomHashSchema() throws Exception {
    PartialRow lower = basicSchema.newPartialRow();
    lower.addInt(0, -100);
    PartialRow upper = basicSchema.newPartialRow();
    upper.addInt(0, 100);

    CreateTableOptions builder = getBasicCreateTableOptions();

    // Using an empty custom hash schema for the range.
    RangePartitionWithCustomHashSchema rangePartition =
        new RangePartitionWithCustomHashSchema(
            lower,
            upper,
            RangePartitionBound.INCLUSIVE_BOUND,
            RangePartitionBound.EXCLUSIVE_BOUND);
    builder.addRangePartition(rangePartition);

    KuduTable table = client.createTable(tableName, basicSchema, builder);

    // Check the result: retrieve the information on tablets from master
    // and check if each partition has the expected parameters.
    {
      for (KuduScanToken token : new KuduScanToken.KuduScanTokenBuilder(asyncClient, table)
          .setTimeout(client.getDefaultOperationTimeoutMs()).build()) {
        Partition p = token.getTablet().getPartition();
        // No hash partitions are expected.
        assertEquals(0, p.getHashBuckets().size());
      }

      final List<Partition> rangePartitions =
          table.getRangePartitions(client.getDefaultOperationTimeoutMs());
      assertEquals(1, rangePartitions.size());
      final Partition p = rangePartitions.get(0);

      assertTrue(p.getRangeKeyStart().length > 0);
      PartialRow rangeKeyStartDecoded = p.getDecodedRangeKeyStart(table);
      assertEquals(-100, rangeKeyStartDecoded.getInt(0));
      assertTrue(p.getRangeKeyEnd().length > 0);
      PartialRow rangeKeyEndDecoded = p.getDecodedRangeKeyEnd(table);
      assertEquals(100, rangeKeyEndDecoded.getInt(0));
    }

    List<String> expected = Lists.newArrayList();
    expected.add("-100 <= VALUES < 100");
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testCreateTablePartitionWithCustomHashSchema() throws Exception {
    PartialRow lower = basicSchema.newPartialRow();
    lower.addInt(0, -100);
    PartialRow upper = basicSchema.newPartialRow();
    upper.addInt(0, 200);

    // Simple custom hash schema for the range: two buckets on the column "key".
    RangePartitionWithCustomHashSchema rangePartition =
        new RangePartitionWithCustomHashSchema(
            lower,
            upper,
            RangePartitionBound.INCLUSIVE_BOUND,
            RangePartitionBound.EXCLUSIVE_BOUND);
    rangePartition.addHashPartitions(ImmutableList.of("key"), 2, 0);

    CreateTableOptions builder = getBasicCreateTableOptions();
    builder.addRangePartition(rangePartition);

    // Add table-wide schema: it should have the same number of dimensions
    // as the range-specific hash schema. However, this schema isn't used
    // in this test scenario.
    builder.addHashPartitions(ImmutableList.of("key"), 7, 0);

    KuduTable table = client.createTable(tableName, basicSchema, builder);

    List<String> expected = Lists.newArrayList();
    expected.add("-100 <= VALUES < 200");
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));

    // Check the result: retrieve the information on tablets from master
    // and check if each partition has expected parameters.
    {
      Set<Integer> buckets = new HashSet();
      for (KuduScanToken token : new KuduScanToken.KuduScanTokenBuilder(asyncClient, table)
          .setTimeout(client.getDefaultOperationTimeoutMs()).build()) {
        Partition p = token.getTablet().getPartition();
        // Two hash partitions are expected per range.
        assertEquals(1, p.getHashBuckets().size());
        for (Integer idx : p.getHashBuckets()) {
          buckets.add(idx);
        }
      }
      assertEquals(2, buckets.size());
      for (int i = 0; i < buckets.size(); ++i) {
        assertTrue(String.format("must have bucket %d", i), buckets.contains(i));
      }

      final List<Partition> rangePartitions =
          table.getRangePartitions(client.getDefaultOperationTimeoutMs());
      assertEquals(1, rangePartitions.size());
      final Partition p = rangePartitions.get(0);

      assertTrue(p.getRangeKeyStart().length > 0);
      PartialRow rangeKeyStartDecoded = p.getDecodedRangeKeyStart(table);
      assertEquals(-100, rangeKeyStartDecoded.getInt(0));
      assertTrue(p.getRangeKeyEnd().length > 0);
      PartialRow rangeKeyEndDecoded = p.getDecodedRangeKeyEnd(table);
      assertEquals(200, rangeKeyEndDecoded.getInt(0));
    }
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testRangePartitionWithCustomHashSchemaBasic() throws Exception {
    final int valLower = 10;
    final int valUpper = 20;

    PartialRow lower = basicSchema.newPartialRow();
    lower.addInt(0, valLower);
    PartialRow upper = basicSchema.newPartialRow();
    upper.addInt(0, valUpper);

    // Simple custom hash schema for the range: five buckets on the column "key".
    RangePartitionWithCustomHashSchema rangePartition =
        new RangePartitionWithCustomHashSchema(
            lower,
            upper,
            RangePartitionBound.INCLUSIVE_BOUND,
            RangePartitionBound.EXCLUSIVE_BOUND);
    rangePartition.addHashPartitions(ImmutableList.of("key"), 5, 0);

    CreateTableOptions builder = getBasicCreateTableOptions();
    builder.addRangePartition(rangePartition);
    // Add table-wide schema: it should have the same number of dimensions
    // as the range-specific hash schema. However, this schema isn't used
    // in this test scenario.
    builder.addHashPartitions(ImmutableList.of("key"), 32, 0);

    final KuduTable table = client.createTable(tableName, basicSchema, builder);
    final PartitionSchema ps = table.getPartitionSchema();
    assertTrue(ps.hasCustomHashSchemas());
    assertFalse(ps.isSimpleRangePartitioning());

    // NOTE: use schema from server since ColumnIDs are needed for row encoding
    final PartialRow rowLower = table.getSchema().newPartialRow();
    rowLower.addInt(0, valLower);

    final PartialRow rowUpper = table.getSchema().newPartialRow();
    rowUpper.addInt(0, valUpper);

    {
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(rowLower, ps.getRangeSchema()));
      // There should be just one dimension with five buckets.
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }
    {
      // There should be 5 partitions: the newly created table has a single
      // range with 5 hash buckets, but KuduTable.getRangePartitions() removes
      // the 'duplicates' with hash code other than 0. So, the result should be
      // just one partition with hash code 0.
      List<Partition> partitions = table.getRangePartitions(50000);
      assertEquals(1, partitions.size());
      List<Integer> buckets = partitions.get(0).getHashBuckets();
      assertEquals(1, buckets.size());  // there is just one hash dimension
      assertEquals(0, buckets.get(0).intValue());
    }
    {
      final byte[] rowLowerEnc = ps.encodePartitionKey(rowLower);
      final byte[] rowUpperEnc = ps.encodePartitionKey(rowUpper);

      // The range part comes after the hash part in an encoded partition key.
      // The hash part contains 4 * number_of_hash_dimensions bytes.
      byte[] hashLower = Arrays.copyOfRange(rowLowerEnc, 4, rowLowerEnc.length);
      byte[] hashUpper = Arrays.copyOfRange(rowUpperEnc, 4, rowUpperEnc.length);

      Set<Integer> buckets = new HashSet();
      for (KuduScanToken token : new KuduScanToken.KuduScanTokenBuilder(asyncClient, table)
          .setTimeout(client.getDefaultOperationTimeoutMs()).build()) {
        final Partition p = token.getTablet().getPartition();
        assertEquals(0, Bytes.memcmp(p.getRangeKeyStart(), hashLower));
        assertEquals(0, Bytes.memcmp(p.getRangeKeyEnd(), hashUpper));
        assertEquals(1, p.getHashBuckets().size());
        buckets.add(p.getHashBuckets().get(0));
      }

      // Check the generated scan tokens cover all the tablets for the range:
      // all hash bucket indices should be present.
      assertEquals(5, buckets.size());
      for (int i = 0; i < buckets.size(); ++i) {
        assertTrue(String.format("must have bucket %d", i), buckets.contains(i));
      }
    }
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testCreateTableCustomHashSchemasTwoRanges() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();

    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 0);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 100);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("key"), 2, 0);
      builder.addRangePartition(rangePartition);
    }

    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 100);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("key"), 3, 0);
      builder.addRangePartition(rangePartition);
    }

    // Add table-wide schema as well -- that's to satisfy the constraint on
    // the number of hash dimensions in table's hash schemas. However, this
    // scenario isn't going to create a range with table-wide hash schema.
    builder.addHashPartitions(ImmutableList.of("key"), 5, 0);

    KuduTable table = client.createTable(tableName, basicSchema, builder);

    // Check the result: retrieve the information on tablets from master
    // and check if each partition has expected parameters.
    List<LocatedTablet> tablets = table.getTabletsLocations(10000);
    // There should be 5 tablets: 2 for [0, 100) range and 3 for [100, 200).
    assertEquals(5, tablets.size());

    List<String> expected = Lists.newArrayList();
    expected.add("0 <= VALUES < 100");
    expected.add("100 <= VALUES < 200");
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));

    // Insert data into the newly created table and read it back.
    KuduSession session = client.newSession();
    for (int key = 0; key < 200; ++key) {
      Insert insert = createBasicSchemaInsert(table, key);
      session.apply(insert);
    }
    session.flush();

    // Do full table scan.
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, -1));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 0));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 1));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 99));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 100));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 101));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 199));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 200));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 201));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 0));
    assertEquals(199, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 100));
    assertEquals(99, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 199));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 200));
    assertEquals(0, rowStrings.size());

    // Predicate to have all rows in the range with table-wide hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 100));
    assertEquals(100, rowStrings.size());

    // Predicate to have all rows in the range with custom hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 100),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(100, rowStrings.size());

    // Predicate to have one part of the rows in the range with table-wide hash
    // schema, and the other part from the range with custom hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 50),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 150));
    assertEquals(100, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 150));
    assertEquals(150, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 1),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 199));
    assertEquals(198, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 1),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(199, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 199));
    assertEquals(199, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 199));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS_EQUAL, 0));
    assertEquals(1, rowStrings.size());

    // Predicate to cover exactly both ranges.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 200));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 0));
    assertEquals(0, rowStrings.size());
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testCreateTableCustomHashSchemasTwoMixedRanges() throws Exception {
    CreateTableOptions builder = getBasicCreateTableOptions();

    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 0);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 100);

      // Simple custom hash schema for the range: two buckets on the column "key".
      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("key"), 2, 0);
      builder.addRangePartition(rangePartition);
    }

    // Add table-wide schema as well.
    builder.addHashPartitions(ImmutableList.of("key"), 5, 0);

    // Add a range to have the table-wide hash schema.
    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 100);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 200);
      builder.addRangePartition(lower, upper);
    }

    KuduTable table = client.createTable(tableName, basicSchema, builder);

    // Check the result: retrieve the information on tablets from master
    // and check if each partition has expected parameters.
    List<LocatedTablet> tablets = table.getTabletsLocations(10000);
    // There should be 7 tablets: 2 for the [0, 100) range and 5 for [100, 200).
    assertEquals(7, tablets.size());

    List<String> expected = Lists.newArrayList();
    expected.add("0 <= VALUES < 100");
    expected.add("100 <= VALUES < 200");
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));

    // Insert data into the newly created table and read it back.
    KuduSession session = client.newSession();
    for (int key = 0; key < 200; ++key) {
      Insert insert = createBasicSchemaInsert(table, key);
      session.apply(insert);
    }
    session.flush();

    // Do full table scan.
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, -1));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 0));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 1));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 99));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 100));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 101));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 199));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 200));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), EQUAL, 201));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 0));
    assertEquals(199, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 100));
    assertEquals(99, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 199));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER, 200));
    assertEquals(0, rowStrings.size());

    // Predicate to have all rows in the range with table-wide hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 100));
    assertEquals(100, rowStrings.size());

    // Predicate to have all rows in the range with custom hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 100),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(100, rowStrings.size());

    // Predicate to have one part of the rows in the range with table-wide hash
    // schema, and the other part from the range with custom hash schema.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 50),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 150));
    assertEquals(100, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 150));
    assertEquals(150, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 1),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 199));
    assertEquals(198, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 1),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(199, rowStrings.size());

    // Predicates to almost cover the both ranges (sort of off-by-one situation).
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 199));
    assertEquals(199, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 199));
    assertEquals(1, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS_EQUAL, 0));
    assertEquals(1, rowStrings.size());

    // Predicate to cover exactly both ranges.
    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0),
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 200));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 0));
    assertEquals(200, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), GREATER_EQUAL, 200));
    assertEquals(0, rowStrings.size());

    rowStrings = scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(basicSchema.getColumn("key"), LESS, 0));
    assertEquals(0, rowStrings.size());
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testCreateTableCustomHashSchemaDifferentDimensions() throws Exception {
    // Have the table-wide hash schema different from custom hash schema per
    // various ranges: it should not be possible to create a table.
    ArrayList<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0i", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1i", Type.INT64).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2s", Type.STRING).key(true).build());
    final Schema schema = new Schema(columns);

    CreateTableOptions builder = new CreateTableOptions().setRangePartitionColumns(
        ImmutableList.of("c0i"));

    // Add table-wide schema with two hash dimensions.
    builder.addHashPartitions(ImmutableList.of("c1i"), 7, 0);
    builder.addHashPartitions(ImmutableList.of("c2s"), 3, 0);

    // Simple custom hash schema for the range: two buckets on the column "key".
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt(0, -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt(0, 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("c0i"), 2, 0);
      builder.addRangePartition(rangePartition);
    }

    try {
      client.createTable(tableName, schema, builder);
      fail("shouldn't be able to create a table with hash schemas varying in " +
          "number of hash dimensions across table partitions");
    } catch (KuduException ex) {
      assertTrue(ex.getStatus().isNotSupported());
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(
          "varying number of hash dimensions per range is not yet supported"));
    }

    // OK, now try a mixed case: one range with hash schema matching the number
    // of hash dimensions of the table-wide hash schema, and a few more ranges
    // with different number of hash dimensions in their hash schema.
    // Simple custom hash schema for the range: two buckets on the column "key".
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt(0, 200);
      PartialRow upper = schema.newPartialRow();
      upper.addInt(0, 300);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("c0i"), 2, 0);
      rangePartition.addHashPartitions(ImmutableList.of("c1i"), 3, 0);
      builder.addRangePartition(rangePartition);
    }

    try {
      client.createTable(tableName, schema, builder);
      fail("shouldn't be able to create a table with hash schemas varying in " +
          "number of hash dimensions across table partitions");
    } catch (KuduException ex) {
      assertTrue(ex.getStatus().isNotSupported());
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, errmsg.matches(
          "varying number of hash dimensions per range is not yet supported"));
    }
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testGetHashSchemaForRange() throws Exception {
    final int valLower = 100;
    final int valUpper = 200;

    PartialRow lower = basicSchema.newPartialRow();
    lower.addInt(0, valLower);
    PartialRow upper = basicSchema.newPartialRow();
    upper.addInt(0, valUpper);

    // Custom hash schema for the range: three buckets on the column "key".
    RangePartitionWithCustomHashSchema rangePartition =
        new RangePartitionWithCustomHashSchema(
            lower,
            upper,
            RangePartitionBound.INCLUSIVE_BOUND,
            RangePartitionBound.EXCLUSIVE_BOUND);
    rangePartition.addHashPartitions(ImmutableList.of("key"), 3, 0);

    CreateTableOptions builder = getBasicCreateTableOptions();
    builder.addRangePartition(rangePartition);

    // Add table-wide schema with one dimensions and five buckets.
    builder.addHashPartitions(ImmutableList.of("key"), 5, 0);

    final KuduTable table = client.createTable(tableName, basicSchema, builder);
    final PartitionSchema ps = table.getPartitionSchema();

    // Should get the table-wide schema as the result when asking for a point
    // in a non-covered range.
    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 99);

      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }

    // The exact range boundary: should get the custom hash schema.
    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 100);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(3, s.get(0).getNumBuckets());
    }

    // A value within the range: should get the custom hash schema.
    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 101);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(3, s.get(0).getNumBuckets());
    }

    // Should get the table-wide schema as the result when asking for the
    // upper exclusive boundary.
    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 200);

      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }

    // Should get the table-wide schema as the result when asking for a point
    // in a non-covered range.
    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 300);

      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      // There should be just one dimension with two buckets.
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testGetHashSchemaForRangeUnbounded() throws Exception {
    // The test table is created with the following ranges:
    //   (-inf, -100) [-100, 0) [0, 100), [100, +inf)

    CreateTableOptions builder = getBasicCreateTableOptions();
    // Add table-wide schema with one dimensions and two buckets.
    builder.addHashPartitions(ImmutableList.of("key"), 2, 0);

    // Add range partition with custom hash schema: (-inf, -100)
    {
      PartialRow lower = basicSchema.newPartialRow();
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, -100);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("key"), 3, 0);

      builder.addRangePartition(rangePartition);
    }

    // Add range partition with table-wide hash schema: [-100, 0)
    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, -100);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 0);

      builder.addRangePartition(lower, upper);
    }

    // Add range partition with custom hash schema: [0, 100)
    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 0);
      PartialRow upper = basicSchema.newPartialRow();
      upper.addInt(0, 100);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("key"), 5, 0);

      builder.addRangePartition(rangePartition);
    }

    // Add range partition with table-wide hash schema: [100, +inf)
    {
      PartialRow lower = basicSchema.newPartialRow();
      lower.addInt(0, 100);
      PartialRow upper = basicSchema.newPartialRow();

      builder.addRangePartition(lower, upper);
    }

    final KuduTable table = client.createTable(tableName, basicSchema, builder);
    final PartitionSchema ps = table.getPartitionSchema();

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, -2002);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(3, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, -101);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(3, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, -100);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(2, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 0);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 99);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(5, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 100);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(2, s.get(0).getNumBuckets());
    }

    {
      PartialRow row = table.getSchema().newPartialRow();
      row.addInt(0, 1001);
      final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
          KeyEncoder.encodeRangePartitionKey(row, ps.getRangeSchema()));
      assertEquals(1, s.size());
      assertEquals(2, s.get(0).getNumBuckets());
    }
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

  @SuppressWarnings("deprecation")
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
      } catch (IllegalArgumentException e) {
        // ignored
      }

      // After waiting for the alter to finish and reloading the schema,
      // 'newName' should be visible and 'oldName' should be gone.
      assertTrue(client.isAlterTableDone(tableName));
      table = client.openTable(tableName);
      try {
        table.getSchema().getColumn(oldName);
        fail(String.format("Old column name %s should not be visible", oldName));
      } catch (IllegalArgumentException e) {
        // ignored
      }
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
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testAlterTableAddRangePartitionCustomHashSchemaOverlapped() throws Exception {
    final List<ColumnSchema> columns = ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());
    final Schema schema = new Schema(columns);

    CreateTableOptions options = getBasicCreateTableOptions();
    // Add table-wide schema for the table.
    options.addHashPartitions(ImmutableList.of("key"), 2, 0);

    client.createTable(tableName, schema, options);

    // Originally, there are no range partitions in the newly created table.
    assertEquals(
        ImmutableList.of("UNBOUNDED"),
        client.openTable(tableName).getFormattedRangePartitions(10000));

    PartialRow lower = schema.newPartialRow();
    lower.addInt(0, -1);
    PartialRow upper = schema.newPartialRow();
    upper.addInt(0, 1);

    RangePartitionWithCustomHashSchema range =
        new RangePartitionWithCustomHashSchema(
            lower,
            upper,
            RangePartitionBound.INCLUSIVE_BOUND,
            RangePartitionBound.EXCLUSIVE_BOUND);
    range.addHashPartitions(ImmutableList.of("key"), 3, 0);

    try {
      client.alterTable(tableName, new AlterTableOptions().addRangePartition(range));
      fail("should not be able to add a partition which overlaps with existing unbounded one");
    } catch (KuduException ex) {
      final String errmsg = ex.getMessage();
      assertTrue(errmsg, ex.getStatus().isInvalidArgument());
      assertTrue(errmsg, errmsg.matches(".*new range partition conflicts with existing one:.*"));
    }
  }

  @Test(timeout = 100000)
  @KuduTestHarness.MasterServerConfig(flags = {
      "--enable_per_range_hash_schemas=true",
  })
  public void testAlterTableAddRangePartitionCustomHashSchema() throws Exception {
    final List<ColumnSchema> columns = ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true).build());
    final Schema schema = new Schema(columns);

    CreateTableOptions builder = getBasicCreateTableOptions();
    // Add table-wide schema for the table.
    builder.addHashPartitions(ImmutableList.of("key"), 2, 0);

    // Add a range partition with table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt(0, -100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt(0, 100);
      builder.addRangePartition(lower, upper);
    }

    client.createTable(tableName, schema, builder);

    assertEquals(
        ImmutableList.of("-100 <= VALUES < 100"),
        client.openTable(tableName).getFormattedRangePartitions(10000));

    {
      PartialRow lower = schema.newPartialRow();
      lower.addInt(0, 100);
      PartialRow upper = schema.newPartialRow();
      upper.addInt(0, 200);

      RangePartitionWithCustomHashSchema range =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      range.addHashPartitions(ImmutableList.of("key"), 7, 0);

      client.alterTable(
          tableName, new AlterTableOptions().addRangePartition(range));
    }

    final KuduTable table = client.openTable(tableName);

    List<String> expected = ImmutableList.of(
        "-100 <= VALUES < 100", "100 <= VALUES < 200");
    assertEquals(
        expected,
        client.openTable(tableName).getFormattedRangePartitions(10000));

    final PartitionSchema ps = client.openTable(tableName).getPartitionSchema();
    assertTrue(ps.hasCustomHashSchemas());

    {
      // NOTE: use schema from server since ColumnIDs are needed for row encoding
      final PartialRow rowLower = table.getSchema().newPartialRow();
      rowLower.addInt(0, -100);

      final PartialRow rowUpper = table.getSchema().newPartialRow();
      rowUpper.addInt(0, 100);

      // There should be 2 tablets for the range with the table-wide hash schema
      // adding during table creation of the table.
      {
        final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
            KeyEncoder.encodeRangePartitionKey(rowLower, ps.getRangeSchema()));
        // There should be just one dimension with 2 buckets.
        assertEquals(1, s.size());
        assertEquals(2, s.get(0).getNumBuckets());
      }
      {
        final byte[] rowLowerEnc = ps.encodePartitionKey(rowLower);
        final byte[] rowUpperEnc = ps.encodePartitionKey(rowUpper);

        // The range part comes after the hash part in an encoded partition key.
        // The hash part contains 4 * number_of_hash_dimensions bytes.
        byte[] hashLower = Arrays.copyOfRange(rowLowerEnc, 4, rowLowerEnc.length);
        byte[] hashUpper = Arrays.copyOfRange(rowUpperEnc, 4, rowUpperEnc.length);

        Set<Integer> buckets = new HashSet();
        for (KuduScanToken token : new KuduScanToken.KuduScanTokenBuilder(asyncClient, table)
            .addPredicate(KuduPredicate.newComparisonPredicate(
                columns.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, -100))
            .addPredicate(KuduPredicate.newComparisonPredicate(
                columns.get(0), KuduPredicate.ComparisonOp.LESS, 100))
            .setTimeout(client.getDefaultOperationTimeoutMs()).build()) {
          final Partition p = token.getTablet().getPartition();
          assertEquals(0, Bytes.memcmp(p.getRangeKeyStart(), hashLower));
          assertEquals(0, Bytes.memcmp(p.getRangeKeyEnd(), hashUpper));
          assertEquals(1, p.getHashBuckets().size());
          buckets.add(p.getHashBuckets().get(0));
        }

        // Check that the generated scan tokens cover all the tablets for the range:
        // all hash bucket indices should be present.
        assertEquals(2, buckets.size());
        for (int i = 0; i < buckets.size(); ++i) {
          assertTrue(String.format("must have bucket %d", i), buckets.contains(i));
        }
      }
    }

    {
      // NOTE: use schema from server since ColumnIDs are needed for row encoding
      final PartialRow rowLower = table.getSchema().newPartialRow();
      rowLower.addInt(0, 100);

      final PartialRow rowUpper = table.getSchema().newPartialRow();
      rowUpper.addInt(0, 200);

      // There should be 7 tablets for the newly added range: and the newly added
      // range with 7 hash buckets.
      {
        final List<PartitionSchema.HashBucketSchema> s = ps.getHashSchemaForRange(
            KeyEncoder.encodeRangePartitionKey(rowLower, ps.getRangeSchema()));
        // There should be just one dimension with 7 buckets.
        assertEquals(1, s.size());
        assertEquals(7, s.get(0).getNumBuckets());
      }
      {
        final byte[] rowLowerEnc = ps.encodePartitionKey(rowLower);
        final byte[] rowUpperEnc = ps.encodePartitionKey(rowUpper);

        // The range part comes after the hash part in an encoded partition key.
        // The hash part contains 4 * number_of_hash_dimensions bytes.
        byte[] hashLower = Arrays.copyOfRange(rowLowerEnc, 4, rowLowerEnc.length);
        byte[] hashUpper = Arrays.copyOfRange(rowUpperEnc, 4, rowUpperEnc.length);

        Set<Integer> buckets = new HashSet();
        for (KuduScanToken token : new KuduScanToken.KuduScanTokenBuilder(asyncClient, table)
            .addPredicate(KuduPredicate.newComparisonPredicate(
                columns.get(0), KuduPredicate.ComparisonOp.GREATER_EQUAL, 100))
            .addPredicate(KuduPredicate.newComparisonPredicate(
                columns.get(0), KuduPredicate.ComparisonOp.LESS, 200))
            .setTimeout(client.getDefaultOperationTimeoutMs()).build()) {
          final Partition p = token.getTablet().getPartition();
          assertEquals(0, Bytes.memcmp(p.getRangeKeyStart(), hashLower));
          assertEquals(0, Bytes.memcmp(p.getRangeKeyEnd(), hashUpper));
          assertEquals(1, p.getHashBuckets().size());
          buckets.add(p.getHashBuckets().get(0));
        }

        // Check that the generated scan tokens cover all the tablets for the range:
        // all hash bucket indices should be present.
        assertEquals(7, buckets.size());
        for (int i = 0; i < buckets.size(); ++i) {
          assertTrue(String.format("must have bucket %d", i), buckets.contains(i));
        }
      }
    }

    // Make sure it's possible to insert into the newly added range.
    KuduSession session = client.newSession();
    {
      for (int key = 0; key < 9; ++key) {
        insertDefaultRow(table, session, key);
      }
      session.flush();

      List<String> rowStrings = scanTableToStrings(table);
      assertEquals(9, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); i++) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT32 key=%d, STRING value=NULL", i));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }

      rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, 8));
      assertEquals(1, rowStrings.size());
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("INT32 key=8, STRING value=NULL"));
      assertEquals(expectedRow.toString(), rowStrings.get(0));
    }

    // Insert more rows: those should go into both ranges -- the range with
    // the table-wide and the newly added range with custom hash schema.
    {
      for (int key = 9; key < 200; ++key) {
        insertDefaultRow(table, session, key);
      }
      session.flush();

      List<String> rowStrings = scanTableToStrings(table);
      assertEquals(200, rowStrings.size());
      rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, 100));
      assertEquals(100, rowStrings.size());

      rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, 180));
      assertEquals(20, rowStrings.size());
    }

    // Insert more rows into the range with table-wide hash schema.
    {
      for (int key = -100; key < 0; ++key) {
        insertDefaultRow(table, session, key);
      }
      session.flush();

      List<String> rowStrings = scanTableToStrings(table);
      assertEquals(300, rowStrings.size());

      rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), LESS, 0));
      assertEquals(100, rowStrings.size());

      // Predicate to have one part of the rows in the range with table-wide hash
      // schema, and the other part from the range with custom hash schema.
      rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, 50),
          KuduPredicate.newComparisonPredicate(schema.getColumn("key"), LESS, 150));
      assertEquals(100, rowStrings.size());
    }
  }

  @Test(timeout = 100000)
  @SuppressWarnings("deprecation")
  public void testDimensionLabel() throws Exception {
    // Create a table with dimension label.
    final KuduTable table = client.createTable(tableName, basicSchema,
        getBasicTableOptionsWithNonCoveredRange().setDimensionLabel("labelA"));

    // Add a range partition to the table with dimension label.
    AlterTableOptions ato = new AlterTableOptions();
    PartialRow lowerBound = BASIC_SCHEMA.newPartialRow();
    lowerBound.addInt("key", 300);
    PartialRow upperBound = BASIC_SCHEMA.newPartialRow();
    upperBound.addInt("key", 400);
    ato.addRangePartition(lowerBound, upperBound, "labelB",
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
    assertEquals(num, currentStatistics.getLiveRowCount());
  }
}
