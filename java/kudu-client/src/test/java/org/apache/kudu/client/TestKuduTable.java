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

import org.junit.Rule;
import org.junit.rules.TestName;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestKuduTable extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduTable.class);

  @Rule
  public TestName name = new TestName();

  private static Schema schema = getBasicSchema();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  @Test(timeout = 100000)
  public void testAlterTable() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();
    createTable(tableName, basicSchema, getBasicCreateTableOptions());
    try {

      // Add a col.
      AlterTableOptions ato = new AlterTableOptions().addColumn("testaddint", Type.INT32, 4);
      submitAlterAndCheck(ato, tableName);

      // Rename that col.
      ato = new AlterTableOptions().renameColumn("testaddint", "newtestaddint");
      submitAlterAndCheck(ato, tableName);

      // Delete it.
      ato = new AlterTableOptions().dropColumn("newtestaddint");
      submitAlterAndCheck(ato, tableName);

      String newTableName = tableName +"new";

      // Rename our table.
      ato = new AlterTableOptions().renameTable(newTableName);
      submitAlterAndCheck(ato, tableName, newTableName);

      // Rename it back.
      ato = new AlterTableOptions().renameTable(tableName);
      submitAlterAndCheck(ato, newTableName, tableName);

      // Add 3 columns, where one has default value, nullable and Timestamp with default value
      ato = new AlterTableOptions()
          .addColumn("testaddmulticolnotnull", Type.INT32, 4)
          .addNullableColumn("testaddmulticolnull", Type.STRING)
          .addColumn("testaddmulticolTimestampcol", Type.UNIXTIME_MICROS,
              (System.currentTimeMillis() * 1000));
      submitAlterAndCheck(ato, tableName);


      // Try altering a table that doesn't exist.
      String nonExistingTableName = "table_does_not_exist";
      try {
        syncClient.alterTable(nonExistingTableName, ato);
        fail("Shouldn't be able to alter a table that doesn't exist");
      } catch (KuduException ex) {
        assertTrue(ex.getStatus().isNotFound());
      }

      try {
        syncClient.isAlterTableDone(nonExistingTableName);
        fail("Shouldn't be able to query if an alter table is done here");
      } catch (KuduException ex) {
        assertTrue(ex.getStatus().isNotFound());
      }
    } finally {
      // Normally Java tests accumulate tables without issue, deleting them all
      // when shutting down the mini cluster at the end of every test class.
      // However, testGetLocations below expects a certain table count, so
      // we'll delete our table to ensure there's no interaction between them.
      syncClient.deleteTable(tableName);
    }
  }

  /**
   * Helper method to submit an Alter and wait for it to happen, using the default table name to
   * check.
   */
  private void submitAlterAndCheck(AlterTableOptions ato, String tableToAlter)
      throws Exception {
    submitAlterAndCheck(ato, tableToAlter, tableToAlter);
  }

  private void submitAlterAndCheck(AlterTableOptions ato,
                                         String tableToAlter, String tableToCheck) throws
      Exception {
    AlterTableResponse alterResponse = syncClient.alterTable(tableToAlter, ato);
    boolean done  = syncClient.isAlterTableDone(tableToCheck);
    assertTrue(done);
  }

  /**
   * Test creating tables of different sizes and see that we get the correct number of tablets back
   * @throws Exception
   */
  @Test
  public void testGetLocations() throws Exception {
    String table1 = name.getMethodName() + System.currentTimeMillis();

    // Test a non-existing table
    try {
      openTable(table1);
      fail("Should receive an exception since the table doesn't exist");
    } catch (Exception ex) {
      // expected
    }
    // Test with defaults
    String tableWithDefault = name.getMethodName() + "WithDefault" + System.currentTimeMillis();
    CreateTableOptions builder = getBasicCreateTableOptions();
    List<ColumnSchema> columns = new ArrayList<ColumnSchema>(schema.getColumnCount());
    int defaultInt = 30;
    String defaultString = "data";
    for (ColumnSchema columnSchema : schema.getColumns()) {

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
    KuduTable kuduTable = createTable(tableWithDefault, schemaWithDefault, builder);
    assertEquals(defaultInt, kuduTable.getSchema().getColumnByIndex(0).getDefaultValue());
    assertEquals(defaultString,
        kuduTable.getSchema().getColumnByIndex(columns.size() - 2).getDefaultValue());
    assertEquals(true,
            kuduTable.getSchema().getColumnByIndex(columns.size() - 1).getDefaultValue());

    // Make sure the table's schema includes column IDs.
    assertTrue(kuduTable.getSchema().hasColumnIds());

    // Test we can open a table that was already created.
    openTable(tableWithDefault);

    // Test splitting and reading those splits
    KuduTable kuduTableWithoutDefaults = createTableWithSplitsAndTest(0);
    // finish testing read defaults
    assertNull(kuduTableWithoutDefaults.getSchema().getColumnByIndex(0).getDefaultValue());
    createTableWithSplitsAndTest(3);
    createTableWithSplitsAndTest(10);

    KuduTable table = createTableWithSplitsAndTest(30);

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
    assertEquals(0, client.getTablesList(table1).join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(1, client.getTablesList(tableWithDefault)
                          .join(DEFAULT_SLEEP).getTablesList().size());
    assertEquals(5, client.getTablesList().join(DEFAULT_SLEEP).getTablesList().size());
    assertFalse(client.getTablesList(tableWithDefault).
        join(DEFAULT_SLEEP).getTablesList().isEmpty());

    assertFalse(client.tableExists(table1).join(DEFAULT_SLEEP));
    assertTrue(client.tableExists(tableWithDefault).join(DEFAULT_SLEEP));
  }

  @Test(timeout = 100000)
  public void testLocateTableNonCoveringRange() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();
    syncClient.createTable(tableName, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = syncClient.openTable(tableName);

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
    PartialRow row = schema.newPartialRow();
    row.addInt(0, i);
    return row.encodePrimaryKey();
  }

  @Test(timeout = 100000)
  public void testAlterTableNonCoveringRange() throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();
    syncClient.createTable(tableName, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = syncClient.openTable(tableName);
    KuduSession session = syncClient.newSession();

    AlterTableOptions ato = new AlterTableOptions();
    PartialRow bLowerBound = schema.newPartialRow();
    bLowerBound.addInt("key", 300);
    PartialRow bUpperBound = schema.newPartialRow();
    bUpperBound.addInt("key", 400);
    ato.addRangePartition(bLowerBound, bUpperBound);
    syncClient.alterTable(tableName, ato);

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
    bLowerBound = schema.newPartialRow();
    bLowerBound.addInt("key", 200);
    bUpperBound = schema.newPartialRow();
    bUpperBound.addInt("key", 300);
    ato.dropRangePartition(bLowerBound, bUpperBound);
    syncClient.alterTable(tableName, ato);

    insert = createBasicSchemaInsert(table, 202);
    OperationResponse response = session.apply(insert);
    assertTrue(response.hasRowError());
    assertTrue(response.getRowError().getErrorStatus().isNotFound());
  }

  public KuduTable createTableWithSplitsAndTest(int splitsCount) throws Exception {
    String tableName = name.getMethodName() + System.currentTimeMillis();
    CreateTableOptions builder = getBasicCreateTableOptions();

    if (splitsCount != 0) {
      for (int i = 1; i <= splitsCount; i++) {
        PartialRow row = schema.newPartialRow();
        row.addInt(0, i);
        builder.addSplitRow(row);
      }
    }
    KuduTable table = createTable(tableName, schema, builder);

    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(splitsCount + 1, tablets.size());
    assertEquals(splitsCount + 1, table.asyncGetTabletsLocations(DEFAULT_SLEEP).join().size());
    for (LocatedTablet tablet : tablets) {
      assertEquals(3, tablet.getReplicas().size());
    }
    return table;
  }
}
