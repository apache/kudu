// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestKuduTable extends BaseKuduTest {

  private static final String BASE_TABLE_NAME = TestKuduTable.class.getName();

  private static Schema schema = getBasicSchema();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadSchema() {
    // Test creating a table with keys in the wrong order
    List<ColumnSchema> badColumns = new ArrayList<ColumnSchema>(2);
    badColumns.add(new ColumnSchema.ColumnSchemaBuilder("not_key", Type.STRING).build());
    badColumns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
        .key(true)
        .build());
    new Schema(badColumns);
  }

  /**
   * Test creating tables of different sizes and see that we get the correct number of tablets back
   * @throws Exception
   */
  @Test
  public void testGetLocations() throws Exception {
    String table1 = BASE_TABLE_NAME + System.currentTimeMillis();

    // Test a non-existing table
    try {
      openTable(table1);
      fail("Should receive an exception since the table doesn't exist");
    } catch (Exception ex) {
      // expected
    }
    // Test with defaults
    String tableWithDefault = BASE_TABLE_NAME + "WithDefault" + System.currentTimeMillis();
    CreateTableBuilder builder = new CreateTableBuilder();
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
    createTable(tableWithDefault, schemaWithDefault, builder);
    KuduTable kuduTable = openTable(tableWithDefault);
    assertEquals(defaultInt, kuduTable.getSchema().getColumnByIndex(0).getDefaultValue());
    assertEquals(defaultString,
        kuduTable.getSchema().getColumnByIndex(columns.size() - 2).getDefaultValue());
    assertEquals(true,
            kuduTable.getSchema().getColumnByIndex(columns.size() - 1).getDefaultValue());

    // Make sure the table's schema includes column IDs.
    assertTrue(kuduTable.getSchema().hasColumnIds());

    // Test splitting and reading those splits
    KuduTable kuduTableWithoutDefaults = createTableWithSplitsAndTest(0);
    // finish testing read defaults
    assertNull(kuduTableWithoutDefaults.getSchema().getColumnByIndex(0).getDefaultValue());
    createTableWithSplitsAndTest(3);
    createTableWithSplitsAndTest(10);

    KuduTable table = createTableWithSplitsAndTest(30);

    List<LocatedTablet>tablets = table.getTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(10, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(0), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(10, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(9), DEFAULT_SLEEP);
    assertEquals(5, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(14), DEFAULT_SLEEP);
    assertEquals(10, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), getKeyInBytes(31), DEFAULT_SLEEP);
    assertEquals(26, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(5), null, DEFAULT_SLEEP);
    assertEquals(26, tablets.size());

    tablets = table.getTabletsLocations(null, getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(31, tablets.size());

    tablets = table.getTabletsLocations(getKeyInBytes(20), getKeyInBytes(10000), DEFAULT_SLEEP);
    assertEquals(11, tablets.size());


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

  public byte[] getKeyInBytes(int i) {
    PartialRow row = schema.newPartialRow();
    row.addInt(0, i);
    return row.encodePrimaryKey();
  }

  public KuduTable createTableWithSplitsAndTest(int splitsCount) throws Exception {
    String tableName = BASE_TABLE_NAME + System.currentTimeMillis();
    CreateTableBuilder builder = new CreateTableBuilder();

    if (splitsCount != 0) {
      PartialRow row = schema.newPartialRow();
      for (int i = 1; i <= splitsCount; i++) {
        row.addInt(0, i);
        builder.addSplitRow(row);
      }
    }
    createTable(tableName, schema, builder);

    KuduTable table = openTable(tableName);
    // calling getTabletsLocation won't wait on the table to be assigned so we trigger the wait
    // by scanning
    countRowsInScan(client.newScannerBuilder(table).build());

    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(splitsCount + 1, tablets.size());
    for (LocatedTablet tablet : tablets) {
      assertEquals(1, tablet.getReplicas().size());
    }
    return table;
  }
}
