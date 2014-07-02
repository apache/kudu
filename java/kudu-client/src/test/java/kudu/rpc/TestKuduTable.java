// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKuduTable extends BaseKuduTest {

  private static final String BASE_TABLE_NAME = TestKuduTable.class.getName();

  private static Schema schema = getBasicSchema();

  private static KeyBuilder keyBuilder = new KeyBuilder(schema);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadSchema() {
    // Test creating a table with keys in the wrong order
    List<ColumnSchema> badColumns = new ArrayList<ColumnSchema>(2);
    badColumns.add(new ColumnSchema("not_key", Type.STRING, false));
    badColumns.add(new ColumnSchema("key", Type.STRING, true));
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
      } else {
        defaultValue = defaultString;
      }
      columns.add(new ColumnSchema(columnSchema.getName(), columnSchema.getType(),
          columnSchema.isKey(), columnSchema.isNullable(), defaultValue));
    }
    Schema schemaWithDefault = new Schema(columns);
    createTable(tableWithDefault, schemaWithDefault, builder);
    KuduTable kuduTable = openTable(tableWithDefault);
    assertEquals(new Integer(defaultInt), kuduTable.getSchema().getColumn(0).getDefaultValue());
    assertEquals(defaultString,
        kuduTable.getSchema().getColumn(columns.size() - 1).getDefaultValue());

    // Test splitting and reading those splits
    KuduTable kuduTableWithoutDefaults = createTableWithSplitsAndTest(0);
    // finish testing read defaults
    assertNull(kuduTableWithoutDefaults.getSchema().getColumn(0).getDefaultValue());
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
  }

  public byte[] getKeyInBytes(int i) {
    return keyBuilder.addUnsignedInt(i).extractByteArray();
  }

  public KuduTable createTableWithSplitsAndTest(int splitsCount) throws Exception {
    String tableName = BASE_TABLE_NAME + System.currentTimeMillis();
    CreateTableBuilder builder = new CreateTableBuilder();

    if (splitsCount != 0) {
      for (int i = 1; i <= splitsCount; i++) {
        builder.addSplitKey(keyBuilder.addInt(i));
      }
    }
    createTable(tableName, schema, builder);

    KuduTable table = openTable(tableName);
    // calling getTabletsLocation won't wait on the table to be assigned so we trigger the wait
    // by scanning
    countRowsInScan(client.newScanner(table, schema));

    List<LocatedTablet> tablets = table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(splitsCount + 1, tablets.size());
    for (LocatedTablet tablet : tablets) {
      assertEquals(1, tablet.getReplicas().size());
    }
    return table;
  }
}
