// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import kudu.Common;
import kudu.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.NavigableMap;

public class TestKuduTable extends BaseKuduTest {

  private final static String baseTableName = TestKuduTable.class.getName();

  private static Schema schema = getBasicSchema();

  private static KeyBuilder keyBuilder = new KeyBuilder(schema);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  /**
   * Test creating tables of different sizes and see that we get the correct number of tablets back
   * @throws Exception
   */
  @Test
  public void testGetLocations() throws Exception {
    String table1 = baseTableName + System.currentTimeMillis();

    // Test a non-existing table
    try {
      client.openTable(table1).join(DEFAULT_SLEEP);
      fail("Should receive an exception since the table doesn't exist");
    } catch (Exception ex) {
      // expected
    }
    NavigableMap<KuduClient.RemoteTablet, List<Common.HostPortPB>> tablets =
        client.syncLocateTable(table1, null, null, DEFAULT_SLEEP);
    assertEquals(0, tablets.size());

    createTableWithSplitsAndTest(0);

    createTableWithSplitsAndTest(3);

    createTableWithSplitsAndTest(10);

    KuduTable table = createTableWithSplitsAndTest(30);

    tablets = table.getTabletsLocations(null, getKeyInBytes(9), DEFAULT_SLEEP);
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
    String tableName = baseTableName + System.currentTimeMillis();
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

    NavigableMap<KuduClient.RemoteTablet, List<Common.HostPortPB>> tablets =
        table.getTabletsLocations(DEFAULT_SLEEP);
    assertEquals(splitsCount + 1, tablets.size());
    for (KuduClient.RemoteTablet tablet : tablets.keySet()) {
      assertEquals(1, tablets.get(tablet).size());
    }
    return table;
  }
}
