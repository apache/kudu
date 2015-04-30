// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.stumbleupon.async.Deferred;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;

import static org.kududb.Type.STRING;
import static org.junit.Assert.assertEquals;

public class TestScannerMultiTablet extends BaseKuduTest {
  // Generate a unique table name
  private static final String TABLE_NAME =
      TestScannerMultiTablet.class.getName()+"-"+System.currentTimeMillis();

  private static Schema schema = getSchema();
  private static KuduTable table;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
    // create a 4-tablets table for scanning
    CreateTableBuilder builder = new CreateTableBuilder();
    KeyBuilder keyBuilder = new KeyBuilder(schema);
    builder.addSplitKey(keyBuilder.addString("1"));
    builder.addSplitKey(keyBuilder.addString("2"));
    builder.addSplitKey(keyBuilder.addString("3"));
    createTable(TABLE_NAME, schema, builder);

    table = openTable(TABLE_NAME);
  }

  @Test(timeout = 100000)
  public void test() throws Exception {
    KuduSession session = client.newSession();
    session.setFlushMode(KuduSession.FlushMode.AUTO_FLUSH_SYNC);

    String[] keys = new String[] {"1", "2", "3"};
    for (String key1 : keys) {
      for (String key2 : keys) {
        Insert insert = table.newInsert();
        insert.addString(schema.getColumn(0).getName(), key1 + key2);
        Deferred<OperationResponse> d = session.apply(insert);
        d.join(DEFAULT_SLEEP);
      }
    }

    // The data layout ends up like this:
    // tablet '', '1': no rows
    // tablet '1', '2': '11', '12', '13'
    // tablet '2', '3': '21', '22', '23'
    // tablet '3', '': '31', '32', '33'

    assertEquals(0, countRowsInScan(getScanner("", "1"))); // There's nothing in the 1st tablet
    assertEquals(1, countRowsInScan(getScanner("", "11"))); // Grab the very first row
    assertEquals(3, countRowsInScan(getScanner("11", "13"))); // Grab the whole 2nd tablet
    assertEquals(3, countRowsInScan(getScanner("11", "2"))); // Same, and peek at the 3rd
    assertEquals(3, countRowsInScan(getScanner("11", "20"))); // Same, different peek
    assertEquals(4, countRowsInScan(getScanner("12", "22"))); // Middle of 2nd to middle of 3rd
    assertEquals(3, countRowsInScan(getScanner("14", "24"))); // Peek at the 2nd then whole 3rd
    assertEquals(6, countRowsInScan(getScanner("14", "34"))); // Middle of 2nd to middle of 4th
    assertEquals(9, countRowsInScan(getScanner("", "4"))); // Full table scan

    assertEquals(9, countRowsInScan(getScanner("", null))); // Full table scan with empty upper
    assertEquals(9, countRowsInScan(getScanner(null, "4"))); // Full table scan with empty lower
    assertEquals(9, countRowsInScan(getScanner(null, null))); // Full table scan with empty bounds

  }

  private KuduScanner getScanner(String lowerBound, String upperBound) {
    KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table, schema);
    ColumnRangePredicate pred = new ColumnRangePredicate(schema.getColumn(0));
    if (lowerBound != null) {
      pred.setLowerBound(lowerBound);
    }
    if (upperBound != null) {
      pred.setUpperBound(upperBound);
    }
    if (lowerBound != null || upperBound != null) {
      builder.addColumnRangePredicate(pred);
    }
    return builder.build();
  }

  private static Schema getSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(1);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", STRING)
        .key(true)
        .build());
    return new Schema(columns);
  }
}
