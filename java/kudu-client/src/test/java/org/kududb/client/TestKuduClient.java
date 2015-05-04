// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;

import java.util.List;

public class TestKuduClient extends BaseKuduTest {
  private static final String TABLE_NAME =
      TestKuduClient.class.getName() + "-" + System.currentTimeMillis();

  /**
   * Test basic interactions through a KuduClient.
   */
  @Test(timeout = 100000)
  public void test() throws Exception {
    // Check that we can create a table.
    syncClient.createTable(TABLE_NAME, basicSchema);
    assertFalse(syncClient.getTablesList().getTablesList().isEmpty());
    assertEquals(TABLE_NAME, syncClient.getTablesList().getTablesList().get(0));

    // Check that we can delete it.
    syncClient.deleteTable(TABLE_NAME);
    assertTrue(syncClient.getTablesList().getTablesList().isEmpty());

    // Check that we can re-recreate it, with a different schema.
    List<ColumnSchema> columns = basicSchema.getColumns();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("one more", Type.STRING).build());
    Schema newSchema = new Schema(columns);
    syncClient.createTable(TABLE_NAME, newSchema);

    // Check that we can open a table and see that it has the new schema.
    KuduTable table = syncClient.openTable(TABLE_NAME);
    assertEquals(newSchema.getColumnCount(), table.getSchema().getColumnCount());
  }
}
