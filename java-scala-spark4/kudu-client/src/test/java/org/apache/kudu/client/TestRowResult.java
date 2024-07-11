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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.test.ClientTestUtil.getAllTypesCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getSchemaWithAllTypes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.DateUtil;

public class TestRowResult {

  // Generate a unique table name
  private static final String TABLE_NAME =
      TestRowResult.class.getName() + "-" + System.currentTimeMillis();

  private static final Schema allTypesSchema = getSchemaWithAllTypes();

  // insert 5 rows to test result iterations
  private static final int TEST_ROWS = 5;

  private KuduTable table;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    harness.getClient().createTable(TABLE_NAME, allTypesSchema, getAllTypesCreateTableOptions());
    table = harness.getClient().openTable(TABLE_NAME);

    KuduClient client = harness.getClient();
    KuduSession session = client.newSession();

    for (int i = 0; i < TEST_ROWS; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();

      row.addByte(0, (byte) i);
      row.addShort(1, (short) 2);
      row.addInt(2, 3);
      row.addLong(3, 4L);
      row.addBoolean(4, true);
      row.addFloat(5, 5.6f);
      row.addDouble(6, 7.8);
      row.addString(7, "string-value");
      row.addBinary(8, "binary-array".getBytes(UTF_8));
      ByteBuffer bb = ByteBuffer.wrap("binary-bytebuffer".getBytes(UTF_8));
      bb.position(7); // We're only inserting the bytebuffer part of the original array.
      row.addBinary(9, bb);
      row.setNull(10);
      row.addTimestamp(11, new Timestamp(11));
      row.addDecimal(12, BigDecimal.valueOf(12345, 3));
      row.addVarchar(13, "varcharval");
      row.addDate(14, DateUtil.epochDaysToSqlDate(0));

      session.apply(insert);
    }
  }

  @Test(timeout = 10000)
  public void testRowwiseRowset() throws Exception {
    KuduClient client = harness.getClient();
    KuduScanner scanner = client.newScannerBuilder(table).build();
    checkRows(scanner);
  }

  @Test(timeout = 10000)
  public void testRowwiseRowsetReuse() throws Exception {
    KuduClient client = harness.getClient();
    KuduScanner scanner = client.newScannerBuilder(table).build();
    scanner.setReuseRowResult(true);
    checkRows(scanner);
  }

  @Test(timeout = 10000)
  public void testColumnarRowset() throws Exception {
    KuduClient client = harness.getClient();
    KuduScanner scanner = client.newScannerBuilder(table).build();
    scanner.setRowDataFormat(AsyncKuduScanner.RowDataFormat.COLUMNAR);
    checkRows(scanner);
  }

  @Test(timeout = 10000)
  public void testColumnarRowsetReuse() throws Exception {
    KuduClient client = harness.getClient();
    KuduScanner scanner = client.newScannerBuilder(table).build();
    scanner.setRowDataFormat(AsyncKuduScanner.RowDataFormat.COLUMNAR);
    scanner.setReuseRowResult(true);
    checkRows(scanner);
  }

  private void checkRows(KuduScanner scanner) throws KuduException {
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      assertEquals(TEST_ROWS, it.getNumRows());
      for (int i = 0; i < TEST_ROWS; i++) {
        assertTrue(it.hasNext());
        RowResult rr = it.next();

        assertEquals((byte) i, rr.getByte(0));
        assertEquals((byte) i, rr.getObject(0));
        assertEquals((byte) i, rr.getByte(allTypesSchema.getColumnByIndex(0).getName()));

        assertEquals((short) 2, rr.getShort(1));
        assertEquals((short) 2, rr.getObject(1));
        assertEquals((short) 2, rr.getShort(allTypesSchema.getColumnByIndex(1).getName()));

        assertEquals(3, rr.getInt(2));
        assertEquals(3, rr.getObject(2));
        assertEquals(3, rr.getInt(allTypesSchema.getColumnByIndex(2).getName()));

        assertEquals((long) 4, rr.getLong(3));
        assertEquals((long) 4, rr.getObject(3));
        assertEquals((long) 4, rr.getLong(allTypesSchema.getColumnByIndex(3).getName()));

        assertEquals(true, rr.getBoolean(4));
        assertEquals(true, rr.getObject(4));
        assertEquals(true, rr.getBoolean(allTypesSchema.getColumnByIndex(4).getName()));

        assertEquals(5.6f, rr.getFloat(5), .001f);
        assertEquals(5.6f, (float) rr.getObject(5), .001f);
        assertEquals(5.6f,
                rr.getFloat(allTypesSchema.getColumnByIndex(5).getName()), .001f);

        assertEquals(7.8, rr.getDouble(6), .001);
        assertEquals(7.8, (double) rr.getObject(6), .001);
        assertEquals(7.8,
                rr.getDouble(allTypesSchema.getColumnByIndex(6).getName()), .001f);

        assertEquals("string-value", rr.getString(7));
        assertEquals("string-value", rr.getObject(7));
        assertEquals("string-value",
                rr.getString(allTypesSchema.getColumnByIndex(7).getName()));

        assertArrayEquals("binary-array".getBytes(UTF_8), rr.getBinaryCopy(8));
        assertArrayEquals("binary-array".getBytes(UTF_8), (byte[]) rr.getObject(8));
        assertArrayEquals("binary-array".getBytes(UTF_8),
                rr.getBinaryCopy(allTypesSchema.getColumnByIndex(8).getName()));

        ByteBuffer buffer = rr.getBinary(8);
        assertEquals(buffer, rr.getBinary(allTypesSchema.getColumnByIndex(8).getName()));
        byte[] binaryValue = new byte[buffer.remaining()];
        buffer.get(binaryValue);
        assertArrayEquals("binary-array".getBytes(UTF_8), binaryValue);

        assertArrayEquals("bytebuffer".getBytes(UTF_8), rr.getBinaryCopy(9));

        assertEquals(true, rr.isNull(10));
        assertNull(rr.getObject(10));
        assertEquals(true, rr.isNull(allTypesSchema.getColumnByIndex(10).getName()));

        assertEquals(new Timestamp(11), rr.getTimestamp(11));
        assertEquals(new Timestamp(11), rr.getObject(11));
        assertEquals(new Timestamp(11),
                rr.getTimestamp(allTypesSchema.getColumnByIndex(11).getName()));

        assertEquals(BigDecimal.valueOf(12345, 3), rr.getDecimal(12));
        assertEquals(BigDecimal.valueOf(12345, 3), rr.getObject(12));
        assertEquals(BigDecimal.valueOf(12345, 3),
                rr.getDecimal(allTypesSchema.getColumnByIndex(12).getName()));

        assertEquals("varcharval", rr.getVarchar(13));
        assertEquals("varcharval", rr.getObject(13));
        assertEquals("varcharval",
                rr.getVarchar(allTypesSchema.getColumnByIndex(13).getName()));

        assertEquals(DateUtil.epochDaysToSqlDate(0), rr.getDate(14));
        assertEquals(DateUtil.epochDaysToSqlDate(0), rr.getObject(14));
        assertEquals(DateUtil.epochDaysToSqlDate(0),
                rr.getDate(allTypesSchema.getColumnByIndex(14).getName()));

        // We test with the column name once since it's the same method for all types, unlike above.
        assertEquals(Type.INT8, rr.getColumnType(allTypesSchema.getColumnByIndex(0).getName()));
        assertEquals(Type.INT8, rr.getColumnType(0));
        assertEquals(Type.INT16, rr.getColumnType(1));
        assertEquals(Type.INT32, rr.getColumnType(2));
        assertEquals(Type.INT64, rr.getColumnType(3));
        assertEquals(Type.BOOL, rr.getColumnType(4));
        assertEquals(Type.FLOAT, rr.getColumnType(5));
        assertEquals(Type.DOUBLE, rr.getColumnType(6));
        assertEquals(Type.STRING, rr.getColumnType(7));
        assertEquals(Type.BINARY, rr.getColumnType(8));
        assertEquals(Type.UNIXTIME_MICROS, rr.getColumnType(11));
        assertEquals(Type.DECIMAL, rr.getColumnType(12));
        assertEquals(Type.VARCHAR, rr.getColumnType(13));
        assertEquals(Type.DATE, rr.getColumnType(14));
      }
    }
  }
}
