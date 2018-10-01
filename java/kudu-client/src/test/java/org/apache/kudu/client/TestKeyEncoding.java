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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;
import org.apache.kudu.client.PartitionSchema.RangeSchema;
import org.apache.kudu.util.DecimalUtil;

public class TestKeyEncoding {

  private KuduClient client;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  private static Schema buildSchema(ColumnSchemaBuilder... columns) {
    int i = 0;
    Common.SchemaPB.Builder pb = Common.SchemaPB.newBuilder();
    for (ColumnSchemaBuilder column : columns) {
      Common.ColumnSchemaPB.Builder columnPb =
          ProtobufHelper.columnToPb(column.build()).toBuilder();
      columnPb.setId(i++);
      pb.addColumns(columnPb);
    }
    return ProtobufHelper.pbToSchema(pb.build());
  }

  private static void assertBytesEquals(byte[] actual, byte[] expected) {
    assertTrue(String.format("expected: '%s', got: '%s'",
                             Bytes.pretty(expected),
                             Bytes.pretty(actual)),
               Bytes.equals(expected, actual));
  }

  private static void assertBytesEquals(byte[] actual, String expected) {
    assertBytesEquals(actual, expected.getBytes(UTF_8));
  }

  /**
   * Builds the default partition schema for a schema.
   * @param schema the schema
   * @return a default partition schema
   */
  private PartitionSchema defaultPartitionSchema(Schema schema) {
    List<Integer> columnIds = new ArrayList<>();
    for (int i = 0; i < schema.getPrimaryKeyColumnCount(); i++) {
      // Schema does not provide a way to lookup a column ID by column index,
      // so instead we assume that the IDs for the primary key columns match
      // their respective index, which holds up when the schema is created
      // with buildSchema.
      columnIds.add(i);
    }
    return new PartitionSchema(
        new PartitionSchema.RangeSchema(columnIds),
        ImmutableList.<PartitionSchema.HashBucketSchema>of(), schema);
  }

  /**
   * Builds the default CreateTableOptions for a schema.
   *
   * @param schema the schema
   * @return a default CreateTableOptions
   */
  private CreateTableOptions defaultCreateTableOptions(Schema schema) {
    List<String> columnNames = new ArrayList<>();
    for (ColumnSchema columnSchema : schema.getPrimaryKeyColumns()) {
      columnNames.add(columnSchema.getName());
    }
    return new CreateTableOptions()
        .setRangePartitionColumns(columnNames);
  }

  @Test
  public void testPrimaryKeys() {
    Schema schemaOneString =
        buildSchema(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true));
    KuduTable table = new KuduTable(null, "one", "one", schemaOneString,
                                    defaultPartitionSchema(schemaOneString), 3);
    Insert oneKeyInsert = new Insert(table);
    PartialRow row = oneKeyInsert.getRow();
    row.addString("key", "foo");
    assertBytesEquals(row.encodePrimaryKey(), "foo");

    Schema schemaTwoString = buildSchema(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true),
        new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING).key(true));
    KuduTable table2 = new KuduTable(null, "two", "two", schemaTwoString,
                                     defaultPartitionSchema(schemaTwoString), 3);
    Insert twoKeyInsert = new Insert(table2);
    row = twoKeyInsert.getRow();
    row.addString("key", "foo");
    row.addString("key2", "bar");
    assertBytesEquals(row.encodePrimaryKey(), "foo\0\0bar");

    Insert twoKeyInsertWithNull = new Insert(table2);
    row = twoKeyInsertWithNull.getRow();
    row.addString("key", "xxx\0yyy");
    row.addString("key2", "bar");
    assertBytesEquals(row.encodePrimaryKey(), "xxx\0\1yyy\0\0bar");

    // test that we get the correct memcmp result, the bytes are in big-endian order in a key
    Schema schemaIntString = buildSchema(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true),
        new ColumnSchema.ColumnSchemaBuilder("key2", Type.STRING).key(true));
    PartitionSchema partitionSchemaIntString = defaultPartitionSchema(schemaIntString);
    KuduTable table3 = new KuduTable(null, "three", "three",
        schemaIntString, partitionSchemaIntString, 3);
    Insert small = new Insert(table3);
    row = small.getRow();
    row.addInt("key", 20);
    row.addString("key2", "data");
    byte[] smallPK = small.getRow().encodePrimaryKey();
    assertEquals(0, Bytes.memcmp(smallPK, smallPK));

    Insert big = new Insert(table3);
    row = big.getRow();
    row.addInt("key", 10000);
    row.addString("key2", "data");
    byte[] bigPK = big.getRow().encodePrimaryKey();
    assertTrue(Bytes.memcmp(smallPK, bigPK) < 0);
    assertTrue(Bytes.memcmp(bigPK, smallPK) > 0);

    // The following tests test our assumptions on unsigned data types sorting from KeyEncoder
    byte four = 4;
    byte onHundredTwentyFour = -4;
    four = Bytes.xorLeftMostBit(four);
    onHundredTwentyFour = Bytes.xorLeftMostBit(onHundredTwentyFour);
    assertTrue(four < onHundredTwentyFour);

    byte[] threeHundred = Bytes.fromInt(300);
    byte[] reallyBigNumber = Bytes.fromInt(-300);
    threeHundred[0] = Bytes.xorLeftMostBit(threeHundred[0]);
    reallyBigNumber[3] = Bytes.xorLeftMostBit(reallyBigNumber[3]);
    assertTrue(Bytes.memcmp(threeHundred, reallyBigNumber) < 0);
  }

  @Test
  public void testPrimaryKeyEncoding() {
    Schema schema = buildSchema(
        new ColumnSchemaBuilder("int8", Type.INT8).key(true),
        new ColumnSchemaBuilder("int16", Type.INT16).key(true),
        new ColumnSchemaBuilder("int32", Type.INT32).key(true),
        new ColumnSchemaBuilder("int64", Type.INT64).key(true),
        new ColumnSchemaBuilder("decimal32", Type.DECIMAL).key(true)
            .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL32_PRECISION, 0)),
        new ColumnSchemaBuilder("decimal64", Type.DECIMAL).key(true)
            .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL64_PRECISION, 0)),
        new ColumnSchemaBuilder("decimal128", Type.DECIMAL).key(true)
            .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL128_PRECISION, 0)),
        new ColumnSchemaBuilder("string", Type.STRING).key(true),
        new ColumnSchemaBuilder("binary", Type.BINARY).key(true));

    PartialRow rowA = schema.newPartialRow();
    rowA.addByte("int8", Byte.MIN_VALUE);
    rowA.addShort("int16", Short.MIN_VALUE);
    rowA.addInt("int32", Integer.MIN_VALUE);
    rowA.addLong("int64", Long.MIN_VALUE);
    // Note: The decimal value is not the minimum of the underlying int32, int64, int128 type so
    // we don't use "minimum" values in the test.
    rowA.addDecimal("decimal32", BigDecimal.valueOf(5));
    rowA.addDecimal("decimal64", BigDecimal.valueOf(6));
    rowA.addDecimal("decimal128", BigDecimal.valueOf(7));
    rowA.addString("string", "");
    rowA.addBinary("binary", "".getBytes(UTF_8));

    byte[] rowAEncoded = rowA.encodePrimaryKey();
    assertBytesEquals(rowAEncoded,
                      new byte[] {
                          0,
                          0, 0,
                          0, 0, 0, 0,
                          0, 0, 0, 0, 0, 0, 0, 0,
                          (byte) 0x80, 0, 0, 5,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 6,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                          0, 0
                      });
    assertEquals(rowA.stringifyRowKey(),
                 KeyEncoder.decodePrimaryKey(schema, rowAEncoded).stringifyRowKey());

    PartialRow rowB = schema.newPartialRow();
    rowB.addByte("int8", Byte.MAX_VALUE);
    rowB.addShort("int16", Short.MAX_VALUE);
    rowB.addInt("int32", Integer.MAX_VALUE);
    rowB.addLong("int64", Long.MAX_VALUE);
    // Note: The decimal value is not the maximum of the underlying int32, int64, int128 type so
    // we don't use "minimum" values in the test.
    rowB.addDecimal("decimal32", BigDecimal.valueOf(5));
    rowB.addDecimal("decimal64", BigDecimal.valueOf(6));
    rowB.addDecimal("decimal128", BigDecimal.valueOf(7));
    rowB.addString("string", "abc\1\0def");
    rowB.addBinary("binary", "\0\1binary".getBytes(UTF_8));

    byte[] rowBEncoded = rowB.encodePrimaryKey();
    assertBytesEquals(rowB.encodePrimaryKey(),
                      new byte[] {
                          -1,
                          -1, -1,
                          -1, -1, -1, -1,
                          -1, -1, -1, -1, -1, -1, -1, -1,
                          (byte) 0x80, 0, 0, 5,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 6,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                          'a', 'b', 'c', 1, 0, 1, 'd', 'e', 'f', 0, 0,
                          0, 1, 'b', 'i', 'n', 'a', 'r', 'y',
                      });
    assertEquals(rowB.stringifyRowKey(),
                 KeyEncoder.decodePrimaryKey(schema, rowBEncoded).stringifyRowKey());

    PartialRow rowC = schema.newPartialRow();
    rowC.addByte("int8", (byte) 1);
    rowC.addShort("int16", (short) 2);
    rowC.addInt("int32", 3);
    rowC.addLong("int64", 4);
    rowC.addDecimal("decimal32", BigDecimal.valueOf(5));
    rowC.addDecimal("decimal64", BigDecimal.valueOf(6));
    rowC.addDecimal("decimal128", BigDecimal.valueOf(7));
    rowC.addString("string", "abc\n123");
    rowC.addBinary("binary", "\0\1\2\3\4\5".getBytes(UTF_8));

    byte[] rowCEncoded = rowC.encodePrimaryKey();
    assertBytesEquals(rowCEncoded,
                      new byte[] {
                          (byte) 0x81,
                          (byte) 0x80, 2,
                          (byte) 0x80, 0, 0, 3,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 4,
                          (byte) 0x80, 0, 0, 5,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 6,
                          (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7,
                          'a', 'b', 'c', '\n', '1', '2', '3', 0, 0,
                          0, 1, 2, 3, 4, 5,
                      });
    assertEquals(rowC.stringifyRowKey(),
                 KeyEncoder.decodePrimaryKey(schema, rowCEncoded).stringifyRowKey());

    PartialRow rowD = schema.newPartialRow();
    rowD.addByte("int8", (byte) -1);
    rowD.addShort("int16", (short) -2);
    rowD.addInt("int32", -3);
    rowD.addLong("int64", -4);
    rowD.addDecimal("decimal32", BigDecimal.valueOf(-5));
    rowD.addDecimal("decimal64", BigDecimal.valueOf(-6));
    rowD.addDecimal("decimal128", BigDecimal.valueOf(-7));
    rowD.addString("string", "\0abc\n\1\1\0 123\1\0");
    rowD.addBinary("binary", "\0\1\2\3\4\5\0".getBytes(UTF_8));

    byte[] rowDEncoded = rowD.encodePrimaryKey();
    assertBytesEquals(rowDEncoded,
                      new byte[] {
                          (byte) 127,
                          (byte) 127, -2,
                          (byte) 127, -1, -1, -3,
                          (byte) 127, -1, -1, -1, -1, -1, -1, -4,
                          (byte) 127, -1, -1, -5,
                          (byte) 127, -1, -1, -1, -1, -1, -1, -6,
                          (byte) 127, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -7,
                          0, 1, 'a', 'b', 'c', '\n', 1, 1, 0, 1, ' ', '1', '2', '3', 1, 0, 1, 0, 0,
                          0, 1, 2, 3, 4, 5, 0,
                      });
    assertEquals(rowD.stringifyRowKey(),
                 KeyEncoder.decodePrimaryKey(schema, rowDEncoded).stringifyRowKey());
  }

  @Test
  public void testPartitionKeyEncoding() {
    Schema schema = buildSchema(
        new ColumnSchemaBuilder("a", Type.INT32).key(true),
        new ColumnSchemaBuilder("b", Type.STRING).key(true),
        new ColumnSchemaBuilder("c", Type.STRING).key(true));

    PartitionSchema partitionSchema =
        new PartitionSchema(new RangeSchema(ImmutableList.of(0, 1, 2)),
                            ImmutableList.of(
                                new HashBucketSchema(ImmutableList.of(0, 1), 32, 0),
                                new HashBucketSchema(ImmutableList.of(2), 32, 42)),
                            schema);

    PartialRow rowA = schema.newPartialRow();
    rowA.addInt("a", 0);
    rowA.addString("b", "");
    rowA.addString("c", "");
    assertBytesEquals(KeyEncoder.encodePartitionKey(rowA, partitionSchema),
                      new byte[]{
                          0, 0, 0, 0,           // hash(0, "")
                          0, 0, 0, 0x14,        // hash("")
                          (byte) 0x80, 0, 0, 0, // a = 0
                          0, 0,                 // b = ""; c is elided
                      });

    PartialRow rowB = schema.newPartialRow();
    rowB.addInt("a", 1);
    rowB.addString("b", "");
    rowB.addString("c", "");
    assertBytesEquals(KeyEncoder.encodePartitionKey(rowB, partitionSchema),
                      new byte[]{
                          0, 0, 0, 0x5,         // hash(1, "")
                          0, 0, 0, 0x14,        // hash("")
                          (byte) 0x80, 0, 0, 1, // a = 0
                          0, 0,                 // b = ""; c is elided
                      });

    PartialRow rowC = schema.newPartialRow();
    rowC.addInt("a", 0);
    rowC.addString("b", "b");
    rowC.addString("c", "c");
    assertBytesEquals(KeyEncoder.encodePartitionKey(rowC, partitionSchema),
                      new byte[]{
                          0, 0, 0, 0x1A,        // hash(0, "b")
                          0, 0, 0, 0x1D,        // hash("c")
                          (byte) 0x80, 0, 0, 0, // a = 1
                          'b', 0, 0,            // b = "b"
                          'c'                   // b = "c"
                      });

    PartialRow rowD = schema.newPartialRow();
    rowD.addInt("a", 1);
    rowD.addString("b", "b");
    rowD.addString("c", "c");
    assertBytesEquals(KeyEncoder.encodePartitionKey(rowD, partitionSchema),
                      new byte[]{
                          0, 0, 0, 0,           // hash(1, "b")
                          0, 0, 0, 0x1D,        // hash("c")
                          (byte) 0x80, 0, 0, 1, // a = 0
                          'b', 0, 0,            // b = "b"
                          'c'                   // b = "c"
                      });
  }

  @Test(timeout = 100000)
  public void testAllPrimaryKeyTypes() throws Exception {
    Schema schema = buildSchema(
        new ColumnSchemaBuilder("int8", Type.INT8).key(true),
        new ColumnSchemaBuilder("int16", Type.INT16).key(true),
        new ColumnSchemaBuilder("int32", Type.INT32).key(true),
        new ColumnSchemaBuilder("int64", Type.INT64).key(true),
        new ColumnSchemaBuilder("string", Type.STRING).key(true),
        new ColumnSchemaBuilder("binary", Type.BINARY).key(true),
        new ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS).key(true),
        new ColumnSchemaBuilder("decimal32", Type.DECIMAL).key(true)
            .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL32_PRECISION, 0)),
        new ColumnSchemaBuilder("decimal64", Type.DECIMAL).key(true)
          .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL64_PRECISION, 0)),
        new ColumnSchemaBuilder("decimal128", Type.DECIMAL).key(true)
          .typeAttributes(DecimalUtil.typeAttributes(DecimalUtil.MAX_DECIMAL128_PRECISION, 0)),
        new ColumnSchemaBuilder("bool", Type.BOOL),       // not primary key type
        new ColumnSchemaBuilder("float", Type.FLOAT),     // not primary key type
        new ColumnSchemaBuilder("double", Type.DOUBLE));  // not primary key type

    KuduTable table = client.createTable("testAllPrimaryKeyTypes-" + System.currentTimeMillis(),
        schema, defaultCreateTableOptions(schema));
    KuduSession session = client.newSession();

    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addByte(0, (byte) 1);
    row.addShort(1, (short) 2);
    row.addInt(2, 3);
    row.addLong(3, 4l);
    row.addString(4, "foo");
    row.addBinary(5, "bar".getBytes(UTF_8));
    row.addLong(6, 6l);
    row.addDecimal(7, BigDecimal.valueOf(DecimalUtil.MAX_UNSCALED_DECIMAL32));
    row.addDecimal(8, BigDecimal.valueOf(DecimalUtil.MAX_UNSCALED_DECIMAL64));
    row.addDecimal(9, new BigDecimal(DecimalUtil.MAX_UNSCALED_DECIMAL128));
    row.addBoolean(10, true);
    row.addFloat(11, 8.8f);
    row.addDouble(12, 9.9);
    session.apply(insert);
    session.close();

    KuduScanner scanner = client.newScannerBuilder(table).build();
    while (scanner.hasMoreRows()) {
      RowResultIterator it = scanner.nextRows();
      assertTrue(it.hasNext());
      RowResult rr = it.next();
      assertEquals((byte) 0x01, rr.getByte(0));
      assertEquals((short) 2, rr.getShort(1));
      assertEquals(3, rr.getInt(2));
      assertEquals(4l, rr.getLong(3));
      assertBytesEquals(rr.getBinaryCopy(4), "foo");
      assertBytesEquals(rr.getBinaryCopy(5), "bar");
      assertEquals(6l, rr.getLong(6));
      assertTrue(BigDecimal.valueOf(DecimalUtil.MAX_UNSCALED_DECIMAL32)
          .compareTo(rr.getDecimal(7)) == 0);
      assertTrue(BigDecimal.valueOf(DecimalUtil.MAX_UNSCALED_DECIMAL64)
          .compareTo(rr.getDecimal(8)) == 0);
      assertTrue(new BigDecimal(DecimalUtil.MAX_UNSCALED_DECIMAL128)
          .compareTo(rr.getDecimal(9)) == 0);
      assertTrue(rr.getBoolean(10));
      assertEquals(8.8f, rr.getFloat(11), .001f);
      assertEquals(9.9, rr.getDouble(12), .001);
    }
  }
}
