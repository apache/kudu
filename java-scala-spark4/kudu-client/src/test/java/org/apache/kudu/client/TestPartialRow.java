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

import static org.apache.kudu.test.ClientTestUtil.getPartialRowWithAllTypes;
import static org.apache.kudu.test.ClientTestUtil.getSchemaWithAllTypes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.util.DateUtil;

public class TestPartialRow {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testGetters() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    assertEquals(true, partialRow.getBoolean("bool"));
    assertEquals(42, partialRow.getByte("int8"));
    assertEquals(43, partialRow.getShort("int16"));
    assertEquals(44, partialRow.getInt("int32"));
    assertEquals(45, partialRow.getLong("int64"));
    assertEquals(new Timestamp(1234567890), partialRow.getTimestamp("timestamp"));
    assertEquals(Date.valueOf(LocalDate.ofEpochDay(0)), partialRow.getDate("date"));
    assertEquals(52.35F, partialRow.getFloat("float"), 0.0f);
    assertEquals(53.35, partialRow.getDouble("double"), 0.0);
    assertEquals("fun with ütf\0", partialRow.getString("string"));
    assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 },
        partialRow.getBinaryCopy("binary-array"));
    assertArrayEquals(new byte[] { 5, 6, 7, 8, 9 },
        partialRow.getBinaryCopy("binary-bytebuffer"));
    assertEquals(ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 }),
        partialRow.getBinary("binary-array"));
    assertEquals(ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 }),
        partialRow.getBinary("binary-bytebuffer"));
    assertTrue(partialRow.isSet("null"));
    assertTrue(partialRow.isNull("null"));
    assertEquals(BigDecimal.valueOf(12345, 3),
        partialRow.getDecimal("decimal"));
  }

  @Test
  public void testGetObject() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    assertTrue(partialRow.getObject("bool") instanceof Boolean);
    assertEquals(true, partialRow.getObject("bool"));
    assertTrue(partialRow.getObject("int8") instanceof Byte);
    assertEquals((byte) 42, partialRow.getObject("int8"));
    assertTrue(partialRow.getObject("int16") instanceof Short);
    assertEquals((short)43, partialRow.getObject("int16"));
    assertTrue(partialRow.getObject("int32") instanceof Integer);
    assertEquals(44, partialRow.getObject("int32"));
    assertTrue(partialRow.getObject("int64") instanceof Long);
    assertEquals((long) 45, partialRow.getObject("int64"));
    assertTrue(partialRow.getObject("timestamp") instanceof Timestamp);
    assertEquals(new Timestamp(1234567890), partialRow.getObject("timestamp"));
    assertTrue(partialRow.getObject("date") instanceof Date);
    assertEquals(Date.valueOf(LocalDate.ofEpochDay(0)), partialRow.getObject("date"));
    assertTrue(partialRow.getObject("float") instanceof Float);
    assertEquals(52.35F, (float) partialRow.getObject("float"), 0.0f);
    assertTrue(partialRow.getObject("double") instanceof Double);
    assertEquals(53.35, (double) partialRow.getObject("double"), 0.0);
    assertTrue(partialRow.getObject("string") instanceof String);
    assertEquals("fun with ütf\0", partialRow.getObject("string"));
    assertTrue(partialRow.getObject("varchar") instanceof String);
    assertEquals("árvíztűrő ", partialRow.getObject("varchar"));
    assertTrue(partialRow.getObject("binary-array") instanceof byte[]);
    assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 },
        partialRow.getBinaryCopy("binary-array"));
    assertTrue(partialRow.getObject("binary-bytebuffer") instanceof byte[]);
    assertEquals(ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 }),
        partialRow.getBinary("binary-bytebuffer"));
    assertNull(partialRow.getObject("null"));
    assertTrue(partialRow.getObject("decimal") instanceof BigDecimal);
    assertEquals(BigDecimal.valueOf(12345, 3),
        partialRow.getObject("decimal"));
  }

  @Test
  public void testAddObject() {
    Schema schema = getSchemaWithAllTypes();
    // Ensure we aren't missing any types
    assertEquals(15, schema.getColumnCount());

    PartialRow row = schema.newPartialRow();
    row.addObject("int8", (byte) 42);
    row.addObject("int16", (short) 43);
    row.addObject("int32", 44);
    row.addObject("int64", 45L);
    row.addObject("timestamp", new Timestamp(1234567890));
    row.addObject("date", Date.valueOf(LocalDate.ofEpochDay(0)));
    row.addObject("bool", true);
    row.addObject("float", 52.35F);
    row.addObject("double", 53.35);
    row.addObject("string", "fun with ütf\0");
    row.addObject("varchar", "árvíztűrő tükörfúrógép");
    row.addObject("binary-array", new byte[] { 0, 1, 2, 3, 4 });
    ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 });
    row.addObject("binary-bytebuffer", binaryBuffer);
    row.addObject("null", null);
    row.addObject("decimal", BigDecimal.valueOf(12345, 3));

    PartialRow expected = getPartialRowWithAllTypes();
    for (ColumnSchema col : schema.getColumns()) {
      assertEquals(callGetByName(expected, col.getName(), col.getType()),
          callGetByName(row, col.getName(), col.getType()));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetNullColumn() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    assertTrue(partialRow.isSet("null"));
    assertTrue(partialRow.isNull("null"));
    partialRow.getString("null");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetNonNullableColumn() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.setNull("int32");
  }

  @Test
  public void testGetUnsetColumn() {
    Schema schema = getSchemaWithAllTypes();
    PartialRow partialRow = schema.newPartialRow();
    for (ColumnSchema columnSchema : schema.getColumns()) {
      assertFalse(partialRow.isSet("null"));
      assertFalse(partialRow.isNull("null"));
      try {
        callGetByName(partialRow, columnSchema.getName(), columnSchema.getType());
        fail("Expected IllegalArgumentException for type: " + columnSchema.getType());
      } catch (IllegalArgumentException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testGetMissingColumnName() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callGetByName(partialRow, "not-a-column", columnSchema.getType());
        fail("Expected IllegalArgumentException for type: " + columnSchema.getType());
      } catch (IllegalArgumentException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testGetMissingColumnIndex() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callGetByIndex(partialRow, 999, columnSchema.getType());
        fail("Expected IndexOutOfBoundsException for type: " + columnSchema.getType());
      } catch (IndexOutOfBoundsException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testGetWrongTypeColumn() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        // Skip the null column because `isNull` is not type specific.
        if ("null".equals(columnSchema.getName())) {
          continue;
        }
        callGetByName(partialRow, columnSchema.getName(), getShiftedType(columnSchema.getType()));
        fail("Expected IllegalArgumentException for type: " + columnSchema.getType());
      } catch (IllegalArgumentException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testAddMissingColumnName() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callAddByName(partialRow, "not-a-column", columnSchema.getType());
        fail("Expected IllegalArgumentException for type: " + columnSchema.getType());
      } catch (IllegalArgumentException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testAddMissingColumnIndex() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callAddByIndex(partialRow, 999, columnSchema.getType());
        fail("Expected IndexOutOfBoundsException for type: " + columnSchema.getType());
      } catch (IndexOutOfBoundsException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testAddWrongTypeColumn() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callAddByName(partialRow, columnSchema.getName(), getShiftedType(columnSchema.getType()));
        fail("Expected IllegalArgumentException for type: " + columnSchema.getType());
      } catch (IllegalArgumentException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test
  public void testAddToFrozenRow() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.freeze();
    for (ColumnSchema columnSchema : partialRow.getSchema().getColumns()) {
      try {
        callAddByName(partialRow, columnSchema.getName(), columnSchema.getType());
        fail("Expected IllegalStateException for type: " + columnSchema.getType());
      } catch (IllegalStateException ex) {
        // This is the expected exception.
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsNullMissingColumnName() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.isNull("not-a-column");
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testIsNullMissingColumnIndex() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.isNull(999);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIsSetMissingColumnName() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.isSet("not-a-column");
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testIsSetMissingColumnIndex() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.isSet(999);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidPrecisionDecimal() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.addDecimal("decimal", BigDecimal.valueOf(123456, 3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidScaleDecimal() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.addDecimal("decimal", BigDecimal.valueOf(12345, 4));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddInvalidCoercedScaleDecimal() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.addDecimal("decimal", BigDecimal.valueOf(12345, 2));
  }

  @Test
  public void testAddCoercedScaleAndPrecisionDecimal() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    partialRow.addDecimal("decimal", BigDecimal.valueOf(222, 1));
    BigDecimal decimal = partialRow.getDecimal("decimal");
    assertEquals("22.200", decimal.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddDateOutOfRange() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    Date d = Date.valueOf(LocalDate.of(10000, 1, 1));
    partialRow.addDate("date", d);
  }

  @Test
  public void testToString() {
    Schema schema = getSchemaWithAllTypes();

    PartialRow row = schema.newPartialRow();
    assertEquals("()", row.toString());

    row.addInt("int32", 42);
    row.addByte("int8", (byte) 42);

    assertEquals("(int8 int8=42, int32 int32=42)", row.toString());

    row.addString("string", "fun with ütf\0");
    assertEquals("(int8 int8=42, int32 int32=42, string string=\"fun with ütf\\0\")",
                 row.toString());

    ByteBuffer binary = ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    binary.position(2);
    binary.limit(5);

    row.addBinary("binary-bytebuffer", binary);
    assertEquals("(int8 int8=42, int32 int32=42, string string=\"fun with ütf\\0\", " +
                     "binary binary-bytebuffer=[2, 3, 4])",
                 row.toString());

    row.addDouble("double", 52.35);
    assertEquals("(int8 int8=42, int32 int32=42, double double=52.35, " +
                     "string string=\"fun with ütf\\0\", binary binary-bytebuffer=[2, 3, 4])",
                 row.toString());

    row.addDecimal("decimal", BigDecimal.valueOf(12345, 3));
    assertEquals("(int8 int8=42, int32 int32=42, double double=52.35, " +
            "string string=\"fun with ütf\\0\", binary binary-bytebuffer=[2, 3, 4], " +
            "decimal(5, 3) decimal=12.345)",
        row.toString());

    row.addVarchar("varchar", "árvíztűrő tükörfúrógép");
    assertEquals("(int8 int8=42, int32 int32=42, double double=52.35, " +
        "string string=\"fun with ütf\\0\", binary binary-bytebuffer=[2, 3, 4], " +
        "decimal(5, 3) decimal=12.345, varchar(10) varchar=\"árvíztűrő \")",
        row.toString());

    PartialRow row2 = schema.newPartialRow();
    assertEquals("()", row2.toString());
    row2.addDate("date", Date.valueOf(LocalDate.ofEpochDay(0)));
    assertEquals("(date date=1970-01-01)", row2.toString());
  }

  @Test
  public void testIncrementColumn() {
    PartialRow partialRow = getPartialRowWithAllTypes();

    // Boolean
    int boolIndex = getColumnIndex(partialRow, "bool");
    partialRow.addBoolean(boolIndex, false);
    assertTrue(partialRow.incrementColumn(boolIndex));
    assertEquals(true, partialRow.getBoolean(boolIndex));
    assertFalse(partialRow.incrementColumn(boolIndex));

    // Int8
    int int8Index = getColumnIndex(partialRow, "int8");
    partialRow.addByte(int8Index, (byte)(Byte.MAX_VALUE - 1));
    assertTrue(partialRow.incrementColumn(int8Index));
    assertEquals(Byte.MAX_VALUE, partialRow.getByte(int8Index));
    assertFalse(partialRow.incrementColumn(int8Index));

    // Int16
    int int16Index = getColumnIndex(partialRow, "int16");
    partialRow.addShort(int16Index, (short)(Short.MAX_VALUE - 1));
    assertTrue(partialRow.incrementColumn(int16Index));
    assertEquals(Short.MAX_VALUE, partialRow.getShort(int16Index));
    assertFalse(partialRow.incrementColumn(int16Index));

    // Int32
    int int32Index = getColumnIndex(partialRow, "int32");
    partialRow.addInt(int32Index, Integer.MAX_VALUE - 1);
    assertTrue(partialRow.incrementColumn(int32Index));
    assertEquals(Integer.MAX_VALUE, partialRow.getInt(int32Index));
    assertFalse(partialRow.incrementColumn(int32Index));

    // Int64
    int int64Index = getColumnIndex(partialRow, "int64");
    partialRow.addLong(int64Index, Long.MAX_VALUE - 1);
    assertTrue(partialRow.incrementColumn(int64Index));
    assertEquals(Long.MAX_VALUE, partialRow.getLong(int64Index));
    assertFalse(partialRow.incrementColumn(int64Index));

    // Float
    int floatIndex = getColumnIndex(partialRow, "float");
    partialRow.addFloat(floatIndex, Float.MAX_VALUE);
    assertTrue(partialRow.incrementColumn(floatIndex));
    assertEquals(Float.POSITIVE_INFINITY, partialRow.getFloat(floatIndex), 0.0f);
    assertFalse(partialRow.incrementColumn(floatIndex));

    // Float
    int doubleIndex = getColumnIndex(partialRow, "double");
    partialRow.addDouble(doubleIndex, Double.MAX_VALUE);
    assertTrue(partialRow.incrementColumn(doubleIndex));
    assertEquals(Double.POSITIVE_INFINITY, partialRow.getDouble(doubleIndex), 0.0);
    assertFalse(partialRow.incrementColumn(doubleIndex));

    // Decimal
    int decimalIndex = getColumnIndex(partialRow, "decimal");
    // Decimal with precision 5, scale 3 has a max of 99.999
    partialRow.addDecimal(decimalIndex, new BigDecimal("99.998"));
    assertTrue(partialRow.incrementColumn(decimalIndex));
    assertEquals(new BigDecimal("99.999"), partialRow.getDecimal(decimalIndex));
    assertFalse(partialRow.incrementColumn(decimalIndex));

    // String
    int stringIndex = getColumnIndex(partialRow, "string");
    partialRow.addString(stringIndex, "hello");
    assertTrue(partialRow.incrementColumn(stringIndex));
    assertEquals("hello\0", partialRow.getString(stringIndex));

    // Binary
    int binaryIndex = getColumnIndex(partialRow, "binary-array");
    partialRow.addBinary(binaryIndex, new byte[] { 0, 1, 2, 3, 4 });
    assertTrue(partialRow.incrementColumn(binaryIndex));
    assertArrayEquals(new byte[] { 0, 1, 2, 3, 4, 0 }, partialRow.getBinaryCopy(binaryIndex));

    // Varchar
    int varcharIndex = getColumnIndex(partialRow, "varchar");
    partialRow.addVarchar(varcharIndex, "hello");
    assertTrue(partialRow.incrementColumn(varcharIndex));
    assertEquals("hello\0", partialRow.getVarchar(varcharIndex));

    // Date
    int dateIndex = getColumnIndex(partialRow, "date");
    partialRow.addDate(dateIndex, DateUtil.epochDaysToSqlDate(DateUtil.MAX_DATE_VALUE - 1));
    assertTrue(partialRow.incrementColumn(dateIndex));
    Date maxDate = DateUtil.epochDaysToSqlDate(DateUtil.MAX_DATE_VALUE);
    assertEquals(maxDate, partialRow.getDate(dateIndex));
    assertFalse(partialRow.incrementColumn(dateIndex));
  }

  @Test
  public void testSetMin() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    for (int i = 0; i < partialRow.getSchema().getColumnCount(); i++) {
      partialRow.setMin(i);
    }
    assertEquals(false, partialRow.getBoolean("bool"));
    assertEquals(Byte.MIN_VALUE, partialRow.getByte("int8"));
    assertEquals(Short.MIN_VALUE, partialRow.getShort("int16"));
    assertEquals(Integer.MIN_VALUE, partialRow.getInt("int32"));
    assertEquals(Long.MIN_VALUE, partialRow.getLong("int64"));
    assertEquals(Long.MIN_VALUE, partialRow.getLong("timestamp"));
    assertEquals(DateUtil.epochDaysToSqlDate(DateUtil.MIN_DATE_VALUE), partialRow.getDate("date"));
    assertEquals(-Float.MAX_VALUE, partialRow.getFloat("float"), 0.0f);
    assertEquals(-Double.MAX_VALUE, partialRow.getDouble("double"), 0.0);
    assertEquals("", partialRow.getString("string"));
    assertEquals("", partialRow.getVarchar("varchar"));
    assertArrayEquals(new byte[0], partialRow.getBinaryCopy("binary-array"));
    assertArrayEquals(new byte[0], partialRow.getBinaryCopy("binary-bytebuffer"));
    assertEquals(BigDecimal.valueOf(-99999, 3), partialRow.getDecimal("decimal"));
  }

  private int getColumnIndex(PartialRow partialRow, String columnName) {
    return partialRow.getSchema().getColumnIndex(columnName);
  }

  // Shift the type one position to force the wrong type for all types.
  private Type getShiftedType(Type type) {
    int shiftedPosition = (type.ordinal() + 1) % Type.values().length;
    return Type.values()[shiftedPosition];
  }

  private Object callGetByName(PartialRow partialRow, String columnName, Type type) {
    if (partialRow.isNull(columnName)) {
      return null;
    }
    switch (type) {
      case INT8: return partialRow.getByte(columnName);
      case INT16: return partialRow.getShort(columnName);
      case INT32: return partialRow.getInt(columnName);
      case INT64: return partialRow.getLong(columnName);
      case DATE: return partialRow.getDate(columnName);
      case UNIXTIME_MICROS: return partialRow.getTimestamp(columnName);
      case VARCHAR: return partialRow.getVarchar(columnName);
      case STRING: return partialRow.getString(columnName);
      case BINARY: return partialRow.getBinary(columnName);
      case FLOAT: return partialRow.getFloat(columnName);
      case DOUBLE: return partialRow.getDouble(columnName);
      case BOOL: return partialRow.getBoolean(columnName);
      case DECIMAL: return partialRow.getDecimal(columnName);
      default:
        throw new UnsupportedOperationException();
    }
  }

  private Object callGetByIndex(PartialRow partialRow, int columnIndex, Type type) {
    if (partialRow.isNull(columnIndex)) {
      return null;
    }
    switch (type) {
      case INT8: return partialRow.getByte(columnIndex);
      case INT16: return partialRow.getShort(columnIndex);
      case INT32: return partialRow.getInt(columnIndex);
      case INT64: return partialRow.getLong(columnIndex);
      case DATE: return partialRow.getDate(columnIndex);
      case UNIXTIME_MICROS: return partialRow.getTimestamp(columnIndex);
      case VARCHAR: return partialRow.getVarchar(columnIndex);
      case STRING: return partialRow.getString(columnIndex);
      case BINARY: return partialRow.getBinary(columnIndex);
      case FLOAT: return partialRow.getFloat(columnIndex);
      case DOUBLE: return partialRow.getDouble(columnIndex);
      case BOOL: return partialRow.getBoolean(columnIndex);
      case DECIMAL: return partialRow.getDecimal(columnIndex);
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void callAddByName(PartialRow partialRow, String columnName, Type type) {
    switch (type) {
      case INT8:
        partialRow.addByte(columnName, (byte) 42);
        break;
      case INT16:
        partialRow.addShort(columnName, (short) 43);
        break;
      case INT32:
        partialRow.addInt(columnName, 44);
        break;
      case INT64:
        partialRow.addLong(columnName, 45);
        break;
      case UNIXTIME_MICROS:
        partialRow.addTimestamp(columnName, new Timestamp(1234567890));
        break;
      case VARCHAR:
        partialRow.addVarchar(columnName, "fun with ütf\0");
        break;
      case STRING:
        partialRow.addString(columnName, "fun with ütf\0");
        break;
      case BINARY:
        partialRow.addBinary(columnName, new byte[] { 0, 1, 2, 3, 4 });
        break;
      case FLOAT:
        partialRow.addFloat(columnName, 52.35F);
        break;
      case DOUBLE:
        partialRow.addDouble(columnName, 53.35);
        break;
      case BOOL:
        partialRow.addBoolean(columnName, true);
        break;
      case DECIMAL:
        partialRow.addDecimal(columnName, BigDecimal.valueOf(12345, 3));
        break;
      case DATE:
        partialRow.addDate(columnName, DateUtil.epochDaysToSqlDate(0));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void callAddByIndex(PartialRow partialRow, int columnIndex, Type type) {
    switch (type) {
      case INT8:
        partialRow.addByte(columnIndex, (byte) 42);
        break;
      case INT16:
        partialRow.addShort(columnIndex, (short) 43);
        break;
      case INT32:
        partialRow.addInt(columnIndex, 44);
        break;
      case INT64:
        partialRow.addLong(columnIndex, 45);
        break;
      case UNIXTIME_MICROS:
        partialRow.addTimestamp(columnIndex, new Timestamp(1234567890));
        break;
      case VARCHAR:
        partialRow.addVarchar(columnIndex, "fun with ütf\0");
        break;
      case STRING:
        partialRow.addString(columnIndex, "fun with ütf\0");
        break;
      case BINARY:
        partialRow.addBinary(columnIndex, new byte[] { 0, 1, 2, 3, 4 });
        break;
      case FLOAT:
        partialRow.addFloat(columnIndex, 52.35F);
        break;
      case DOUBLE:
        partialRow.addDouble(columnIndex, 53.35);
        break;
      case BOOL:
        partialRow.addBoolean(columnIndex, true);
        break;
      case DECIMAL:
        partialRow.addDecimal(columnIndex, BigDecimal.valueOf(12345, 3));
        break;
      case DATE:
        partialRow.addDate(columnIndex, DateUtil.epochDaysToSqlDate(0));
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

}
