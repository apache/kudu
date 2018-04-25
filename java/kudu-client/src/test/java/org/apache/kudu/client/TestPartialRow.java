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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

public class TestPartialRow {

  @Test
  public void testGetters() {
    PartialRow partialRow = getPartialRowWithAllTypes();
    assertEquals(true, partialRow.getBoolean("bool"));
    assertEquals(42, partialRow.getByte("int8"));
    assertEquals(43, partialRow.getShort("int16"));
    assertEquals(44, partialRow.getInt("int32"));
    assertEquals(45, partialRow.getLong("int64"));
    assertEquals(1234567890, partialRow.getLong("timestamp"));
    assertEquals(52.35F, partialRow.getFloat("float"), 0.0f);
    assertEquals(53.35, partialRow.getDouble("double"), 0.0);
    assertEquals("fun with ütf\0", partialRow.getString("string"));
    assertArrayEquals(new byte[] { 0, 1, 2, 3, 4 }, partialRow.getBinaryCopy("binary-array"));
    assertArrayEquals(new byte[] { 5, 6, 7, 8, 9 }, partialRow.getBinaryCopy("binary-bytebuffer"));
    assertEquals(ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4 }), partialRow.getBinary("binary-array"));
    assertEquals(ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 }), partialRow.getBinary("binary-bytebuffer"));
    assertTrue(partialRow.isSet("null"));
    assertTrue(partialRow.isNull("null"));
    assertEquals(BigDecimal.valueOf(12345, 3), partialRow.getDecimal("decimal"));
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
    Schema schema = BaseKuduTest.getSchemaWithAllTypes();
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

  @Test
  public void testToString() {
    Schema schema = BaseKuduTest.getSchemaWithAllTypes();

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
    assertEquals(-Float.MAX_VALUE, partialRow.getFloat("float"), 0.0f);
    assertEquals(-Double.MAX_VALUE, partialRow.getDouble("double"), 0.0);
    assertEquals("", partialRow.getString("string"));
    assertArrayEquals(new byte[0], partialRow.getBinaryCopy("binary-array"));
    assertArrayEquals(new byte[0], partialRow.getBinaryCopy("binary-bytebuffer"));
    assertEquals(BigDecimal.valueOf(-99999, 3), partialRow.getDecimal("decimal"));
  }

  private int getColumnIndex(PartialRow partialRow, String columnName) {
    return partialRow.getSchema().getColumnIndex(columnName);
  }

  private PartialRow getPartialRowWithAllTypes() {
    Schema schema = BaseKuduTest.getSchemaWithAllTypes();
    // Ensure we aren't missing any types
    assertEquals(13, schema.getColumnCount());

    PartialRow row = schema.newPartialRow();
    row.addByte("int8", (byte) 42);
    row.addShort("int16", (short) 43);
    row.addInt("int32", 44);
    row.addLong("int64", 45);
    row.addLong("timestamp", 1234567890); // Fri, 13 Feb 2009 23:31:30 UTC
    row.addBoolean("bool", true);
    row.addFloat("float", 52.35F);
    row.addDouble("double", 53.35);
    row.addString("string", "fun with ütf\0");
    row.addBinary("binary-array", new byte[] { 0, 1, 2, 3, 4 });
    ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 });
    row.addBinary("binary-bytebuffer", binaryBuffer);
    row.setNull("null");
    row.addDecimal("decimal", BigDecimal.valueOf(12345, 3));
    return row;
  }

  // Shift the type one position to force the wrong type for all types.
  private Type getShiftedType(Type type) {
    int shiftedPosition = (type.ordinal() + 1) % Type.values().length;
    return Type.values()[shiftedPosition];
  }

  private Object callGetByName(PartialRow partialRow, String columnName, Type type) {
    switch (type) {
      case INT8: return partialRow.getByte(columnName);
      case INT16: return partialRow.getShort(columnName);
      case INT32: return partialRow.getInt(columnName);
      case INT64: return partialRow.getLong(columnName);
      case UNIXTIME_MICROS: return partialRow.getLong(columnName);
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
    switch (type) {
      case INT8: return partialRow.getByte(columnIndex);
      case INT16: return partialRow.getShort(columnIndex);
      case INT32: return partialRow.getInt(columnIndex);
      case INT64: return partialRow.getLong(columnIndex);
      case UNIXTIME_MICROS: return partialRow.getLong(columnIndex);
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
      case INT8: partialRow.addByte(columnName, (byte) 42); break;
      case INT16: partialRow.addShort(columnName, (short) 43); break;
      case INT32: partialRow.addInt(columnName, 44); break;
      case INT64: partialRow.addLong(columnName, 45); break;
      case UNIXTIME_MICROS: partialRow.addLong(columnName, 1234567890); break;
      case STRING: partialRow.addString(columnName, "fun with ütf\0"); break;
      case BINARY: partialRow.addBinary(columnName, new byte[] { 0, 1, 2, 3, 4 }); break;
      case FLOAT: partialRow.addFloat(columnName, 52.35F); break;
      case DOUBLE: partialRow.addDouble(columnName, 53.35); break;
      case BOOL: partialRow.addBoolean(columnName, true); break;
      case DECIMAL: partialRow.addDecimal(columnName, BigDecimal.valueOf(12345, 3)); break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void callAddByIndex(PartialRow partialRow, int columnIndex, Type type) {
    switch (type) {
      case INT8: partialRow.addByte(columnIndex, (byte) 42); break;
      case INT16: partialRow.addShort(columnIndex, (short) 43); break;
      case INT32: partialRow.addInt(columnIndex, 44); break;
      case INT64: partialRow.addLong(columnIndex, 45); break;
      case UNIXTIME_MICROS: partialRow.addLong(columnIndex, 1234567890); break;
      case STRING: partialRow.addString(columnIndex, "fun with ütf\0"); break;
      case BINARY: partialRow.addBinary(columnIndex, new byte[] { 0, 1, 2, 3, 4 }); break;
      case FLOAT: partialRow.addFloat(columnIndex, 52.35F); break;
      case DOUBLE: partialRow.addDouble(columnIndex, 53.35); break;
      case BOOL: partialRow.addBoolean(columnIndex, true); break;
      case DECIMAL: partialRow.addDecimal(columnIndex, BigDecimal.valueOf(12345, 3)); break;
      default:
        throw new UnsupportedOperationException();
    }
  }

}
