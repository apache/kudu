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

import java.nio.ByteBuffer;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.junit.Test;

import org.apache.kudu.Schema;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
  }

  private PartialRow getPartialRowWithAllTypes() {
    Schema schema = BaseKuduTest.getSchemaWithAllTypes();
    // Ensure we aren't missing any types
    assertEquals(12, schema.getColumnCount());

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
      default:
        throw new NotImplementedException();
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
      default:
        throw new NotImplementedException();
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
      default:
        throw new NotImplementedException();
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
      default:
        throw new NotImplementedException();
    }
  }

}
