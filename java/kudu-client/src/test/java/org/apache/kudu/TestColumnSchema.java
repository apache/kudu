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

package org.apache.kudu;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import org.apache.kudu.ColumnSchema.AutoIncrementingColumnSchemaBuilder;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.util.CharUtil;
import org.apache.kudu.util.DecimalUtil;

public class TestColumnSchema {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testToString() {
    final ColumnSchema col1 = new ColumnSchemaBuilder("col1", Type.STRING).build();
    final ColumnSchema col2 = new ColumnSchemaBuilder("col2", Type.INT64).build();
    final ColumnSchema col3 = new ColumnSchemaBuilder("col3", Type.DECIMAL)
        .typeAttributes(DecimalUtil.typeAttributes(5, 2))
        .build();
    final ColumnSchema col4 = new ColumnSchemaBuilder("col4", Type.INT16)
        .comment("test comment").build();

    assertEquals("Column name: col1, type: string", col1.toString());
    assertEquals("Column name: col2, type: int64", col2.toString());
    assertEquals("Column name: col3, type: decimal(5, 2)", col3.toString());
    assertEquals("Column name: col4, type: int16, comment: test comment", col4.toString());
  }

  @Test
  public void testEquals() {
    ColumnSchema stringCol1 = new ColumnSchemaBuilder("col1", Type.STRING)
        .defaultValue("test")
        .build();
    // Same instance
    assertEquals(stringCol1, stringCol1);

    // Same value
    ColumnSchema stringCol2 = new ColumnSchemaBuilder("col1", Type.STRING)
        .defaultValue("test")
        .build();
    assertEquals(stringCol1, stringCol2);

    // Different by key
    ColumnSchema isKey = new ColumnSchemaBuilder("col1", Type.STRING)
        .key(true)
        .build();
    assertTrue(isKey.isKey());
    assertNotEquals(stringCol1, isKey);

    // Difference between key and nonUniqueKey
    ColumnSchema isNonUniqueKey = new ColumnSchemaBuilder("col1", Type.STRING)
        .nonUniqueKey(true)
        .build();
    assertTrue(isNonUniqueKey.isKey());
    assertFalse(isNonUniqueKey.isKeyUnique());
    assertNotEquals(isKey, isNonUniqueKey);

    // Different by type
    ColumnSchema isInt = new ColumnSchemaBuilder("col1", Type.INT32)
        .build();
    assertNotEquals(stringCol1, isInt);

    // Same with type attributes
    ColumnSchema decCol1 = new ColumnSchemaBuilder("col1", Type.DECIMAL)
        .typeAttributes(DecimalUtil.typeAttributes(9, 2))
        .build();
    ColumnSchema decCol2 = new ColumnSchemaBuilder("col1", Type.DECIMAL)
        .typeAttributes(DecimalUtil.typeAttributes(9, 2))
        .build();
    assertEquals(decCol1, decCol2);

    // Different by type attributes
    ColumnSchema decCol3 = new ColumnSchemaBuilder("col1", Type.DECIMAL)
        .typeAttributes(DecimalUtil.typeAttributes(9, 0))
        .build();
    assertNotEquals(decCol1, decCol3);

    // Same with comment
    ColumnSchema commentInt1 = new ColumnSchemaBuilder("col1", Type.INT32).comment("test").build();
    ColumnSchema commentInt2 = new ColumnSchemaBuilder("col1", Type.INT32).comment("test").build();
    assertEquals(commentInt1, commentInt2);

    // Different by comment
    ColumnSchema commentInt3 = new ColumnSchemaBuilder("col1", Type.INT32).comment("Test").build();
    assertNotEquals(commentInt1, commentInt3);
  }

  @Test
  public void testOutOfRangeVarchar() throws Exception {
    Throwable thrown = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder("col1", Type.VARCHAR)
                .typeAttributes(CharUtil.typeAttributes(70000)).build();
      }
    });
    assertTrue(thrown.getMessage()
            .contains("VARCHAR's length must be set and between 1 and 65535"));
  }

  @Test
  public void testVarcharWithoutLength() throws Exception {
    Throwable thrown = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder("col1", Type.VARCHAR).build();
      }
    });
    assertTrue(thrown.getMessage()
            .contains("VARCHAR's length must be set and between 1 and 65535"));
  }

  @Test
  public void testAutoIncrementing() throws Exception {
    // Create auto-incrementing column with AutoIncrementingColumnSchemaBuilder
    ColumnSchema autoIncrementing = new AutoIncrementingColumnSchemaBuilder().build();
    assertTrue(autoIncrementing.isAutoIncrementing());
    assertEquals(Schema.getAutoIncrementingColumnType(), autoIncrementing.getType());
    assertTrue(autoIncrementing.isKey());
    assertFalse(autoIncrementing.isKeyUnique());
    assertFalse(autoIncrementing.isNullable());
    assertFalse(autoIncrementing.isImmutable());
    assertEquals(null, autoIncrementing.getDefaultValue());

    // Create column with auto-incrementing column name with ColumnSchemaBuilder
    Throwable thrown = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder(Schema.getAutoIncrementingColumnName(),
            Schema.getAutoIncrementingColumnType()).build();
      }
    });
    assertTrue(thrown.getMessage().contains("Column name " +
        Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
  }

  @Test
  public void testArrayTypeColumn() {
    // STRING[]
    ColumnSchema strArrayCol = new ColumnSchema.ColumnSchemaBuilder("str_arr", Type.STRING)
        .array(true)      // promotes to NESTED with element type STRING
        .nullable(true)   // the array itself can be null
        .build();

    assertEquals("str_arr", strArrayCol.getName());
    assertEquals(Type.NESTED, strArrayCol.getType());
    assertTrue(strArrayCol.isNullable());
    assertEquals(Type.STRING,
        strArrayCol.getNestedTypeDescriptor().getArrayDescriptor().getElemType());

    // DECIMAL(10,4)[]
    ColumnTypeAttributes decimalAttrs = new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
         .precision(10)
         .scale(4)
         .build();

    ColumnSchema decimalArrayCol = new ColumnSchema.ColumnSchemaBuilder("dec_arr", Type.DECIMAL)
        .typeAttributes(decimalAttrs)
        .array(true)
        .nullable(true)
        .build();

    assertEquals("dec_arr", decimalArrayCol.getName());
    assertEquals(Type.NESTED, decimalArrayCol.getType());
    assertTrue(decimalArrayCol.isNullable());
    assertEquals(Type.DECIMAL,
        decimalArrayCol.getNestedTypeDescriptor().getArrayDescriptor().getElemType());

    // INT32[] (non-nullable)
    ColumnSchema intArrayCol = new ColumnSchema.ColumnSchemaBuilder("int_arr", Type.INT32)
        .array(true)
        .nullable(false)
        .build();

    assertEquals("int_arr", intArrayCol.getName());
    assertEquals(Type.NESTED, intArrayCol.getType());
    assertFalse(intArrayCol.isNullable());
    assertEquals(Type.INT32,
        intArrayCol.getNestedTypeDescriptor().getArrayDescriptor().getElemType());

    // VARCHAR(50)[]
    ColumnTypeAttributes varcharAttrs = new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
        .length(50)
        .build();

    ColumnSchema varcharArrayCol = new ColumnSchema.ColumnSchemaBuilder("varchar_arr", Type.VARCHAR)
        .typeAttributes(varcharAttrs)
        .array(true)
        .nullable(true)
        .build();

    assertEquals("varchar_arr", varcharArrayCol.getName());
    assertEquals(Type.NESTED, varcharArrayCol.getType());
    assertTrue(varcharArrayCol.isNullable());
    assertEquals(Type.VARCHAR,
        varcharArrayCol.getNestedTypeDescriptor().getArrayDescriptor().getElemType());

    // Test constructor restriction: cannot pass NESTED directly
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () ->
            new ColumnSchema.ColumnSchemaBuilder("nested", Type.NESTED)
    );
    assertTrue(thrown.getMessage().contains(
        "Column nested cannot be set to NESTED type. Use ColumnSchemaBuilder.array(true) instead"
    ));

  }

  @Test
  public void testColumnSchemaBuilderCopyConstructor() {
    // --- Scalar column setup ---
    ColumnSchema scalarCol =
        new ColumnSchema.ColumnSchemaBuilder("age", Type.INT32)
            .nullable(false)
            .key(true)
            .comment("scalar column")
            .build();

    ColumnSchemaBuilder scalarBuilderCopy = new ColumnSchemaBuilder(scalarCol);
    ColumnSchema scalarCopy = scalarBuilderCopy.build();
    // Verify the ColumnSchema objects are identical
    assertEquals(scalarCopy, scalarCol);


    // --- Array column setup ---
    ColumnSchema arrayCol =
        new ColumnSchema.ColumnSchemaBuilder("scores", Type.INT16)
            .array(true)
            .nullable(true)
            .comment("array column")
            .build();

    ColumnSchemaBuilder arrayBuilderCopy = new ColumnSchemaBuilder(arrayCol);
    ColumnSchema arrayCopy = arrayBuilderCopy.build();
    assertEquals(arrayCopy, arrayCol);
    // Ensure nestedTypeDescriptor and attributes are copied
    assertEquals(arrayCol.getNestedTypeDescriptor(), arrayCopy.getNestedTypeDescriptor());
    assertEquals(arrayCol.getTypeAttributes(), arrayCopy.getTypeAttributes());
  }

  @Test
  public void testArrayColumnWithDefaultValue() {
    Throwable thrown = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchema.ColumnSchemaBuilder("arr_col", Type.INT32)
            .array(true)
            .defaultValue(new int[]{1, 2, 3})
            .build();
      }
    });
    assertTrue(thrown.getMessage().contains("cannot have a default value"));
  }

  @Test
  public void testArrayColumnBeingKeyColumn() {
    Throwable thrown = assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchema.ColumnSchemaBuilder("arr_col", Type.INT32)
            .array(true)
            .key(true)
            .build();
      }
    });
    assertTrue(thrown.getMessage().contains("cannot be a key column"));
  }
}
