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
import static org.junit.Assert.assertNotEquals;

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
    Assert.assertTrue(isKey.isKey());
    assertNotEquals(stringCol1, isKey);

    // Difference between key and nonUniqueKey
    ColumnSchema isNonUniqueKey = new ColumnSchemaBuilder("col1", Type.STRING)
        .nonUniqueKey(true)
        .build();
    Assert.assertTrue(isNonUniqueKey.isKey());
    Assert.assertFalse(isNonUniqueKey.isKeyUnique());
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
    Throwable thrown = Assert.assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder("col1", Type.VARCHAR)
                .typeAttributes(CharUtil.typeAttributes(70000)).build();
      }
    });
    Assert.assertTrue(thrown.getMessage()
            .contains("VARCHAR's length must be set and between 1 and 65535"));
  }

  @Test
  public void testVarcharWithoutLength() throws Exception {
    Throwable thrown = Assert.assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder("col1", Type.VARCHAR).build();
      }
    });
    Assert.assertTrue(thrown.getMessage()
            .contains("VARCHAR's length must be set and between 1 and 65535"));
  }

  @Test
  public void testAutoIncrementing() throws Exception {
    // Create auto-incrementing column with AutoIncrementingColumnSchemaBuilder
    ColumnSchema autoIncrementing = new AutoIncrementingColumnSchemaBuilder().build();
    Assert.assertTrue(autoIncrementing.isAutoIncrementing());
    assertEquals(Schema.getAutoIncrementingColumnType(), autoIncrementing.getType());
    Assert.assertTrue(autoIncrementing.isKey());
    Assert.assertFalse(autoIncrementing.isKeyUnique());
    Assert.assertFalse(autoIncrementing.isNullable());
    Assert.assertFalse(autoIncrementing.isImmutable());
    assertEquals(null, autoIncrementing.getDefaultValue());

    // Create column with auto-incrementing column name with ColumnSchemaBuilder
    Throwable thrown = Assert.assertThrows(IllegalArgumentException.class, new ThrowingRunnable() {
      @Override
      public void run() throws Exception {
        new ColumnSchemaBuilder(Schema.getAutoIncrementingColumnName(),
            Schema.getAutoIncrementingColumnType()).build();
      }
    });
    Assert.assertTrue(thrown.getMessage().contains("Column name " +
        Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
  }
}
