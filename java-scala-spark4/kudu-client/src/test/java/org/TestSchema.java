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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

public class TestSchema {

  @Test
  public void testEquals() {
    final ColumnSchema col1 = new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32)
                                              .nullable(false)
                                              .key(true)
                                              .build();
    final ColumnSchema col2 = new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
                                              .nullable(false)
                                              .build();

    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(col1);
    columns.add(col2);
    final Schema schema = new Schema(columns);

    ArrayList<ColumnSchema> columns1 = new ArrayList<>();
    columns1.add(col1);
    columns1.add(col2);
    final Schema schema1 = new Schema(columns1);

    // Two objects are the same.
    assertTrue(schema1.equals(schema1));
    // One of object is not type of 'Schema'.
    assertFalse(schema1.equals(columns1));
    // Two schemas are the same structure.
    assertTrue(schema1.equals(schema));

    final ColumnSchema col3 = new ColumnSchema.ColumnSchemaBuilder("c2", Type.INT32)
                                              .nullable(false)
                                              .key(true)
                                              .build();

    ArrayList<ColumnSchema> columns2 = new ArrayList<>();
    columns2.add(col1);
    columns2.add(col3);
    final Schema schema2 = new Schema(columns2);

    // Two schemas have different number of primary keys.
    assertFalse(schema1.equals(schema2));

    ArrayList<ColumnSchema> columns3 = new ArrayList<>();
    columns3.add(col1);
    columns3.add(col2);
    columns3.add(col3);
    final Schema schema3 = new Schema(columns3);

    // Two schemas have different number of columns.
    assertFalse(schema1.equals(schema3));

    final ColumnSchema col4 = new ColumnSchema.ColumnSchemaBuilder("c3", Type.INT32)
                                              .nullable(false)
                                              .build();

    ArrayList<ColumnSchema> columns4 = new ArrayList<>();
    columns4.add(col1);
    columns4.add(col2);
    columns4.add(col4);
    final Schema schema4 = new Schema(columns4);

    final ColumnSchema col5 = new ColumnSchema.ColumnSchemaBuilder("c4", Type.INT32)
                                              .nullable(false)
                                              .build();
    ArrayList<ColumnSchema> columns5 = new ArrayList<>();
    columns5.add(col1);
    columns5.add(col2);
    columns5.add(col5);
    final Schema schema5 = new Schema(columns5);

    // Two schemas have different column names.
    assertFalse(schema4.equals(schema5));

    final ColumnSchema col6 = new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING)
                                              .nullable(false)
                                              .build();

    ArrayList<ColumnSchema> columns6 = new ArrayList<>();
    columns6.add(col1);
    columns6.add(col2);
    columns6.add(col6);
    final Schema schema6 = new Schema(columns6);

    // Two schemas have different column types.
    assertFalse(schema5.equals(schema6));

    ArrayList<ColumnSchema> columns7 = new ArrayList<>();
    columns7.add(col1);
    columns7.add(col6);
    columns7.add(col2);
    final Schema schema7 = new Schema(columns7);

    // Two schemas have different sequence of columns.
    assertFalse(schema6.equals(schema7));

    final ColumnSchema col7 = new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32)
                                              .nullable(true)
                                              .build();
    // Two column schemas with exact the same types, names, sequence of columns
    // but different nullability for a non-key column
    ArrayList<ColumnSchema> columns8 = new ArrayList<>();
    columns7.add(col1);
    columns7.add(col6);
    columns7.add(col7);
    final Schema schema8 = new Schema(columns8);

    assertFalse(schema7.equals(schema8));
  }
}
