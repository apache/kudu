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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.kudu.Schema;

public class TestPartialRow {

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
}
