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
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.junit.RetryRule;

public class TestPartitionSchemaClasses {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testHashBucketSchemaEquals() {
    PartitionSchema.HashBucketSchema schema1 = new PartitionSchema.HashBucketSchema(
        Arrays.asList(1, 2), 16, 42);
    PartitionSchema.HashBucketSchema schema2 = new PartitionSchema.HashBucketSchema(
        Arrays.asList(1, 2), 16, 42);

    assertEquals(schema1, schema2);
    assertEquals(schema1.hashCode(), schema2.hashCode());

    // Different column IDs
    PartitionSchema.HashBucketSchema schema3 = new PartitionSchema.HashBucketSchema(
        Arrays.asList(1, 3), 16, 42);
    assertFalse(schema1.equals(schema3));
    // Note: hash codes can collide, but we expect them to be different in this case
    assertFalse(schema1.hashCode() == schema3.hashCode());

    // Different number of buckets
    PartitionSchema.HashBucketSchema schema4 = new PartitionSchema.HashBucketSchema(
        Arrays.asList(1, 2), 32, 42);
    assertFalse(schema1.equals(schema4));
    assertFalse(schema1.hashCode() == schema4.hashCode());

    // Different seed
    PartitionSchema.HashBucketSchema schema5 = new PartitionSchema.HashBucketSchema(
        Arrays.asList(1, 2), 16, 100);
    assertFalse(schema1.equals(schema5));
    assertFalse(schema1.hashCode() == schema5.hashCode());
  }

  @Test
  public void testRangeWithHashSchemaEquals() {
    Schema schema = new Schema(Collections.singletonList(
            new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build()));

    PartialRow lower = schema.newPartialRow();
    lower.addInt("key", 10);
    PartialRow upper = schema.newPartialRow();
    upper.addInt("key", 20);

    PartitionSchema.HashBucketSchema hashSchema = new PartitionSchema.HashBucketSchema(
            Collections.singletonList(0), 8, 0);

    // Test equality of identical objects
    PartitionSchema.RangeWithHashSchema range1 = new PartitionSchema.RangeWithHashSchema(
            lower, upper, Collections.singletonList(hashSchema));
    PartitionSchema.RangeWithHashSchema range2 = new PartitionSchema.RangeWithHashSchema(
            lower, upper, Collections.singletonList(hashSchema));
    assertEquals(range1, range2);
    assertEquals(range1.hashCode(), range2.hashCode());

    // Test inequality with different upper bound
    PartialRow differentUpper = schema.newPartialRow();
    differentUpper.addInt("key", 30);
    PartitionSchema.RangeWithHashSchema range3 = new PartitionSchema.RangeWithHashSchema(
            lower, differentUpper, Collections.singletonList(hashSchema));
    assertFalse(range1.equals(range3));

    // Test inequality with different hash schema
    PartitionSchema.HashBucketSchema differentHashSchema = new PartitionSchema.HashBucketSchema(
            Collections.singletonList(0), 16, 0);
    PartitionSchema.RangeWithHashSchema range4 = new PartitionSchema.RangeWithHashSchema(
            lower, upper, Collections.singletonList(differentHashSchema));
    assertFalse(range1.equals(range4));
  }
}
