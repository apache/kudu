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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.RowOperations.RowOperationsPB;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Operation.ChangeType;
import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.tserver.Tserver.WriteRequestPBOrBuilder;
import org.apache.kudu.util.CharUtil;
import org.apache.kudu.util.DateUtil;

/**
 * Unit tests for Operation
 */
public class TestOperation {

  @Rule
  public RetryRule retryRule = new RetryRule();

  private Schema createManyStringsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING).nullable(true).build());
    return new Schema(columns);
  }

  @Test
  public void testSetStrings() {
    KuduTable table = Mockito.mock(KuduTable.class);
    Mockito.doReturn(createManyStringsSchema()).when(table).getSchema();
    Insert insert = new Insert(table);
    PartialRow row = insert.getRow();
    row.addString("c0", "c0_val");
    row.addString("c2", "c2_val");
    row.addString("c1", "c1_val");
    row.addString("c3", "c3_val");
    row.addString("c4", "c4_val");

    {
      WriteRequestPBOrBuilder pb =
          Operation.createAndFillWriteRequestPB(ImmutableList.of(insert));
      RowOperationsPB rowOps = pb.getRowOperations();
      assertEquals(6 * 5, rowOps.getIndirectData().size());
      assertEquals("c0_valc1_valc2_valc3_valc4_val", rowOps.getIndirectData().toStringUtf8());
      byte[] rows = rowOps.getRows().toByteArray();
      assertEquals(ChangeType.INSERT.toEncodedByte(), rows[0]);
      // The "isset" bitset should have 5 bits set
      assertEquals(0x1f, rows[1]);
      // The "null" bitset should have no bits set
      assertEquals(0, rows[2]);

      // Check the strings.
      int offset = 3;
      for (long i = 0; i <= 4; i++) {
        // The offset into the indirect buffer
        assertEquals(6L * i, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
        // The length of the pointed-to string.
        assertEquals(6L, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
      }

      // Should have used up whole buffer.
      assertEquals(rows.length, offset);
    }

    // Setting a field to NULL should add to the null bitmap and remove
    // the old value from the indirect buffer.
    row.setNull("c3");
    {
      WriteRequestPBOrBuilder pb =
          Operation.createAndFillWriteRequestPB(ImmutableList.of(insert));
      RowOperationsPB rowOps = pb.getRowOperations();
      assertEquals(6 * 4, rowOps.getIndirectData().size());
      assertEquals("c0_valc1_valc2_valc4_val", rowOps.getIndirectData().toStringUtf8());
      byte[] rows = rowOps.getRows().toByteArray();
      assertEquals(ChangeType.INSERT.toEncodedByte(), rows[0]);
      // The "isset" bitset should have 5 bits set
      assertEquals(0x1f, rows[1]);
      // The "null" bitset should have 1 bit set for the null column
      assertEquals(1 << 3, rows[2]);

      // Check the strings.
      int offset = 3;
      int indirOffset = 0;
      for (int i = 0; i <= 4; i++) {
        if (i == 3) {
          continue;
        }
        // The offset into the indirect buffer
        assertEquals(indirOffset, Bytes.getLong(rows, offset));
        indirOffset += 6;
        offset += Longs.BYTES;
        // The length of the pointed-to string.
        assertEquals(6, Bytes.getLong(rows, offset));
        offset += Longs.BYTES;
      }
      // Should have used up whole buffer.
      assertEquals(rows.length, offset);
    }
  }

  private Schema createAllTypesKeySchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(7);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT8).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT16).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.INT64).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.UNIXTIME_MICROS).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c5", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c6", Type.BINARY).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c7", Type.DATE).key(true).build());
    return new Schema(columns);
  }

  @Test
  public void testRowKeyStringify() {
    KuduTable table = Mockito.mock(KuduTable.class);
    Mockito.doReturn(createAllTypesKeySchema()).when(table).getSchema();
    Insert insert = new Insert(table);
    PartialRow row = insert.getRow();
    row.addByte("c0", (byte) 1);
    row.addShort("c1", (short) 2);
    row.addInt("c2", 3);
    row.addLong("c3", 4);
    row.addLong("c4", 5);
    row.addString("c5", "c5_val");
    row.addBinary("c6", Bytes.fromString("c6_val"));
    row.addDate("c7", DateUtil.epochDaysToSqlDate(0));

    assertEquals("(int8 c0=1, int16 c1=2, int32 c2=3, int64 c3=4, " +
                 "unixtime_micros c4=1970-01-01T00:00:00.000005Z, string c5=\"c5_val\", " +
                 "binary c6=\"c6_val\", date c7=1970-01-01)",
                 insert.getRow().stringifyRowKey());

    // Test an incomplete row key.
    insert = new Insert(table);
    row = insert.getRow();
    row.addByte("c0", (byte) 1);
    try {
      row.stringifyRowKey();
      fail("Should not be able to stringifyRowKey when not all keys are specified");
    } catch (IllegalStateException ise) {
      // Expected.
    }
  }

  @Test
  public void testEncodeDecodeRangeSimpleTypes() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT64).build());
    final Schema schema = new Schema(columns);

    final PartialRow lower = schema.newPartialRow();
    lower.addInt("c0", 0);

    final PartialRow upper = schema.newPartialRow();
    upper.addInt("c0", 100);

    final Operation.OperationsEncoder enc = new Operation.OperationsEncoder();
    final RowOperationsPB encoded = enc.encodeLowerAndUpperBounds(
        lower, upper, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND);

    Operation.OperationsDecoder dec = new Operation.OperationsDecoder();
    List<CreateTableOptions.RangePartition> decoded =
        dec.decodeRangePartitions(encoded, schema);
    assertEquals(1, decoded.size());
    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(0).getLowerBoundType());
    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(0).getUpperBoundType());
    final PartialRow lowerDecoded = decoded.get(0).getLowerBound();
    final PartialRow upperDecoded = decoded.get(0).getUpperBound();

    assertTrue(lowerDecoded.isSet("c0"));
    assertEquals(0, lowerDecoded.getInt("c0"));
    assertFalse(lowerDecoded.isSet("c1"));
    assertEquals(lower.toString(), lowerDecoded.toString());

    assertTrue(upperDecoded.isSet("c0"));
    assertEquals(100, upperDecoded.getInt("c0"));
    assertFalse(upperDecoded.isSet("c1"));
    assertEquals(upper.toString(), upperDecoded.toString());
  }

  @Test
  public void testEncodeDecodeRangeStringTypes() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.VARCHAR)
        .nullable(true)
        .typeAttributes(CharUtil.typeAttributes(10))
        .build());
    final Schema schema = new Schema(columns);

    final PartialRow lower = schema.newPartialRow();
    lower.addString("c0", "a");

    final PartialRow upper = schema.newPartialRow();
    upper.addString("c0", "b");

    final Operation.OperationsEncoder enc = new Operation.OperationsEncoder();
    final RowOperationsPB encoded = enc.encodeLowerAndUpperBounds(
        lower, upper, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND);

    Operation.OperationsDecoder dec = new Operation.OperationsDecoder();
    List<CreateTableOptions.RangePartition> decoded =
        dec.decodeRangePartitions(encoded, schema);
    assertEquals(1, decoded.size());
    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(0).getLowerBoundType());
    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(0).getUpperBoundType());
    final PartialRow lowerDecoded = decoded.get(0).getLowerBound();
    final PartialRow upperDecoded = decoded.get(0).getUpperBound();

    assertTrue(lowerDecoded.isSet("c0"));
    assertEquals("a", lowerDecoded.getString("c0"));
    assertFalse(lowerDecoded.isSet("c1"));
    assertFalse(lowerDecoded.isSet("c2"));
    assertEquals(lower.toString(), lowerDecoded.toString());

    assertTrue(upperDecoded.isSet("c0"));
    assertEquals("b", upperDecoded.getString("c0"));
    assertFalse(upperDecoded.isSet("c1"));
    assertFalse(upperDecoded.isSet("c2"));
    assertEquals(upper.toString(), upperDecoded.toString());
  }

  @Test
  public void testEncodeDecodeRangeMixedTypes() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0i", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1s", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2i", Type.INT64).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3s", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4i", Type.INT16).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c5s", Type.BINARY).nullable(true).build());
    final Schema schema = new Schema(columns);

    final PartialRow lower = schema.newPartialRow();
    lower.addInt("c0i", 0);
    lower.addString("c1s", "a");
    lower.addLong("c2i", -10);
    lower.addString("c3s", "A");
    lower.addShort("c4i", (short)-100);

    final PartialRow upper = schema.newPartialRow();
    upper.addInt("c0i", 1);
    upper.addString("c1s", "b");
    upper.addLong("c2i", 10);
    upper.addString("c3s", "B");
    upper.addShort("c4i", (short)100);

    final Operation.OperationsEncoder enc = new Operation.OperationsEncoder();
    final RowOperationsPB encoded = enc.encodeLowerAndUpperBounds(
        lower, upper, RangePartitionBound.INCLUSIVE_BOUND, RangePartitionBound.EXCLUSIVE_BOUND);

    Operation.OperationsDecoder dec = new Operation.OperationsDecoder();
    List<CreateTableOptions.RangePartition> decoded =
        dec.decodeRangePartitions(encoded, schema);
    assertEquals(1, decoded.size());
    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(0).getLowerBoundType());
    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(0).getUpperBoundType());
    final PartialRow lowerDecoded = decoded.get(0).getLowerBound();
    final PartialRow upperDecoded = decoded.get(0).getUpperBound();

    assertTrue(lowerDecoded.isSet("c0i"));
    assertEquals(0, lowerDecoded.getInt("c0i"));
    assertTrue(lowerDecoded.isSet("c1s"));
    assertEquals("a", lowerDecoded.getString("c1s"));
    assertTrue(lowerDecoded.isSet("c2i"));
    assertEquals(-10, lowerDecoded.getLong("c2i"));
    assertTrue(lowerDecoded.isSet("c3s"));
    assertEquals("A", lowerDecoded.getString("c3s"));
    assertTrue(lowerDecoded.isSet("c4i"));
    assertEquals(-100, lowerDecoded.getShort("c4i"));
    assertFalse(lowerDecoded.isSet("c5s"));
    assertEquals(lower.toString(), lowerDecoded.toString());

    assertTrue(upperDecoded.isSet("c0i"));
    assertEquals(1, upperDecoded.getInt("c0i"));
    assertTrue(upperDecoded.isSet("c1s"));
    assertEquals("b", upperDecoded.getString("c1s"));
    assertTrue(upperDecoded.isSet("c2i"));
    assertEquals(10, upperDecoded.getLong("c2i"));
    assertTrue(upperDecoded.isSet("c3s"));
    assertEquals("B", upperDecoded.getString("c3s"));
    assertTrue(upperDecoded.isSet("c4i"));
    assertEquals(100, upperDecoded.getShort("c4i"));
    assertFalse(upperDecoded.isSet("c5s"));
    assertEquals(upper.toString(), upperDecoded.toString());
  }

  @Test
  public void testEncodeDecodeMultipleRangePartitions() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT64).build());
    final Schema schema = new Schema(columns);

    List<CreateTableOptions.RangePartition> rangePartitions = new ArrayList<>();
    {
      final PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 0);

      final PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      rangePartitions.add(new CreateTableOptions.RangePartition(
          lower,
          upper,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.EXCLUSIVE_BOUND));
    }
    {
      final PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 200);

      final PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 300);
      rangePartitions.add(new CreateTableOptions.RangePartition(
          lower,
          upper,
          RangePartitionBound.EXCLUSIVE_BOUND,
          RangePartitionBound.INCLUSIVE_BOUND));
    }

    final Operation.OperationsEncoder enc = new Operation.OperationsEncoder();
    final RowOperationsPB encoded = enc.encodeRangePartitions(
        rangePartitions, ImmutableList.of());

    Operation.OperationsDecoder dec = new Operation.OperationsDecoder();
    List<CreateTableOptions.RangePartition> decoded =
        dec.decodeRangePartitions(encoded, schema);
    assertEquals(2, decoded.size());

    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(0).getLowerBoundType());
    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(0).getUpperBoundType());
    {
      final PartialRow lowerDecoded = decoded.get(0).getLowerBound();
      final PartialRow upperDecoded = decoded.get(0).getUpperBound();

      assertTrue(lowerDecoded.isSet("c0"));
      assertEquals(0, lowerDecoded.getInt("c0"));
      assertFalse(lowerDecoded.isSet("c1"));

      assertTrue(upperDecoded.isSet("c0"));
      assertEquals(100, upperDecoded.getInt("c0"));
      assertFalse(upperDecoded.isSet("c1"));
    }

    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(1).getLowerBoundType());
    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(1).getUpperBoundType());
    {
      final PartialRow lowerDecoded = decoded.get(1).getLowerBound();
      final PartialRow upperDecoded = decoded.get(1).getUpperBound();

      assertTrue(lowerDecoded.isSet("c0"));
      assertEquals(200, lowerDecoded.getInt("c0"));
      assertFalse(lowerDecoded.isSet("c1"));

      assertTrue(upperDecoded.isSet("c0"));
      assertEquals(300, upperDecoded.getInt("c0"));
      assertFalse(upperDecoded.isSet("c1"));
    }
  }

  @Test
  public void testEncodeDecodeMultipleRangePartitionsNullableColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.INT64).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    final Schema schema = new Schema(columns);

    List<CreateTableOptions.RangePartition> rangePartitions = new ArrayList<>();
    {
      final PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 0);
      lower.addString("c1", "a");

      final PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 100);
      upper.addString("c1", "c");
      rangePartitions.add(new CreateTableOptions.RangePartition(
          lower,
          upper,
          RangePartitionBound.INCLUSIVE_BOUND,
          RangePartitionBound.EXCLUSIVE_BOUND));
    }
    {
      final PartialRow lower = schema.newPartialRow();
      lower.addInt("c0", 200);
      lower.addString("c1", "e");

      final PartialRow upper = schema.newPartialRow();
      upper.addInt("c0", 300);
      upper.addString("c1", "f");
      rangePartitions.add(new CreateTableOptions.RangePartition(
          lower,
          upper,
          RangePartitionBound.EXCLUSIVE_BOUND,
          RangePartitionBound.INCLUSIVE_BOUND));
    }

    final Operation.OperationsEncoder enc = new Operation.OperationsEncoder();
    final RowOperationsPB encoded = enc.encodeRangePartitions(
        rangePartitions, ImmutableList.of());

    Operation.OperationsDecoder dec = new Operation.OperationsDecoder();
    List<CreateTableOptions.RangePartition> decoded =
        dec.decodeRangePartitions(encoded, schema);
    assertEquals(2, decoded.size());

    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(0).getLowerBoundType());
    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(0).getUpperBoundType());
    {
      final PartialRow lowerDecoded = decoded.get(0).getLowerBound();
      final PartialRow upperDecoded = decoded.get(0).getUpperBound();

      assertTrue(lowerDecoded.isSet("c0"));
      assertEquals(0, lowerDecoded.getInt("c0"));
      assertTrue(lowerDecoded.isSet("c1"));
      assertEquals("a", lowerDecoded.getString("c1"));
      assertFalse(lowerDecoded.isSet("c2"));
      assertFalse(lowerDecoded.isSet("c3"));

      assertTrue(upperDecoded.isSet("c0"));
      assertEquals(100, upperDecoded.getInt("c0"));
      assertTrue(upperDecoded.isSet("c1"));
      assertEquals("c", upperDecoded.getString("c1"));
      assertFalse(upperDecoded.isSet("c2"));
      assertFalse(upperDecoded.isSet("c3"));
    }


    assertEquals(RangePartitionBound.EXCLUSIVE_BOUND,
        decoded.get(1).getLowerBoundType());
    assertEquals(RangePartitionBound.INCLUSIVE_BOUND,
        decoded.get(1).getUpperBoundType());
    {
      final PartialRow lowerDecoded = decoded.get(1).getLowerBound();
      final PartialRow upperDecoded = decoded.get(1).getUpperBound();

      assertTrue(lowerDecoded.isSet("c0"));
      assertEquals(200, lowerDecoded.getInt("c0"));
      assertTrue(lowerDecoded.isSet("c1"));
      assertEquals("e", lowerDecoded.getString("c1"));
      assertFalse(lowerDecoded.isSet("c2"));
      assertFalse(lowerDecoded.isSet("c3"));

      assertTrue(upperDecoded.isSet("c0"));
      assertEquals(300, upperDecoded.getInt("c0"));
      assertTrue(upperDecoded.isSet("c1"));
      assertEquals("f", upperDecoded.getString("c1"));
      assertFalse(upperDecoded.isSet("c2"));
      assertFalse(upperDecoded.isSet("c3"));
    }
  }
}
