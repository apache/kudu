// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Common;
import org.kududb.Schema;
import org.kududb.Type;

import static org.junit.Assert.*;

public class TestPartialRow {

  @Test
  public void testToPB() throws Exception {

    List<ColumnSchema> columns =
        ImmutableList.of(
            new ColumnSchema.ColumnSchemaBuilder("int8", Type.INT8).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build(),
            new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build(),
            new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build(),
            new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build(),
            new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build(),
            new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build(),
            new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("null", Type.STRING).nullable(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("missing", Type.STRING).build());

    Schema schema = new Schema(columns);
    PartialRow row = schema.newPartialRow();

    row.addByte("int8", (byte) 1);
    row.addShort("int16", (short) 2);
    row.addInt("int32", 3);
    row.addLong("int64", 4l);
    row.addBoolean("bool", true);
    row.addFloat("float", 5.6f);
    row.addDouble("double", 7.8);
    row.addString("string", "string-value");
    row.addBinary("binary", "binary-value".getBytes());
    row.setNull("null");

    final Common.PartialRowPB pb = row.toPB();

    assertEquals((byte) 1, pb.getColumns(0).getInt8Val());
    assertEquals(0, pb.getColumns(0).getIndex());

    assertEquals((short) 2, pb.getColumns(1).getInt16Val());
    assertEquals(1, pb.getColumns(1).getIndex());

    assertEquals(3, pb.getColumns(2).getInt32Val());
    assertEquals(2, pb.getColumns(2).getIndex());

    assertEquals(4, pb.getColumns(3).getInt64Val());
    assertEquals(3, pb.getColumns(3).getIndex());

    assertEquals(true, pb.getColumns(4).getBoolVal());
    assertEquals(4, pb.getColumns(4).getIndex());

    assertEquals(5.6f, pb.getColumns(5).getFloatVal(), .001f);
    assertEquals(5, pb.getColumns(5).getIndex());

    assertEquals(7.8, pb.getColumns(6).getDoubleVal(), .001);
    assertEquals(6, pb.getColumns(6).getIndex());

    assertEquals("string-value", pb.getColumns(7).getStringVal());
    assertEquals(7, pb.getColumns(7).getIndex());

    assertArrayEquals("binary-value".getBytes(), pb.getColumns(8).getBinaryVal().toByteArray());
    assertEquals(8, pb.getColumns(8).getIndex());

    assertEquals(Common.PartialRowPB.ColumnEntryPB.ValueCase.VALUE_NOT_SET,
        pb.getColumns(9).getValueCase());
    assertEquals(9, pb.getColumns(9).getIndex());

    assertEquals(10, pb.getColumnsCount());
  }
}
