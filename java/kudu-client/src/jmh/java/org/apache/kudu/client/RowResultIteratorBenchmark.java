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

import java.util.ArrayList;
import java.util.List;

import io.netty.util.CharsetUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.Slice;
import org.apache.kudu.util.Slices;

@State(Scope.Thread)
@Fork(1)
public class RowResultIteratorBenchmark {

  final Schema schema;

  // for rowwise result
  Slice rowwiseBs;
  Slice rowwiseIndirectBs;

  // for columnar result
  Slice[] columnarData;
  Slice[] columnarVarlenData;
  Slice[] columnarNonNullBitmaps;

  @Param({"true", "false"})
  boolean reuseResultRow;

  @Param({"1", "10", "10000"})
  int numRows;

  public RowResultIteratorBenchmark() {
    List<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("action", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("time", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("seq", Type.INT64).key(true).build());
    this.schema = new Schema(columns);
  }

  @Setup
  public void prepare() {
    prepareRowwiseSlices();
    prepareColumnarSlices();
  }

  private void prepareRowwiseSlices() {
    byte[] data = new byte[numRows * schema.getRowSize()];
    byte[] vardata = new byte[numRows * 10];

    int offset = 0;
    int vardataOffset = 0;

    for (int i = 0; i < numRows; i++) {
      String action = "action" + i;
      int actionLen = action.getBytes(CharsetUtil.UTF_8).length;

      offset += writeLong(data, offset, vardataOffset);
      offset += writeLong(data, offset, actionLen);
      offset += writeInt(data, offset, i);
      offset += writeLong(data, offset, i * 10000L);

      vardataOffset += writeString(vardata, vardataOffset, action);
    }

    rowwiseBs = Slices.wrappedBuffer(data);
    rowwiseIndirectBs = Slices.wrappedBuffer(vardata);
  }

  private void prepareColumnarSlices() {
    byte[][] data = new byte[3][];
    data[0] = new byte[4 * (numRows + 1)];
    data[1] = new byte[4 * numRows];
    data[2] = new byte[8 * numRows];

    byte[][] varData = new byte[3][];
    varData[0] = new byte[numRows * 10];
    varData[1] = new byte[0];
    varData[2] = new byte[0];

    byte[][] nonNullBitmaps = new byte[3][];
    nonNullBitmaps[0] = new byte[0];
    nonNullBitmaps[1] = new byte[0];
    nonNullBitmaps[2] = new byte[0];

    int dataOffset0 = 0;
    int dataOffset1 = 0;
    int dataOffset2 = 0;

    int varDataOffset0 = 0;

    for (int i = 0; i < numRows; i++) {
      String action = "action" + i;

      dataOffset0 += writeInt(data[0], dataOffset0, varDataOffset0);
      varDataOffset0 += writeString(varData[0], varDataOffset0, action);

      dataOffset1 += writeInt(data[1], dataOffset1, i);
      dataOffset2 += writeLong(data[2], dataOffset2, i * 10000L);
    }
    // write offset for last row.
    writeInt(data[0], dataOffset0, varDataOffset0);

    columnarData = new Slice[3];
    columnarVarlenData = new Slice[3];
    columnarNonNullBitmaps = new Slice[3];
    for (int i = 0; i < 3; i++) {
      columnarData[i] = Slices.wrappedBuffer(data[i]);
      columnarVarlenData[i] = Slices.wrappedBuffer(varData[i]);
      columnarNonNullBitmaps[i] = Slices.wrappedBuffer(nonNullBitmaps[i]);
    }
  }

  @Benchmark
  public void testRowwiseResult(Blackhole blackhole) {
    RowResultIterator iter = new RowwiseRowResultIterator(
            0, "uuid", schema, numRows,
            rowwiseBs, rowwiseIndirectBs, reuseResultRow);

    while (iter.hasNext()) {
      RowResult row = iter.next();
      String action = row.getString(0);
      int time = row.getInt(1);
      long seq = row.getLong(2);

      blackhole.consume(action);
      blackhole.consume(time);
      blackhole.consume(seq);
    }
  }

  @Benchmark
  public void testColumnarResult(Blackhole blackhole) {
    RowResultIterator iter = new ColumnarRowResultIterator(
            0, "uuid", schema, numRows,
            columnarData, columnarVarlenData, columnarNonNullBitmaps, reuseResultRow);

    while (iter.hasNext()) {
      RowResult row = iter.next();
      String action = row.getString(0);
      int time = row.getInt(1);
      long seq = row.getLong(2);

      blackhole.consume(action);
      blackhole.consume(time);
      blackhole.consume(seq);
    }
  }

  private static int writeInt(final byte[] b, final int offset, final int value) {
    b[offset + 0] = (byte) (value >> 0);
    b[offset + 1] = (byte) (value >> 8);
    b[offset + 2] = (byte) (value >> 16);
    b[offset + 3] = (byte) (value >> 24);
    return 4;
  }

  private static int writeLong(final byte[] b, final int offset, final long value) {
    b[offset + 0] = (byte) (value >> 0);
    b[offset + 1] = (byte) (value >> 8);
    b[offset + 2] = (byte) (value >> 16);
    b[offset + 3] = (byte) (value >> 24);
    b[offset + 4] = (byte) (value >> 32);
    b[offset + 5] = (byte) (value >> 40);
    b[offset + 6] = (byte) (value >> 48);
    b[offset + 7] = (byte) (value >> 56);
    return 8;
  }

  private static int writeString(final byte[] b, final int offset, final String value) {
    byte[] data = value.getBytes(CharsetUtil.UTF_8);
    System.arraycopy(data, 0, b, offset, data.length);
    return data.length;
  }
}