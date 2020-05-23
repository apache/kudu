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

import java.util.NoSuchElementException;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Schema;
import org.apache.kudu.WireProtocol;
import org.apache.kudu.util.Slice;

/**
 * Class that contains the rows in columnar layout sent by a tablet server,
 * exhausting this iterator only means that all the rows from the last server response were read.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@SuppressWarnings("IterableAndIterator")
class ColumnarRowResultIterator extends RowResultIterator {

  private static final ColumnarRowResultIterator EMPTY =
      new ColumnarRowResultIterator(0, null, null, 0,
              null, null, null, false);

  private final Slice[] data;
  private final Slice[] varlenData;
  private final Slice[] nonNullBitmaps;
  private final RowResult sharedRowResult;

  /**
   * Package private constructor, only meant to be instantiated from AsyncKuduScanner.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param tsUUID UUID of the tablet server that handled our request
   * @param schema schema used to parse the rows
   * @param numRows how many rows are contained in the bs slice
   * @param data The raw columnar data corresponding to the primitive-typed columns
   * @param varlenData The variable-length data for the variable-length-typed columns
   * @param nonNullBitmaps The bitmaps corresponding to the non-null status of the cells
   * @param reuseRowResult reuse same row result for next row
   */
  ColumnarRowResultIterator(long elapsedMillis,
                                    String tsUUID,
                                    Schema schema,
                                    int numRows,
                                    Slice[] data,
                                    Slice[] varlenData,
                                    Slice[] nonNullBitmaps,
                                    boolean reuseRowResult) {
    super(elapsedMillis, tsUUID, schema, numRows, reuseRowResult);
    this.data = data;
    this.varlenData = varlenData;
    this.nonNullBitmaps = nonNullBitmaps;
    this.sharedRowResult = (reuseRowResult && numRows != 0) ?
            new ColumnarRowResult(this.schema, data, varlenData, nonNullBitmaps, -1) :
            null;
  }

  static ColumnarRowResultIterator makeRowResultIterator(long elapsedMillis,
                                                         String tsUUID,
                                                         Schema schema,
                                                         WireProtocol.ColumnarRowBlockPB data,
                                                         final CallResponse callResponse,
                                                         boolean reuseRowResult)
      throws KuduException {
    if (data == null || data.getNumRows() == 0) {
      return new ColumnarRowResultIterator(elapsedMillis, tsUUID, schema, 0,
              null, null, null, reuseRowResult);
    }

    Slice[] dataSlices = new Slice[data.getColumnsCount()];
    Slice[] varlenDataSlices = new Slice[data.getColumnsCount()];
    Slice[] nonNullBitmapSlices = new Slice[data.getColumnsCount()];

    for (int i = 0; i < data.getColumnsCount(); i++) {
      WireProtocol.ColumnarRowBlockPB.Column column = data.getColumns(i);
      dataSlices[i] = callResponse.getSidecar(column.getDataSidecar());
      varlenDataSlices[i] = callResponse.getSidecar(column.getVarlenDataSidecar());
      nonNullBitmapSlices[i] = callResponse.getSidecar(column.getNonNullBitmapSidecar());
    }
    int numRows = Math.toIntExact(data.getNumRows());

    return new ColumnarRowResultIterator(elapsedMillis, tsUUID, schema, numRows,
            dataSlices, varlenDataSlices, nonNullBitmapSlices, reuseRowResult);
  }

  /**
   * @return an empty row result iterator
   */
  public static ColumnarRowResultIterator empty() {
    return EMPTY;
  }

  @Override
  public RowResult next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    // If sharedRowResult is not null, we should reuse it for every next call.
    if (sharedRowResult != null) {
      this.sharedRowResult.advancePointerTo(this.currentRow++);
      return sharedRowResult;
    } else {
      return new ColumnarRowResult(this.schema, this.data, this.varlenData, this.nonNullBitmaps,
              this.currentRow++);
    }
  }

  @Override
  public String toString() {
    return "RowResultColumnarIterator for " + this.numRows + " rows";
  }

}
