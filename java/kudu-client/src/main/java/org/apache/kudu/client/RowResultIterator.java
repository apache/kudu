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

import java.util.Iterator;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.Schema;
import org.apache.kudu.WireProtocol;
import org.apache.kudu.util.Slice;

/**
 * Class that contains the rows sent by a tablet server, exhausting this iterator only means
 * that all the rows from the last server response were read.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("IterableAndIterator")
public class RowResultIterator extends KuduRpcResponse implements Iterator<RowResult>,
    Iterable<RowResult> {

  private static final RowResultIterator EMPTY =
      new RowResultIterator(0, null, null, 0, null, null, false);

  private final Schema schema;
  private final Slice bs;
  private final Slice indirectBs;
  private final int numRows;
  private int currentRow = 0;
  private final RowResult sharedRowResult;

  /**
   * Package private constructor, only meant to be instantiated from AsyncKuduScanner.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param tsUUID UUID of the tablet server that handled our request
   * @param schema schema used to parse the rows
   * @param numRows how many rows are contained in the bs slice
   * @param bs normal row data
   * @param indirectBs indirect row data
   */
  private RowResultIterator(long elapsedMillis,
                            String tsUUID,
                            Schema schema,
                            int numRows,
                            Slice bs,
                            Slice indirectBs,
                            boolean reuseRowResult) {
    super(elapsedMillis, tsUUID);
    this.schema = schema;
    this.numRows = numRows;
    this.bs = bs;
    this.indirectBs = indirectBs;
    this.sharedRowResult = (reuseRowResult && numRows != 0) ?
        new RowResult(this.schema, this.bs, this.indirectBs, -1) : null;
  }

  static RowResultIterator makeRowResultIterator(long elapsedMillis,
                                                 String tsUUID,
                                                 Schema schema,
                                                 WireProtocol.RowwiseRowBlockPB data,
                                                 final CallResponse callResponse,
                                                 boolean reuseRowResult)
      throws KuduException {
    if (data == null || data.getNumRows() == 0) {
      return new RowResultIterator(elapsedMillis, tsUUID, schema, 0, null, null, reuseRowResult);
    }

    Slice bs = callResponse.getSidecar(data.getRowsSidecar());
    Slice indirectBs = callResponse.getSidecar(data.getIndirectDataSidecar());
    int numRows = data.getNumRows();

    // Integrity check
    int rowSize = schema.getRowSize();
    int expectedSize = numRows * rowSize;
    if (expectedSize != bs.length()) {
      Status statusIllegalState = Status.IllegalState("RowResult block has " + bs.length() +
          " bytes of data but expected " + expectedSize + " for " + numRows + " rows");
      throw new NonRecoverableException(statusIllegalState);
    }
    return new RowResultIterator(elapsedMillis, tsUUID, schema, numRows, bs, indirectBs,
        reuseRowResult);
  }

  /**
   * @return an empty row result iterator
   */
  public static RowResultIterator empty() {
    return EMPTY;
  }

  @Override
  public boolean hasNext() {
    return this.currentRow < numRows;
  }

  @Override
  public RowResult next() {
    // If sharedRowResult is not null, we should reuse it for every next call.
    if (sharedRowResult != null) {
      this.sharedRowResult.advancePointerTo(this.currentRow++);
      return sharedRowResult;
    } else {
      return new RowResult(this.schema, this.bs, this.indirectBs, this.currentRow++);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the number of rows in this iterator. If all you want is to count
   * rows, call this and skip the rest.
   * @return number of rows in this iterator
   */
  public int getNumRows() {
    return this.numRows;
  }

  @Override
  public String toString() {
    return "RowResultIterator for " + this.numRows + " rows";
  }

  @Override
  public Iterator<RowResult> iterator() {
    return this;
  }
}
