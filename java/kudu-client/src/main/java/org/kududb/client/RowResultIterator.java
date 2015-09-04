// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.Iterator;
import org.kududb.Schema;
import org.kududb.WireProtocol;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.util.Slice;

/**
 * Class that contains the rows sent by a tablet server, exhausting this iterator only means
 * that all the rows from the last server response were read.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RowResultIterator extends KuduRpcResponse implements Iterator<RowResult>,
    Iterable<RowResult> {

  private final Schema schema;
  private final Slice bs;
  private final Slice indirectBs;
  private final int numRows;
  private final RowResult rowResult;
  private int currentRow = 0;

  /**
   * Package private constructor, only meant to be instantiated from AsyncKuduScanner.
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   * @param schema Schema used to parse the rows
   * @param data PB containing the data
   * @param callResponse the call response received from the server for this
   * RPC.
   */
  RowResultIterator(long ellapsedMillis, String tsUUID, Schema schema,
                    WireProtocol.RowwiseRowBlockPB data,
                    final CallResponse callResponse) {
    super(ellapsedMillis, tsUUID);
    this.schema = schema;
    if (data == null || data.getNumRows() == 0) {
      this.bs = this.indirectBs = null;
      this.rowResult = null;
      this.numRows = 0;
      return;
    }
    this.bs = callResponse.getSidecar(data.getRowsSidecar());
    this.indirectBs = callResponse.getSidecar(data.getIndirectDataSidecar());
    this.numRows = data.getNumRows();

    // Integrity check
    int rowSize = schema.getRowSize();
    int expectedSize = numRows * rowSize;
    if (expectedSize != bs.length()) {
      throw new NonRecoverableException("RowResult block has " + bs.length() + " bytes of data " +
          "but expected " + expectedSize + " for " + numRows + " rows");
    }
    this.rowResult = new RowResult(this.schema, this.bs, this.indirectBs);
  }

  @Override
  public boolean hasNext() {
    return this.currentRow < numRows;
  }

  @Override
  public RowResult next() {
    // The rowResult keeps track of where it is internally
    this.rowResult.advancePointer();
    this.currentRow++;
    return rowResult;
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
