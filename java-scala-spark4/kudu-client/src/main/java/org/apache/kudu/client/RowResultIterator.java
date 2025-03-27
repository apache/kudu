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

/**
 * Class that contains the rows sent by a tablet server, exhausting this iterator only means
 * that all the rows from the last server response were read.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("IterableAndIterator")
public abstract class RowResultIterator extends KuduRpcResponse implements Iterator<RowResult>,
    Iterable<RowResult> {

  protected final Schema schema;
  protected final int numRows;
  protected int currentRow = 0;

  /**
   * Package private constructor, only meant to be instantiated from AsyncKuduScanner.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param tsUUID UUID of the tablet server that handled our request
   * @param schema schema used to parse the rows
   * @param numRows how many rows are contained in the bs slice
   */
  RowResultIterator(long elapsedMillis,
                    String tsUUID,
                    Schema schema,
                    int numRows) {
    super(elapsedMillis, tsUUID);
    this.schema = schema;
    this.numRows = numRows;
  }

  public int getNumRows() {
    return this.numRows;
  }

  public static RowResultIterator empty() {
    return RowwiseRowResultIterator.empty();
  }

  @Override
  public boolean hasNext() {
    return this.currentRow < numRows;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<RowResult> iterator() {
    return this;
  }
}
