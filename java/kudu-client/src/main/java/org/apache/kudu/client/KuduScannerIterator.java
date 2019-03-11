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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.Iterator;

/**
 * An iterator for the RowResults of a KuduScanner.
 * Exhausting this iterator means that all of the rows from a KuduScanner have been read.
 *
 * This iterator also handles sending keep alive requests to ensure the scanner
 * does not time out.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduScannerIterator implements Iterator<RowResult> {

  private final KuduScanner scanner;
  private final long keepAlivePeriodMs;

  private RowResultIterator currentIterator = RowResultIterator.empty();
  private long lastKeepAliveTimeMs = System.currentTimeMillis();

  KuduScannerIterator(KuduScanner scanner, long keepAlivePeriodMs) {
    this.scanner = scanner;
    this.keepAlivePeriodMs = keepAlivePeriodMs;
  }

  /**
   * Calls the keepAlive API on the current scanner if the keepAlivePeriodMs has passed.
   */
  private void KeepKuduScannerAlive() throws KuduException {
    long now = System.currentTimeMillis();
    if (now >= lastKeepAliveTimeMs + keepAlivePeriodMs && !scanner.isClosed()) {
      scanner.keepAlive();
      lastKeepAliveTimeMs = now;
    }
  }

  /**
   * Special implementation of hasNext that calls a callback each time
   * {@link KuduScanner#nextRows} is called.
   *
   * @param nextRowsCallback the NextRowsCallback to call
   * @return {@code true} if the iteration has more elements
   */
  @InterfaceAudience.LimitedPrivate("Spark")
  public boolean hasNext(NextRowsCallback nextRowsCallback) {
    try {
      while (!currentIterator.hasNext() && scanner.hasMoreRows()) {
        currentIterator = scanner.nextRows();
        if (nextRowsCallback != null) {
          nextRowsCallback.call(currentIterator.getNumRows());
        }
      }
      KeepKuduScannerAlive();
      return currentIterator.hasNext();
    } catch (KuduException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean hasNext() {
    return hasNext(null);
  }

  @Override
  public RowResult next() {
    return currentIterator.next();
  }

  @InterfaceAudience.LimitedPrivate("Spark")
  public static abstract class NextRowsCallback {

    /**
     * @param numRows The number of rows returned from the
     *                {@link KuduScanner#nextRows} call.
     */
    public abstract void call(int numRows);
  }
}
