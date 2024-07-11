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

import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduScanner.ReadMode;

/**
 * Synchronous version of {@link AsyncKuduScanner}. Offers the same API but with blocking methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduScanner implements Iterable<RowResult> {

  private final AsyncKuduScanner asyncScanner;

  KuduScanner(AsyncKuduScanner asyncScanner) {
    this.asyncScanner = asyncScanner;
  }

  /**
   * Tells if the last rpc returned that there might be more rows to scan.
   * @return true if there might be more data to scan, else false
   */
  public boolean hasMoreRows() {
    return asyncScanner.hasMoreRows();
  }

  /**
   * If set to true, the {@link RowResult} object returned by the {@link RowResultIterator}
   * will be reused with each call to {@link RowResultIterator#next()}.
   * This can be a useful optimization to reduce the number of objects created.
   *
   * Note: DO NOT use this if the RowResult is stored between calls to next().
   * Enabling this optimization means that a call to next() mutates the previously returned
   * RowResult. Accessing the previously returned RowResult after a call to next(), by storing all
   * RowResults in a collection and accessing them later for example, will lead to all of the
   * stored RowResults being mutated as per the data in the last RowResult returned.
   */
  public void setReuseRowResult(boolean reuseRowResult) {
    asyncScanner.setReuseRowResult(reuseRowResult);
  }

  /**
   * Optionally set expected row data format.
   *
   * @param rowDataFormat Row data format to be expected.
   */
  public void setRowDataFormat(AsyncKuduScanner.RowDataFormat rowDataFormat) {
    asyncScanner.setRowDataFormat(rowDataFormat);
  }

  /**
   * Scans a number of rows.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return a list of rows.
   * @throws KuduException if anything went wrong.
   */
  public RowResultIterator nextRows() throws KuduException {
    return KuduClient.joinAndHandleException(asyncScanner.nextRows());
  }

  /**
   * Keep the current remote scanner alive.
   * <p>
   * Keep the current remote scanner alive on the Tablet server for an
   * additional time-to-live. This is useful if the interval in between
   * nextRows() calls is big enough that the remote scanner might be garbage
   * collected. The scanner time-to-live can be configured on the tablet
   * server via the --scanner_ttl_ms configuration flag and has a default
   * of 60 seconds.
   * <p>
   * This does not invalidate any previously fetched results.
   * <p>
   * Note that an exception thrown by this method should not be taken as indication
   * that the scan has failed. Subsequent calls to nextRows() might still be successful,
   * particularly if the scanner is configured to be fault tolerant.
   * @throws KuduException if anything went wrong.
   */
  public final void keepAlive() throws KuduException {
    KuduClient.joinAndHandleException(asyncScanner.keepAlive());
  }

  /**
   * Keep the current remote scanner alive by sending keep-alive requests periodically.
   * <p>
   * startKeepAlivePeriodically() uses a timer to call keepAlive() periodically,
   * which is defined by parameter keepAliveIntervalMS. It sends keep-alive requests to
   * the server periodically using a separate thread. This is useful if the client
   * takes long time to handle the fetched data before having the chance to call
   * keepAlive(). This can be called after the scanner is opened and the timer can be
   * stopped by calling stopKeepAlivePeriodically().
   * <p>
   * @throws KuduException if anything went wrong.
   * <p>
   * * @return true if starting keep-alive timer successfully.
   */
  public final boolean startKeepAlivePeriodically(int keepAliveIntervalMS) throws KuduException {
    return asyncScanner.startKeepAlivePeriodically(keepAliveIntervalMS);
  }

  /**
   * Stop keeping the current remote scanner alive periodically.
   * <p>
   * This function stops to send keep-alive requests to the server periodically.
   * After function startKeepAlivePeriodically is called, this function can be used to
   * stop the keep-alive timer at any time. The timer will be stopped automatically
   * after finishing scanning. But it can also be stopped manually by calling this
   * function.
   * <p>
   * @return true if stopping keep-alive timer successfully.
   */
  public final boolean stopKeepAlivePeriodically() {
    return asyncScanner.stopKeepAlivePeriodically();
  }

  /**
   * @return true if the scanner has been closed.
   */
  public boolean isClosed() {
    return asyncScanner.isClosed();
  }

  /**
   * Closes this scanner (don't forget to call this when you're done with it!).
   * <p>
   * Closing a scanner already closed has no effect.
   * @return a deferred object that indicates the completion of the request
   * @throws KuduException if anything went wrong.
   */
  public RowResultIterator close() throws KuduException {
    return KuduClient.joinAndHandleException(asyncScanner.close());
  }

  /**
   * Returns the maximum number of rows that this scanner was configured to return.
   * @return a long representing the maximum number of rows that can be returned
   */
  public long getLimit() {
    return asyncScanner.getLimit();
  }

  /**
   * Returns if this scanner was configured to cache data blocks or not.
   * @return true if this scanner will cache blocks, else else.
   */
  public boolean getCacheBlocks() {
    return asyncScanner.getCacheBlocks();
  }

  /**
   * Returns the maximum number of bytes returned by the scanner, on each batch.
   * @return a long representing the maximum number of bytes that a scanner can receive at once
   * from a tablet server
   */
  public long getBatchSizeBytes() {
    return asyncScanner.getBatchSizeBytes();
  }

  /**
   * Returns the ReadMode for this scanner.
   * @return the configured read mode for this scanner
   */
  public ReadMode getReadMode() {
    return asyncScanner.getReadMode();
  }

  /**
   * Returns the projection schema of this scanner. If specific columns were
   * not specified during scanner creation, the table schema is returned.
   * @return the projection schema for this scanner
   */
  public Schema getProjectionSchema() {
    return asyncScanner.getProjectionSchema();
  }

  /**
   * Returns the resource metrics of this scanner.
   * @return the resource metrics for this scanner
   */
  public ResourceMetrics getResourceMetrics() {
    return asyncScanner.getResourceMetrics();
  }

  /**
   * Returns the RemoteTablet currently being scanned, if any.
   */
  @InterfaceAudience.LimitedPrivate("Test")
  public RemoteTablet currentTablet() {
    return asyncScanner.currentTablet();
  }

  /**
   * Gets the replica selection mechanism being used.
   *
   * @return the replica selection mechanism
   */
  @InterfaceAudience.LimitedPrivate("Test")
  ReplicaSelection getReplicaSelection() {
    return asyncScanner.getReplicaSelection();
  }

  /**
   * Returns the current value of the scanner's scan request timeout.
   * @return the timeout value, in milliseconds
   */
  public long getScanRequestTimeout() {
    return asyncScanner.getScanRequestTimeout();
  }

  @Override
  public KuduScannerIterator iterator() {
    return new KuduScannerIterator(this, asyncScanner.getKeepAlivePeriodMs());
  }

  /**
   * A Builder class to build {@link KuduScanner}.
   * Use {@link KuduClient#newScannerBuilder} in order to get a builder instance.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class KuduScannerBuilder
      extends AbstractKuduScannerBuilder<KuduScannerBuilder, KuduScanner> {

    KuduScannerBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
    }

    /**
     * Builds a {@link KuduScanner} using the passed configurations.
     * @return a new {@link KuduScanner}
     */
    @Override
    public KuduScanner build() {
      return new KuduScanner(new AsyncKuduScanner(
          client, table, projectedColumnNames, projectedColumnIndexes, readMode, isFaultTolerant,
          scanRequestTimeout, predicates, limit, cacheBlocks, prefetching, lowerBoundPrimaryKey,
          upperBoundPrimaryKey, startTimestamp, htTimestamp, batchSizeBytes,
          PartitionPruner.create(this), replicaSelection, keepAlivePeriodMs, queryId));
    }
  }
}
