// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.stumbleupon.async.Deferred;

/**
 * Synchronous version of {@link AsyncKuduScanner}. Offers the same API but with blocking methods.
 */
public class KuduScanner {

  private final AsyncKuduScanner asyncScanner;
  private final long timeoutMs;

  KuduScanner(AsyncKuduScanner asyncScanner, long timeoutMs) {
    this.asyncScanner = asyncScanner;
    this.timeoutMs = timeoutMs;
  }

  /**
   * Tells if the last rpc returned that there might be more rows to scan.
   * @return true if there might be more data to scan, else false
   */
  public boolean hasMoreRows() {
    return asyncScanner.hasMoreRows();
  }

  /**
   * Scans a number of rows.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return a list of rows.
   */
  public RowResultIterator nextRows() throws Exception {
    Deferred<RowResultIterator> d = asyncScanner.nextRows();
    return d.join(timeoutMs);
  }

  /**
   * Closes this scanner (don't forget to call this when you're done with it!).
   * <p>
   * Closing a scanner already closed has no effect.
   * @return a deferred object that indicates the completion of the request
   */
  public RowResultIterator close() throws Exception {
    Deferred<RowResultIterator> d = asyncScanner.close();
    return d.join(timeoutMs);
  }

  /**
   * A Builder class to build {@link KuduScanner}.
   * Use {@link KuduClient#newScannerBuilder} in order to get a builder instance.
   */
  public static class KuduScannerBuilder
      extends AbstractKuduScannerBuilder<KuduScannerBuilder, KuduScanner> {

    private long nestedTimeoutMs;

    KuduScannerBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
      nestedTimeoutMs = client.getDefaultOperationTimeoutMs();
    }

    /**
     * Sets the timeout used to wait when calling {@link KuduScanner#nextRows()} and
     * {@link KuduScanner#close()}.
     * If not specified, this timeout will be used:
     * {@link KuduClient#getDefaultOperationTimeoutMs()}.
     * A value of 0 disables the timeout functionality.
     * @param timeoutMs timeout in milliseconds
     */
    public KuduScannerBuilder timeoutMs(long timeoutMs) {
      this.nestedTimeoutMs = timeoutMs;
      return this;
    }

    /**
     * Builds a {@link KuduScanner} using the passed configurations.
     * @return a new {@link KuduScanner}
     */
    public KuduScanner build() {
      return new KuduScanner(new AsyncKuduScanner(
          nestedClient, nestedTable, nestedProjectedColumnNames, nestedReadMode,
          nestedDeadlineTracker, nestedColumnRangePredicates, nestedLimit, nestedCacheBlocks,
          nestedPrefetching, nestedLowerBound, nestedUpperBound, nestedHtTimestamp, nestedMaxNumBytes),
          nestedTimeoutMs);
    }
  }
}
