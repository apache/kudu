// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.stumbleupon.async.Deferred;
import org.kududb.Schema;

/**
 * Synchronous version of {@link AsyncKuduScanner}. Offers the same API but with blocking methods.
 */
public class KuduScanner {

  private final AsyncKuduScanner asyncScanner;
  private long currentTimeoutMs = 5000;

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
   * Scans a number of rows.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return a list of rows.
   */
  public AsyncKuduScanner.RowResultIterator nextRows() throws Exception {
    Deferred<AsyncKuduScanner.RowResultIterator> d = asyncScanner.nextRows();
    return d.join(currentTimeoutMs);
  }

  /**
   * Closes this scanner (don't forget to call this when you're done with it!).
   * <p>
   * Closing a scanner already closed has no effect.
   * @return a deferred object that indicates the completion of the request
   */
  public AsyncKuduScanner.RowResultIterator close() throws Exception {
    Deferred<AsyncKuduScanner.RowResultIterator> d = asyncScanner.close();
    return d.join(currentTimeoutMs);
  }

  /**
   * Sets the timeout used to wait when calling {@link KuduScanner#nextRows()} and
   * {@link KuduScanner#close()}.
   * The default timeout is 5 seconds.
   * A value of 0 disables the timeout functionality.
   * @param timeoutMs timeout in milliseconds
   */
  public void setTimeoutMillis(long timeoutMs) {
    this.currentTimeoutMs = timeoutMs;
  }

  /**
   * A Builder class to build {@link KuduScanner}.
   * Use {@link KuduClient#newScannerBuilder} in order to get a builder instance.
   */
  public static class KuduScannerBuilder
      extends AbstractKuduScannerBuilder<KuduScannerBuilder, KuduScanner> {

    KuduScannerBuilder(AsyncKuduClient client, KuduTable table, Schema schema) {
      super(client, table, schema);
    }

    /**
     * Builds a {@link KuduScanner} using the passed configurations.
     * @return a new {@link KuduScanner}
     */
    public KuduScanner build() {
      return new KuduScanner(new AsyncKuduScanner(
          nestedClient, nestedTable, nestedSchema, nestedReadMode,
          nestedDeadlineTracker, nestedColumnRangePredicates, nestedLimit, nestedCacheBlocks,
          nestedPrefetching, nestedStartKey, nestedEndKey, nestedHtTimestamp, nestedMaxNumBytes));
    }
  }
}
