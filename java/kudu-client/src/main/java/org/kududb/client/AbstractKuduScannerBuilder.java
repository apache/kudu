// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.kududb.Schema;

/**
 * Abstract class to extend in order to create builders for scanners.
 */
public abstract class AbstractKuduScannerBuilder
    <S extends AbstractKuduScannerBuilder<? super S, T>, T> {
  protected final AsyncKuduClient nestedClient;
  protected final KuduTable nestedTable;
  protected final DeadlineTracker nestedDeadlineTracker;
  protected final ColumnRangePredicates nestedColumnRangePredicates;

  protected AsyncKuduScanner.ReadMode nestedReadMode = AsyncKuduScanner.ReadMode.READ_LATEST;
  protected int nestedMaxNumBytes = 1024*1024;
  protected long nestedLimit = Long.MAX_VALUE;
  protected boolean nestedPrefetching = false;
  protected boolean nestedCacheBlocks = true;
  protected long nestedHtTimestamp = AsyncKuduClient.NO_TIMESTAMP;
  protected byte[] nestedStartKey = null;
  protected byte[] nestedEndKey = null;
  protected List<String> nestedProjectedColumnNames = null;

  AbstractKuduScannerBuilder(AsyncKuduClient client, KuduTable table) {
    this.nestedClient = client;
    this.nestedTable = table;
    this.nestedDeadlineTracker = new DeadlineTracker();
    this.nestedColumnRangePredicates = new ColumnRangePredicates(table.getSchema());
  }

  /**
   * Sets the read mode, the default is to read the latest values.
   * @param readMode a read mode for the scanner
   * @return this instance
   */
  public S readMode(AsyncKuduScanner.ReadMode readMode) {
    this.nestedReadMode = readMode;
    return (S) this;
  }

  /**
   * Adds a predicate for a column.
   * Very important constraint: row key predicates must be added in order.
   * @param predicate predicate for a column to add
   * @return this instance
   */
  public S addColumnRangePredicate(ColumnRangePredicate predicate) {
    nestedColumnRangePredicates.addColumnRangePredicate(predicate);
    return (S) this;
  }

  /**
   * Set which columns will be read by the Scanner.
   * @param columnNames the names of columns to read, or 'null' to read all columns
   * (the default)
   */
  public S setProjectedColumnNames(List<String> columnNames) {
    if (columnNames != null) {
      nestedProjectedColumnNames = ImmutableList.copyOf(columnNames);
    } else {
      nestedProjectedColumnNames = null;
    }
    return (S) this;
  }

  /**
   * Sets the maximum number of bytes returned at once by the scanner. The default is 1MB.
   * <p>
   * Kudu may actually return more than this many bytes because it will not
   * truncate a rowResult in the middle.
   * @param maxNumBytes a strictly positive number of bytes
   * @return this instance
   */
  public S maxNumBytes(int maxNumBytes) {
    this.nestedMaxNumBytes = maxNumBytes;
    return (S) this;
  }

  /**
   * Sets a limit on the number of rows that will be returned by the scanner. There's no limit
   * by default.
   * @param limit a positive long
   * @return this instance
   */
  public S limit(long limit) {
    this.nestedLimit = limit;
    return (S) this;
  }

  /**
   * Enables prefetching of rows for the scanner, disabled by default.
   * @param prefetching a boolean that indicates if the scanner should prefetch rows
   * @return this instance
   */
  public S prefetching(boolean prefetching) {
    this.nestedPrefetching = prefetching;
    return (S) this;
  }

  /**
   * Sets the block caching policy for the scanner. If true, scanned data blocks will be cached
   * in memory and made available for future scans. Enabled by default.
   * @param cacheBlocks a boolean that indicates if data blocks should be cached or not
   * @return this instance
   */
  public S cacheBlocks(boolean cacheBlocks) {
    this.nestedCacheBlocks = cacheBlocks;
    return (S) this;
  }

  /**
   * Sets a previously encoded HT timestamp as a snapshot timestamp, for tests. None is used by
   * default.
   * @param htTimestamp a long representing a HybridClock-encoded timestamp
   * @return this instance
   * @throws IllegalArgumentException if the timestamp is less than 0
   */
  @VisibleForTesting
  public S snapshotTimestamp(long htTimestamp) {
    this.nestedHtTimestamp = htTimestamp;
    return (S) this;
  }

  /**
   * Sets how long the scanner can run for before it expires. The deadline check is triggered
   * only when more rows must be fetched from a server. There's no timeout by default.
   * @param deadlineMillis a long representing time in milliseconds that the scanner can run for
   * @return this instance
   */
  public S deadlineMillis(long deadlineMillis) {
    this.nestedDeadlineTracker.setDeadline(deadlineMillis);
    return (S) this;
  }

  /**
   * Sets the start key in its encoded format, bypassing the need to add column predicates. By
   * default, none is set.
   * If you don't what that means then don't use this.
   * @param encodedStartKey bytes containing an encoded start key
   * @return this instance
   */
  public S encodedStartKey(byte[] encodedStartKey) {
    this.nestedStartKey = encodedStartKey;
    return (S) this;
  }

  /**
   * Sets the end key in its encoded format, bypassing the need to add column predicates. By
   * default, none is set.
   * If you don't what that means then don't use this.
   * @param encodedEndKey bytes containing an encoded end key
   * @return this instance
   */
  public S encodedEndKey(byte[] encodedEndKey) {
    this.nestedEndKey = encodedEndKey;
    return (S) this;
  }

  public abstract T build();
}

