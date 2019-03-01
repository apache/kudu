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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.HybridTimeUtil;

/**
 * Abstract class to extend in order to create builders for scanners.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractKuduScannerBuilder
    <S extends AbstractKuduScannerBuilder<? super S, T>, T> {
  final AsyncKuduClient client;
  final KuduTable table;

  /** Map of column name to predicate */
  final Map<String, KuduPredicate> predicates = new HashMap<>();

  AsyncKuduScanner.ReadMode readMode = AsyncKuduScanner.ReadMode.READ_LATEST;
  boolean isFaultTolerant = false;
  int batchSizeBytes = 1024 * 1024;
  long limit = Long.MAX_VALUE;
  boolean prefetching = false;
  boolean cacheBlocks = true;
  long startTimestamp = AsyncKuduClient.NO_TIMESTAMP;
  long htTimestamp = AsyncKuduClient.NO_TIMESTAMP;
  byte[] lowerBoundPrimaryKey = AsyncKuduClient.EMPTY_ARRAY;
  byte[] upperBoundPrimaryKey = AsyncKuduClient.EMPTY_ARRAY;
  byte[] lowerBoundPartitionKey = AsyncKuduClient.EMPTY_ARRAY;
  byte[] upperBoundPartitionKey = AsyncKuduClient.EMPTY_ARRAY;
  List<String> projectedColumnNames = null;
  List<Integer> projectedColumnIndexes = null;
  long scanRequestTimeout;
  ReplicaSelection replicaSelection = ReplicaSelection.LEADER_ONLY;
  long keepAlivePeriodMs = AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS;

  AbstractKuduScannerBuilder(AsyncKuduClient client, KuduTable table) {
    this.client = client;
    this.table = table;
    this.scanRequestTimeout = client.getDefaultOperationTimeoutMs();
  }

  /**
   * Sets the read mode, the default is to read the latest values.
   * @param readMode a read mode for the scanner
   * @return this instance
   */
  public S readMode(AsyncKuduScanner.ReadMode readMode) {
    this.readMode = readMode;
    return (S) this;
  }

  /**
   * Make scans resumable at another tablet server if current server fails if
   * isFaultTolerant is true.
   * <p>
   * Scans are by default non fault-tolerant, and scans will fail
   * if scanning an individual tablet fails (for example, if a tablet server
   * crashes in the middle of a tablet scan). If isFaultTolerant is set to true,
   * scans will be resumed at another tablet server in the case of failure.
   *
   * Fault-tolerant scans typically have lower throughput than non
   * fault-tolerant scans. Fault tolerant scans use READ_AT_SNAPSHOT read mode.
   * If no snapshot timestamp is provided, the server will pick one.
   *
   * @param isFaultTolerant a boolean that indicates if scan is fault-tolerant
   * @return this instance
   */
  public S setFaultTolerant(boolean isFaultTolerant) {
    this.isFaultTolerant = isFaultTolerant;
    if (isFaultTolerant) {
      readMode = AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
    }
    return (S) this;
  }

  /**
   * Adds a predicate for a column.
   * @param predicate predicate for a column to add
   * @return this instance
   * @deprecated use {@link #addPredicate(KuduPredicate)}
   */
  @Deprecated
  public S addColumnRangePredicate(ColumnRangePredicate predicate) {
    return addPredicate(predicate.toKuduPredicate());
  }

  /**
   * Adds a list of predicates in their raw format,
   * as given by {@link ColumnRangePredicate#toByteArray(List)}.
   * @param predicateBytes predicates to add
   * @return this instance
   * @throws IllegalArgumentException thrown when the passed bytes aren't valid
   * @deprecated use {@link #addPredicate}
   */
  @Deprecated
  public S addColumnRangePredicatesRaw(byte[] predicateBytes) {
    for (Tserver.ColumnRangePredicatePB pb : ColumnRangePredicate.fromByteArray(predicateBytes)) {
      addPredicate(ColumnRangePredicate.fromPb(pb).toKuduPredicate());
    }
    return (S) this;
  }

  /**
   * Adds a predicate to the scan.
   * @param predicate predicate to add
   * @return this instance
   */
  public S addPredicate(KuduPredicate predicate) {
    String columnName = predicate.getColumn().getName();
    KuduPredicate existing = predicates.get(columnName);
    if (existing != null) {
      predicate = existing.merge(predicate);
    }

    // KUDU-1652: Do not send an IS NOT NULL predicate to the server for a non-nullable column.
    if (!predicate.getColumn().isNullable() &&
        predicate.getType() == KuduPredicate.PredicateType.IS_NOT_NULL) {
      return (S) this;
    }

    predicates.put(columnName, predicate);
    return (S) this;
  }

  /**
   * Set which columns will be read by the Scanner.
   * Calling this method after {@link #setProjectedColumnIndexes(List)} will reset the projected
   * columns to those specified in {@code columnNames}.
   * @param columnNames the names of columns to read, or 'null' to read all columns
   * (the default)
   */
  public S setProjectedColumnNames(List<String> columnNames) {
    projectedColumnIndexes = null;
    if (columnNames != null) {
      projectedColumnNames = ImmutableList.copyOf(columnNames);
    } else {
      projectedColumnNames = null;
    }
    return (S) this;
  }

  /**
   * Set which columns will be read by the Scanner.
   * Calling this method after {@link #setProjectedColumnNames(List)} will reset the projected
   * columns to those specified in {@code columnIndexes}.
   * @param columnIndexes the indexes of columns to read, or 'null' to read all columns
   * (the default)
   */
  public S setProjectedColumnIndexes(List<Integer> columnIndexes) {
    projectedColumnNames = null;
    if (columnIndexes != null) {
      projectedColumnIndexes = ImmutableList.copyOf(columnIndexes);
    } else {
      projectedColumnIndexes = null;
    }
    return (S) this;
  }

  /**
   * Sets the maximum number of bytes returned by the scanner, on each batch. The default is 1MB.
   * <p>
   * Kudu may actually return more than this many bytes because it will not
   * truncate a rowResult in the middle.
   * @param batchSizeBytes a strictly positive number of bytes
   * @return this instance
   */
  public S batchSizeBytes(int batchSizeBytes) {
    this.batchSizeBytes = batchSizeBytes;
    return (S) this;
  }

  /**
   * Sets a limit on the number of rows that will be returned by the scanner. There's no limit
   * by default.
   *
   * @param limit a positive long
   * @return this instance
   */
  public S limit(long limit) {
    this.limit = limit;
    return (S) this;
  }

  /**
   * Enables prefetching of rows for the scanner, i.e. whether to send a request for more data
   * to the server immediately after we receive a response (instead of waiting for the user
   * to call {@code  nextRows()}). Disabled by default.
   * NOTE: This is risky until KUDU-1260 is resolved.
   * @param prefetching a boolean that indicates if the scanner should prefetch rows
   * @return this instance
   */
  public S prefetching(boolean prefetching) {
    this.prefetching = prefetching;
    return (S) this;
  }

  /**
   * Sets the block caching policy for the scanner. If true, scanned data blocks will be cached
   * in memory and made available for future scans. Enabled by default.
   * @param cacheBlocks a boolean that indicates if data blocks should be cached or not
   * @return this instance
   */
  public S cacheBlocks(boolean cacheBlocks) {
    this.cacheBlocks = cacheBlocks;
    return (S) this;
  }

  /**
   * Sets a previously encoded HT timestamp as a snapshot timestamp, for tests. None is used by
   * default.
   * Requires that the ReadMode is READ_AT_SNAPSHOT.
   * @param htTimestamp a long representing a HybridTime-encoded timestamp
   * @return this instance
   * @throws IllegalArgumentException on build(), if the timestamp is less than 0 or if the
   *                                  read mode was not set to READ_AT_SNAPSHOT
   */
  @InterfaceAudience.Private
  public S snapshotTimestampRaw(long htTimestamp) {
    this.htTimestamp = htTimestamp;
    return (S) this;
  }

  /**
   * Sets the timestamp the scan must be executed at, in microseconds since the Unix epoch. None is
   * used by default.
   * Requires that the ReadMode is READ_AT_SNAPSHOT.
   * @param timestamp a long representing an instant in microseconds since the unix epoch.
   * @return this instance
   * @throws IllegalArgumentException on build(), if the timestamp is less than 0 or if the
   *                                  read mode was not set to READ_AT_SNAPSHOT
   */
  public S snapshotTimestampMicros(long timestamp) {
    this.htTimestamp = HybridTimeUtil.physicalAndLogicalToHTTimestamp(timestamp, 0);
    return (S) this;
  }

  /**
   * Sets the start timestamp and end timestamp for a diff scan.
   * The timestamps should be encoded HT timestamps.
   *
   * Additionally sets any other scan properties required by diff scans.
   *
   * @param startTimestamp a long representing a HybridTime-encoded start timestamp
   * @param endTimestamp a long representing a HybridTime-encoded end timestamp
   * @return this instance
   */
  @InterfaceAudience.Private
  public S diffScan(long startTimestamp, long endTimestamp) {
    this.startTimestamp = startTimestamp;
    this.htTimestamp = endTimestamp;
    this.isFaultTolerant = true;
    this.readMode = AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT;
    return (S) this;
  }

  /**
   * Sets how long each scan request to a server can last.
   * Defaults to {@link KuduClient#getDefaultOperationTimeoutMs()}.
   * @param scanRequestTimeout a long representing time in milliseconds
   * @return this instance
   */
  public S scanRequestTimeout(long scanRequestTimeout) {
    this.scanRequestTimeout = scanRequestTimeout;
    return (S) this;
  }

  /**
   * Add a lower bound (inclusive) primary key for the scan.
   * If any bound is already added, this bound is intersected with that one.
   * @param partialRow a partial row with specified key columns
   * @return this instance
   */
  public S lowerBound(PartialRow partialRow) {
    return lowerBoundRaw(partialRow.encodePrimaryKey());
  }

  /**
   * Like lowerBoundPrimaryKey() but the encoded primary key is an opaque byte
   * array obtained elsewhere.
   * @param startPrimaryKey bytes containing an encoded start key
   * @return this instance
   * @deprecated use {@link #lowerBound(PartialRow)}
   */
  @Deprecated
  public S lowerBoundRaw(byte[] startPrimaryKey) {
    if (lowerBoundPrimaryKey.length == 0 ||
        Bytes.memcmp(startPrimaryKey, lowerBoundPrimaryKey) > 0) {
      this.lowerBoundPrimaryKey = startPrimaryKey;
    }
    return (S) this;
  }

  /**
   * Add an upper bound (exclusive) primary key for the scan.
   * If any bound is already added, this bound is intersected with that one.
   * @param partialRow a partial row with specified key columns
   * @return this instance
   */
  public S exclusiveUpperBound(PartialRow partialRow) {
    return exclusiveUpperBoundRaw(partialRow.encodePrimaryKey());
  }

  /**
   * Like exclusiveUpperBound() but the encoded primary key is an opaque byte
   * array obtained elsewhere.
   * @param endPrimaryKey bytes containing an encoded end key
   * @return this instance
   * @deprecated use {@link #exclusiveUpperBound(PartialRow)}
   */
  @Deprecated
  public S exclusiveUpperBoundRaw(byte[] endPrimaryKey) {
    if (upperBoundPrimaryKey.length == 0 ||
        Bytes.memcmp(endPrimaryKey, upperBoundPrimaryKey) < 0) {
      this.upperBoundPrimaryKey = endPrimaryKey;
    }
    return (S) this;
  }

  /**
   * Sets the replica selection mechanism for this scanner. The default is to read from the
   * currently known leader.
   * @param replicaSelection replication selection mechanism to use
   * @return this instance
   */
  public S replicaSelection(ReplicaSelection replicaSelection) {
    this.replicaSelection = replicaSelection;
    return (S) this;
  }

  /**
   * Set an encoded (inclusive) start partition key for the scan.
   *
   * @param partitionKey the encoded partition key
   * @return this instance
   */
  S lowerBoundPartitionKeyRaw(byte[] partitionKey) {
    if (Bytes.memcmp(partitionKey, lowerBoundPartitionKey) > 0) {
      this.lowerBoundPartitionKey = partitionKey;
    }
    return (S) this;
  }

  /**
   * Set an encoded (exclusive) end partition key for the scan.
   *
   * @param partitionKey the encoded partition key
   * @return this instance
   */
  S exclusiveUpperBoundPartitionKeyRaw(byte[] partitionKey) {
    if (upperBoundPartitionKey.length == 0 ||
        Bytes.memcmp(partitionKey, upperBoundPartitionKey) < 0) {
      this.upperBoundPartitionKey = partitionKey;
    }
    return (S) this;
  }

  /**
   * Set the period at which to send keep-alive requests to the tablet
   * server to ensure that this scanner will not time out.
   *
   * @param keepAlivePeriodMs the keep alive period in milliseconds
   * @return this instance
   */
  public S keepAlivePeriodMs(long keepAlivePeriodMs) {
    this.keepAlivePeriodMs = keepAlivePeriodMs;
    return (S) this;
  }

  public abstract T build();
}
