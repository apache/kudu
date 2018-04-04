/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kudu.client;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kudu.tserver.Tserver.NewScanRequestPB;
import static org.apache.kudu.tserver.Tserver.ScanRequestPB;
import static org.apache.kudu.tserver.Tserver.ScanResponsePB;
import static org.apache.kudu.tserver.Tserver.TabletServerErrorPB;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

/**
 * Creates a scanner to read data from Kudu.
 * <p>
 * This class is <strong>not synchronized</strong> as it's expected to be
 * used from a single thread at a time. It's rarely (if ever?) useful to
 * scan concurrently from a shared scanner using multiple threads. If you
 * want to optimize large table scans using extra parallelism, create a few
 * scanners through the {@link KuduScanToken} API. Or use MapReduce.
 * <p>
 * There's no method in this class to explicitly open the scanner. It will open
 * itself automatically when you start scanning by calling {@link #nextRows()}.
 * Also, the scanner will automatically call {@link #close} when it reaches the
 * end key. If, however, you would like to stop scanning <i>before reaching the
 * end key</i>, you <b>must</b> call {@link #close} before disposing of the scanner.
 * Note that it's always safe to call {@link #close} on a scanner.
 * <p>
 * A {@code AsyncKuduScanner} is not re-usable. Should you want to scan the same rows
 * or the same table again, you must create a new one.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link KuduRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class AsyncKuduScanner {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncKuduScanner.class);

  /**
   * The possible read modes for scanners.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum ReadMode {
    /**
     * When READ_LATEST is specified the server will always return committed writes at
     * the time the request was received. This type of read does not return a snapshot
     * timestamp and is not repeatable.
     *
     * In ACID terms this corresponds to Isolation mode: "Read Committed"
     *
     * This is the default mode.
     */
    READ_LATEST(Common.ReadMode.READ_LATEST),

    /**
     * When READ_AT_SNAPSHOT is specified the server will attempt to perform a read
     * at the provided timestamp. If no timestamp is provided the server will take the
     * current time as the snapshot timestamp. In this mode reads are repeatable, i.e.
     * all future reads at the same timestamp will yield the same data. This is
     * performed at the expense of waiting for in-flight transactions whose timestamp
     * is lower than the snapshot's timestamp to complete, so it might incur a latency
     * penalty.
     *
     * In ACID terms this, by itself, corresponds to Isolation mode "Repeatable
     * Read". If all writes to the scanned tablet are made externally consistent,
     * then this corresponds to Isolation mode "Strict-Serializable".
     *
     * Note: there currently "holes", which happen in rare edge conditions, by which writes
     * are sometimes not externally consistent even when action was taken to make them so.
     * In these cases Isolation may degenerate to mode "Read Committed". See KUDU-430.
     */
    READ_AT_SNAPSHOT(Common.ReadMode.READ_AT_SNAPSHOT),

    /**
     * When @c READ_YOUR_WRITES is specified, the client will perform a read
     * such that it follows all previously known writes and reads from this client.
     * Specifically this mode:
     *  (1) ensures read-your-writes and read-your-reads session guarantees,
     *  (2) minimizes latency caused by waiting for outstanding write
     *      transactions to complete.
     *
     * Reads in this mode are not repeatable: two READ_YOUR_WRITES reads, even if
     * they provide the same propagated timestamp bound, can execute at different
     * timestamps and thus may return different results.
     */
    READ_YOUR_WRITES(Common.ReadMode.READ_YOUR_WRITES);

    private Common.ReadMode pbVersion;
    ReadMode(Common.ReadMode pbVersion) {
      this.pbVersion = pbVersion;
    }

    @InterfaceAudience.Private
    public Common.ReadMode pbVersion() {
      return this.pbVersion;
    }
  }

  //////////////////////////
  // Initial configurations.
  //////////////////////////

  private final AsyncKuduClient client;
  private final KuduTable table;
  private final Schema schema;

  private final PartitionPruner pruner;

  /**
   * Map of column name to predicate.
   */
  private final Map<String, KuduPredicate> predicates;

  /**
   * Maximum number of bytes returned by the scanner, on each batch.
   */
  private final int batchSizeBytes;

  /**
   * The maximum number of rows to scan.
   */
  private final long limit;

  /**
   * Set in the builder. If it's not set by the user, it will default to EMPTY_ARRAY.
   * It is then reset to the new start primary key of each tablet we open a scanner on as the scan
   * moves from one tablet to the next.
   */
  private final byte[] startPrimaryKey;

  /**
   * Set in the builder. If it's not set by the user, it will default to EMPTY_ARRAY.
   * It's never modified after that.
   */
  private final byte[] endPrimaryKey;

  private byte[] lastPrimaryKey;

  private final boolean prefetching;

  private final boolean cacheBlocks;

  private final ReadMode readMode;

  private final Common.OrderMode orderMode;

  private final boolean isFaultTolerant;

  private long htTimestamp;

  private long lowerBoundPropagationTimestamp = AsyncKuduClient.NO_TIMESTAMP;

  private final ReplicaSelection replicaSelection;

  /////////////////////
  // Runtime variables.
  /////////////////////

  private boolean closed = false;

  private boolean hasMore = true;

  private long numRowsReturned = 0;

  /**
   * The tabletSlice currently being scanned.
   * If null, we haven't started scanning.
   * If == DONE, then we're done scanning.
   * Otherwise it contains a proper tabletSlice name, and we're currently scanning.
   */
  private RemoteTablet tablet;

  /**
   * This is the scanner ID we got from the TabletServer.
   * It's generated randomly so any value is possible.
   */
  private byte[] scannerId;

  /**
   * The sequence ID of this call. The sequence ID should start at 0
   * with the request for a new scanner, and after each successful request,
   * the client should increment it by 1. When retrying a request, the client
   * should _not_ increment this value. If the server detects that the client
   * missed a chunk of rows from the middle of a scan, it will respond with an
   * error.
   */
  private int sequenceId;

  private Deferred<RowResultIterator> prefetcherDeferred;

  final long scanRequestTimeout;

  AsyncKuduScanner(AsyncKuduClient client, KuduTable table, List<String> projectedNames,
                   List<Integer> projectedIndexes, ReadMode readMode, boolean isFaultTolerant,
                   long scanRequestTimeout,
                   Map<String, KuduPredicate> predicates, long limit,
                   boolean cacheBlocks, boolean prefetching,
                   byte[] startPrimaryKey, byte[] endPrimaryKey,
                   long htTimestamp, int batchSizeBytes, PartitionPruner pruner,
                   ReplicaSelection replicaSelection) {
    checkArgument(batchSizeBytes >= 0, "Need non-negative number of bytes, " +
        "got %s", batchSizeBytes);
    checkArgument(limit > 0, "Need a strictly positive number for the limit, " +
        "got %s", limit);
    if (htTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
      checkArgument(htTimestamp >= 0, "Need non-negative number for the scan, " +
          " timestamp got %s", htTimestamp);
      checkArgument(readMode == ReadMode.READ_AT_SNAPSHOT, "When specifying a " +
          "HybridClock timestamp, the read mode needs to be set to READ_AT_SNAPSHOT");
    }

    this.isFaultTolerant = isFaultTolerant;
    if (this.isFaultTolerant) {
      checkArgument(readMode == ReadMode.READ_AT_SNAPSHOT, "Use of fault tolerance scanner " +
          "requires the read mode to be set to READ_AT_SNAPSHOT");
      this.orderMode = Common.OrderMode.ORDERED;
    } else {
      this.orderMode = Common.OrderMode.UNORDERED;
    }

    this.client = client;
    this.table = table;
    this.pruner = pruner;
    this.readMode = readMode;
    this.scanRequestTimeout = scanRequestTimeout;
    this.predicates = predicates;
    this.limit = limit;
    this.cacheBlocks = cacheBlocks;
    this.prefetching = prefetching;
    this.startPrimaryKey = startPrimaryKey;
    this.endPrimaryKey = endPrimaryKey;
    this.htTimestamp = htTimestamp;
    this.batchSizeBytes = batchSizeBytes;
    this.lastPrimaryKey = AsyncKuduClient.EMPTY_ARRAY;

    // Map the column names to actual columns in the table schema.
    // If the user set this to 'null', we scan all columns.
    if (projectedNames != null) {
      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      for (String columnName : projectedNames) {
        ColumnSchema originalColumn = table.getSchema().getColumn(columnName);
        columns.add(getStrippedColumnSchema(originalColumn));
      }
      this.schema = new Schema(columns);
    } else if (projectedIndexes != null) {
      List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
      for (Integer columnIndex : projectedIndexes) {
        ColumnSchema originalColumn = table.getSchema().getColumnByIndex(columnIndex);
        columns.add(getStrippedColumnSchema(originalColumn));
      }
      this.schema = new Schema(columns);
    } else {
      this.schema = table.getSchema();
    }

    // If the partition pruner has pruned all partitions, then the scan can be
    // short circuited without contacting any tablet servers.
    if (!pruner.hasMorePartitionKeyRanges()) {
      LOG.debug("Short circuiting scan");
      this.hasMore = false;
      this.closed = true;
    }

    this.replicaSelection = replicaSelection;

    // For READ_YOUR_WRITES scan mode, get the latest observed timestamp
    // and store it. Always use this one as the propagated timestamp for
    // the duration of the scan to avoid unnecessary wait.
    if (readMode == ReadMode.READ_YOUR_WRITES) {
      this.lowerBoundPropagationTimestamp = this.client.getLastPropagatedTimestamp();
    }
  }

  /**
   * Clone the given column schema instance. The new instance will include only the name, type, and
   * nullability of the passed one.
   * @return a new column schema
   */
  private static ColumnSchema getStrippedColumnSchema(ColumnSchema columnToClone) {
    return new ColumnSchema.ColumnSchemaBuilder(columnToClone.getName(), columnToClone.getType())
        .nullable(columnToClone.isNullable())
        .typeAttributes(columnToClone.getTypeAttributes())
        .build();
  }

  /**
   * Returns the maximum number of rows that this scanner was configured to return.
   * @return a long representing the maximum number of rows that can be returned
   */
  public long getLimit() {
    return this.limit;
  }

  /**
   * Tells if the last rpc returned that there might be more rows to scan.
   * @return true if there might be more data to scan, else false
   */
  public boolean hasMoreRows() {
    return this.hasMore;
  }

  /**
   * Returns if this scanner was configured to cache data blocks or not.
   * @return true if this scanner will cache blocks, else else.
   */
  public boolean getCacheBlocks() {
    return this.cacheBlocks;
  }

  /**
   * Returns the maximum number of bytes returned by the scanner, on each batch.
   * @return a long representing the maximum number of bytes that a scanner can receive at once
   * from a tablet server
   */
  public long getBatchSizeBytes() {
    return this.batchSizeBytes;
  }

  /**
   * Returns the ReadMode for this scanner.
   * @return the configured read mode for this scanner
   */
  public ReadMode getReadMode() {
    return this.readMode;
  }

  private Common.OrderMode getOrderMode() {
    return this.orderMode;
  }

  /**
   * Returns the projection schema of this scanner. If specific columns were
   * not specified during scanner creation, the table schema is returned.
   * @return the projection schema for this scanner
   */
  public Schema getProjectionSchema() {
    return this.schema;
  }

  long getSnapshotTimestamp() {
    return this.htTimestamp;
  }

  /**
   * Scans a number of rows.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return a deferred list of rows.
   */
  public Deferred<RowResultIterator> nextRows() {
    if (closed) {  // We're already done scanning.
      return Deferred.fromResult(null);
    } else if (tablet == null) {
      Callback<Deferred<RowResultIterator>, AsyncKuduScanner.Response> cb =
          new Callback<Deferred<RowResultIterator>, Response>() {
        @Override
        public Deferred<RowResultIterator> call(Response resp) throws Exception {
          if (htTimestamp == AsyncKuduClient.NO_TIMESTAMP &&
              resp.scanTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
            // If the server-assigned timestamp is present in the tablet
            // server's response, store it in the scanner. The stored value
            // is used for read operations at other tablet servers in the
            // context of the same scan.
            htTimestamp = resp.scanTimestamp;
          }

          long lastPropagatedTimestamp = AsyncKuduClient.NO_TIMESTAMP;
          if (readMode == ReadMode.READ_YOUR_WRITES &&
              resp.scanTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
            // For READ_YOUR_WRITES mode, update the latest propagated timestamp
            // with the chosen snapshot timestamp sent back from the server, to
            // avoid unnecessarily wait for subsequent reads. Since as long as
            // the chosen snapshot timestamp of the next read is greater than
            // the previous one, the scan does not violate READ_YOUR_WRITES
            // session guarantees.
            lastPropagatedTimestamp = resp.scanTimestamp;
          } else if (resp.propagatedTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
            // Otherwise we just use the propagated timestamp returned from
            // the server as the latest propagated timestamp.
            lastPropagatedTimestamp = resp.propagatedTimestamp;
          }
          if (lastPropagatedTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
            client.updateLastPropagatedTimestamp(lastPropagatedTimestamp);
          }

          if (isFaultTolerant && resp.lastPrimaryKey != null) {
            lastPrimaryKey = resp.lastPrimaryKey;
          }

          numRowsReturned += resp.data.getNumRows();

          if (!resp.more || resp.scannerId == null) {
            scanFinished();
            return Deferred.fromResult(resp.data); // there might be data to return
          }
          scannerId = resp.scannerId;
          sequenceId++;
          hasMore = resp.more;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Scanner " + Bytes.pretty(scannerId) + " opened on " + tablet);
          }
          return Deferred.fromResult(resp.data);
        }

        public String toString() {
          return "scanner opened";
        }
      };

      Callback<Deferred<RowResultIterator>, Exception> eb =
          new Callback<Deferred<RowResultIterator>, Exception>() {
        @Override
        public Deferred<RowResultIterator> call(Exception e) throws Exception {
          invalidate();
          if (e instanceof NonCoveredRangeException) {
            NonCoveredRangeException ncre = (NonCoveredRangeException) e;
            pruner.removePartitionKeyRange(ncre.getNonCoveredRangeEnd());

            // Stop scanning if the non-covered range is past the end partition key.
            if (!pruner.hasMorePartitionKeyRanges()) {
              hasMore = false;
              closed = true; // the scanner is closed on the other side at this point
              return Deferred.fromResult(RowResultIterator.empty());
            }
            scannerId = null;
            sequenceId = 0;
            return nextRows();
          } else {
            LOG.debug("Can not open scanner", e);
            // Don't let the scanner think it's opened on this tablet.
            return Deferred.fromError(e); // Let the error propogate.
          }
        }

        public String toString() {
          return "open scanner errback";
        }
      };

      // We need to open the scanner first.
      return client.sendRpcToTablet(getOpenRequest()).addCallbackDeferring(cb).addErrback(eb);
    } else if (prefetching && prefetcherDeferred != null) {
      // TODO KUDU-1260 - Check if this works and add a test
      prefetcherDeferred.chain(new Deferred<RowResultIterator>().addCallback(prefetch));
      return prefetcherDeferred;
    }
    final Deferred<RowResultIterator> d =
        client.scanNextRows(this).addCallbacks(gotNextRow, nextRowErrback());
    if (prefetching) {
      d.chain(new Deferred<RowResultIterator>().addCallback(prefetch));
    }
    return d;
  }

  private final Callback<RowResultIterator, RowResultIterator> prefetch =
      new Callback<RowResultIterator, RowResultIterator>() {
    @Override
    public RowResultIterator call(RowResultIterator arg) throws Exception {
      if (hasMoreRows()) {
        prefetcherDeferred = client.scanNextRows(AsyncKuduScanner.this)
            .addCallbacks(gotNextRow, nextRowErrback());
      }
      return null;
    }
  };

  /**
   * Singleton callback to handle responses of "next" RPCs.
   * This returns an {@code ArrayList<ArrayList<KeyValue>>} (possibly inside a
   * deferred one).
   */
  private final Callback<RowResultIterator, Response> gotNextRow =
      new Callback<RowResultIterator, Response>() {
        public RowResultIterator call(final Response resp) {
          numRowsReturned += resp.data.getNumRows();
          if (!resp.more) {  // We're done scanning this tablet.
            scanFinished();
            return resp.data;
          }
          sequenceId++;
          hasMore = resp.more;
          //LOG.info("Scan.next is returning rows: " + resp.data.getNumRows());
          return resp.data;
        }

        public String toString() {
          return "get nextRows response";
        }
      };

  /**
   * Creates a new errback to handle errors while trying to get more rows.
   */
  private final Callback<Deferred<RowResultIterator>, Exception> nextRowErrback() {
    return new Callback<Deferred<RowResultIterator>, Exception>() {
      @Override
      public Deferred<RowResultIterator> call(Exception e) throws Exception {
        final RemoteTablet old_tablet = tablet;  // Save before invalidate().
        invalidate();  // If there was an error, don't assume we're still OK.
        // If encountered FaultTolerantScannerExpiredException, it means the
        // fault tolerant scanner on the server side expired. Therefore, open
        // a new scanner.
        if (e instanceof FaultTolerantScannerExpiredException) {
          scannerId = null;
          sequenceId = 0;
          LOG.warn("Scanner expired, creating a new one {}", AsyncKuduScanner.this);
          return nextRows();
        } else {
          LOG.warn("{} pretends to not know {}", old_tablet, AsyncKuduScanner.this, e);
          return Deferred.fromError(e); // Let the error propagate.
        }
      }

      public String toString() {
        return "NextRow errback";
      }
    };
  }

  void scanFinished() {
    Partition partition = tablet.getPartition();
    pruner.removePartitionKeyRange(partition.getPartitionKeyEnd());
    // Stop scanning if we have scanned until or past the end partition key, or
    // if we have fulfilled the limit.
    if (!pruner.hasMorePartitionKeyRanges() || numRowsReturned >= limit) {
      hasMore = false;
      closed = true; // the scanner is closed on the other side at this point
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Done scanning tablet {} for partition {} with scanner id {}",
                tablet.getTabletId(), tablet.getPartition(), Bytes.pretty(scannerId));
    }
    scannerId = null;
    sequenceId = 0;
    lastPrimaryKey = AsyncKuduClient.EMPTY_ARRAY;
    invalidate();
  }

  /**
   * Closes this scanner (don't forget to call this when you're done with it!).
   * <p>
   * Closing a scanner already closed has no effect.  The deferred returned
   * will be called back immediately.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} can be null, a RowResultIterator if there was data left
   * in the scanner, or an Exception.
   */
  public Deferred<RowResultIterator> close() {
    if (closed) {
      return Deferred.fromResult(null);
    }
    return client.closeScanner(this).addCallback(closedCallback()); // TODO errBack ?
  }

  /** Callback+Errback invoked when the TabletServer closed our scanner.  */
  private Callback<RowResultIterator, Response> closedCallback() {
    return new Callback<RowResultIterator, Response>() {
      public RowResultIterator call(Response response) {
        closed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner " + Bytes.pretty(scannerId) + " closed on " +
              tablet);
        }
        invalidate();
        scannerId = "client debug closed".getBytes();   // Make debugging easier.
        return response == null ? null : response.data;
      }

      public String toString() {
        return "scanner closed";
      }
    };
  }

  public String toString() {
    final String tablet = this.tablet == null ? "null" : this.tablet.getTabletId();
    final StringBuilder buf = new StringBuilder();
    buf.append("KuduScanner(table=");
    buf.append(table.getName());
    buf.append(", tablet=").append(tablet);
    buf.append(", scannerId=").append(Bytes.pretty(scannerId));
    buf.append(", scanRequestTimeout=").append(scanRequestTimeout);
    buf.append(')');
    return buf.toString();
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  KuduTable table() {
    return table;
  }

  /**
   * Sets the name of the tabletSlice that's hosting {@code this.start_key}.
   * @param tablet The tabletSlice we're currently supposed to be scanning.
   */
  void setTablet(final RemoteTablet tablet) {
    this.tablet = tablet;
  }

  /**
   * Invalidates this scanner and makes it assume it's no longer opened.
   * When a TabletServer goes away while we're scanning it, or some other type
   * of access problem happens, this method should be called so that the
   * scanner will have to re-locate the TabletServer and re-open itself.
   */
  void invalidate() {
    tablet = null;
  }

  /**
   * Returns the tabletSlice currently being scanned, if any.
   */
  RemoteTablet currentTablet() {
    return tablet;
  }

  /**
   * Gets the replica selection mechanism being used.
   *
   * @return the replica selection mechanism.
   */
  ReplicaSelection getReplicaSelection() {
    return replicaSelection;
  }

  /**
   * Returns an RPC to open this scanner.
   */
  KuduRpc<Response> getOpenRequest() {
    checkScanningNotStarted();
    return new ScanRequest(table, State.OPENING, tablet);
  }

  /**
   * Returns an RPC to fetch the next rows.
   */
  KuduRpc<Response> getNextRowsRequest() {
    return new ScanRequest(table, State.NEXT, tablet);
  }

  /**
   * Returns an RPC to close this scanner.
   */
  KuduRpc<Response> getCloseRequest() {
    return new ScanRequest(table, State.CLOSING, tablet);
  }

  /**
   * Throws an exception if scanning already started.
   * @throws IllegalStateException if scanning already started.
   */
  private void checkScanningNotStarted() {
    if (tablet != null) {
      throw new IllegalStateException("scanning already started");
    }
  }

  /**
   *  Helper object that contains all the info sent by a TS after a Scan request.
   */
  static final class Response {
    /** The ID associated with the scanner that issued the request.  */
    private final byte[] scannerId;
    /** The actual payload of the response.  */
    private final RowResultIterator data;

    /**
     * If false, the filter we use decided there was no more data to scan.
     * In this case, the server has automatically closed the scanner for us,
     * so we don't need to explicitly close it.
     */
    private final boolean more;

    /**
     * Server-assigned timestamp for the scan operation. It's used when
     * the scan operates in READ_AT_SNAPSHOT mode and the timestamp is not
     * specified explicitly. The field is set with the snapshot timestamp sent
     * in the response from the very first tablet server contacted while
     * fetching data from corresponding tablets. If the tablet server does not
     * send the snapshot timestamp in its response, this field is assigned
     * a special value AsyncKuduClient.NO_TIMESTAMP.
     */
    private final long scanTimestamp;

    /**
     * The server timestamp to propagate, if set. If the server response does
     * not contain propagated timestamp, this field is set to special value
     * AsyncKuduClient.NO_TIMESTAMP
     */
    private final long propagatedTimestamp;

    private final byte[] lastPrimaryKey;

    Response(final byte[] scannerId,
             final RowResultIterator data,
             final boolean more,
             final long scanTimestamp,
             final long propagatedTimestamp,
             final byte[] lastPrimaryKey) {
      this.scannerId = scannerId;
      this.data = data;
      this.more = more;
      this.scanTimestamp = scanTimestamp;
      this.propagatedTimestamp = propagatedTimestamp;
      this.lastPrimaryKey = lastPrimaryKey;
    }

    public String toString() {
      String ret = "AsyncKuduScanner$Response(scannerId = " + Bytes.pretty(scannerId) +
          ", data = " + data + ", more = " + more;
      if (scanTimestamp != AsyncKuduClient.NO_TIMESTAMP) {
        ret += ", responseScanTimestamp = " + scanTimestamp;
      }
      ret += ")";
      return ret;
    }
  }

  private enum State {
    OPENING,
    NEXT,
    CLOSING
  }

  /**
   * RPC sent out to fetch the next rows from the TabletServer.
   */
  final class ScanRequest extends KuduRpc<Response> {

    State state;

    ScanRequest(KuduTable table, State state, RemoteTablet tablet) {
      super(table);
      setTablet(tablet);
      this.state = state;
      this.setTimeoutMillis(scanRequestTimeout);
    }

    @Override
    String serviceName() {
      return TABLET_SERVER_SERVICE_NAME;
    }

    @Override
    String method() {
      return "Scan";
    }

    @Override
    Collection<Integer> getRequiredFeatures() {
      if (predicates.isEmpty()) {
        return ImmutableList.of();
      } else {
        return ImmutableList.of(Tserver.TabletServerFeatures.COLUMN_PREDICATES_VALUE);
      }
    }

    @Override
    ReplicaSelection getReplicaSelection() {
      return replicaSelection;
    }

    /** Serializes this request.  */
    @Override
    Message createRequestPB() {
      final ScanRequestPB.Builder builder = ScanRequestPB.newBuilder();
      switch (state) {
        case OPENING:
          // Save the tablet in the AsyncKuduScanner.  This kind of a kludge but it really
          // is the easiest way.
          AsyncKuduScanner.this.tablet = super.getTablet();
          NewScanRequestPB.Builder newBuilder = NewScanRequestPB.newBuilder();
          newBuilder.setLimit(limit - AsyncKuduScanner.this.numRowsReturned);
          newBuilder.addAllProjectedColumns(ProtobufHelper.schemaToListPb(schema));
          newBuilder.setTabletId(UnsafeByteOperations.unsafeWrap(tablet.getTabletIdAsBytes()));
          newBuilder.setOrderMode(AsyncKuduScanner.this.getOrderMode());
          newBuilder.setCacheBlocks(cacheBlocks);
          // If the last propagated timestamp is set, send it with the scan.
          // For READ_YOUR_WRITES scan, use the propagated timestamp from
          // the scanner.
          long timestamp;
          if (readMode == ReadMode.READ_YOUR_WRITES) {
            timestamp = lowerBoundPropagationTimestamp;
          } else {
            timestamp = table.getAsyncClient().getLastPropagatedTimestamp();
          }
          if (timestamp != AsyncKuduClient.NO_TIMESTAMP) {
            newBuilder.setPropagatedTimestamp(timestamp);
          }
          newBuilder.setReadMode(AsyncKuduScanner.this.getReadMode().pbVersion());

          // if the mode is set to read on snapshot sent the snapshot timestamp
          if (AsyncKuduScanner.this.getReadMode() == ReadMode.READ_AT_SNAPSHOT &&
              AsyncKuduScanner.this.getSnapshotTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
            newBuilder.setSnapTimestamp(AsyncKuduScanner.this.getSnapshotTimestamp());
          }

          if (isFaultTolerant) {
            if (AsyncKuduScanner.this.lastPrimaryKey.length > 0) {
              newBuilder.setLastPrimaryKey(UnsafeByteOperations.unsafeWrap(lastPrimaryKey));
            }
          }

          if (AsyncKuduScanner.this.startPrimaryKey.length > 0) {
            newBuilder.setStartPrimaryKey(UnsafeByteOperations.unsafeWrap(startPrimaryKey));
          }

          if (AsyncKuduScanner.this.endPrimaryKey.length > 0) {
            newBuilder.setStopPrimaryKey(UnsafeByteOperations.unsafeWrap(endPrimaryKey));
          }

          for (KuduPredicate pred : predicates.values()) {
            newBuilder.addColumnPredicates(pred.toPB());
          }
          builder.setNewScanRequest(newBuilder.build())
                 .setBatchSizeBytes(batchSizeBytes);
          break;
        case NEXT:
          builder.setScannerId(UnsafeByteOperations.unsafeWrap(scannerId))
                 .setCallSeqId(AsyncKuduScanner.this.sequenceId)
                 .setBatchSizeBytes(batchSizeBytes);
          break;
        case CLOSING:
          builder.setScannerId(UnsafeByteOperations.unsafeWrap(scannerId))
                 .setBatchSizeBytes(0)
                 .setCloseScanner(true);
          break;
        default:
          throw new RuntimeException("unreachable!");
      }

      return builder.build();
    }

    @Override
    Pair<Response, Object> deserialize(final CallResponse callResponse,
                                       String tsUUID) throws KuduException {
      ScanResponsePB.Builder builder = ScanResponsePB.newBuilder();
      readProtobuf(callResponse.getPBMessage(), builder);
      ScanResponsePB resp = builder.build();
      final byte[] id = resp.getScannerId().toByteArray();
      TabletServerErrorPB error = resp.hasError() ? resp.getError() : null;

      // Error handling.
      if (error != null) {
        switch (error.getCode()) {
          case TABLET_NOT_FOUND:
          case TABLET_NOT_RUNNING:
            if (state == State.OPENING || (state == State.NEXT && isFaultTolerant)) {
              // Doing this will trigger finding the new location.
              return new Pair<Response, Object>(null, error);
            } else {
              Status statusIncomplete = Status.Incomplete("Cannot continue scanning, " +
                  "the tablet has moved and this isn't a fault tolerant scan");
              throw new NonRecoverableException(statusIncomplete);
            }
          case SCANNER_EXPIRED:
            if (isFaultTolerant) {
              Status status = Status.fromTabletServerErrorPB(error);
              throw new FaultTolerantScannerExpiredException(status);
            }
            // fall through
          default:
            break;
        }
      }
      RowResultIterator iterator = RowResultIterator.makeRowResultIterator(
          deadlineTracker.getElapsedMillis(), tsUUID, schema, resp.getData(),
          callResponse);

      boolean hasMore = resp.getHasMoreResults();
      if (id.length != 0 && scannerId != null && !Bytes.equals(scannerId, id)) {
        Status statusIllegalState = Status.IllegalState("Scan RPC response was for scanner" +
            " ID " + Bytes.pretty(id) + " but we expected " +
            Bytes.pretty(scannerId));
        throw new NonRecoverableException(statusIllegalState);
      }
      Response response = new Response(id, iterator, hasMore,
          resp.hasSnapTimestamp() ? resp.getSnapTimestamp()
                                  : AsyncKuduClient.NO_TIMESTAMP,
          resp.hasPropagatedTimestamp() ? resp.getPropagatedTimestamp()
                                        : AsyncKuduClient.NO_TIMESTAMP,
          resp.getLastPrimaryKey().toByteArray());
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} for scanner {}", response.toString(), AsyncKuduScanner.this);
      }
      return new Pair<Response, Object>(response, error);
    }

    public String toString() {
      return "ScanRequest(scannerId=" + Bytes.pretty(scannerId) +
          (tablet != null ? ", tablet=" + tablet.getTabletId() : "") +
          ", attempt=" + attempt + ", " + super.toString() + ")";
    }

    @Override
    public byte[] partitionKey() {
      // This key is used to lookup where the request needs to go
      return pruner.nextPartitionKey();
    }
  }

  /**
   * A Builder class to build {@link AsyncKuduScanner}.
   * Use {@link AsyncKuduClient#newScannerBuilder} in order to get a builder instance.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static class AsyncKuduScannerBuilder
      extends AbstractKuduScannerBuilder<AsyncKuduScannerBuilder, AsyncKuduScanner> {

    AsyncKuduScannerBuilder(AsyncKuduClient client, KuduTable table) {
      super(client, table);
    }

    /**
     * Builds an {@link AsyncKuduScanner} using the passed configurations.
     * @return a new {@link AsyncKuduScanner}
     */
    public AsyncKuduScanner build() {
      return new AsyncKuduScanner(
          client, table, projectedColumnNames, projectedColumnIndexes, readMode, isFaultTolerant,
          scanRequestTimeout, predicates, limit, cacheBlocks,
          prefetching, lowerBoundPrimaryKey, upperBoundPrimaryKey,
          htTimestamp, batchSizeBytes, PartitionPruner.create(this), replicaSelection);
    }
  }
}
