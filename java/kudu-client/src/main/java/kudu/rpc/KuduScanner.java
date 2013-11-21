/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 * Portions copyright (c) 2014 Cloudera, Inc.
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
package kudu.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.Schema;
import kudu.WireProtocol;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.util.Slice;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static kudu.tserver.Tserver.*;

/**
 * Creates a scanner to read data sequentially from HBase.
 * <p>
 * This class is <strong>not synchronized</strong> as it's expected to be
 * used from a single thread at a time.  It's rarely (if ever?) useful to
 * scan concurrently from a shared scanner using multiple threads.  If you
 * want to optimize large table scans using extra parallelism, create a few
 * scanners and give each of them a partition of the table to scan.  Or use
 * MapReduce.
 * <p>
 * Unlike HBase's traditional client, there's no method in this class to
 * explicitly open the scanner.  It will open itself automatically when you
 * start scanning by calling {@link #nextRows()}.  Also, the scanner will
 * automatically call {@link #close} when it reaches the end key.  If, however,
 * you would like to stop scanning <i>before reaching the end key</i>, you
 * <b>must</b> call {@link #close} before disposing of the scanner.  Note that
 * it's always safe to call {@link #close} on a scanner.
 * <p>
 * A {@code Scanner} is not re-usable.  Should you want to scan the same rows
 * or the same table again, you must create a new one.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link KuduRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class KuduScanner {

  private static final Logger LOG = LoggerFactory.getLogger(KuduScanner.class);
  private final KuduClient client;
  private final KuduTable table;
  private Deferred<RowResultIterator> prefetcherDeferred;
  private final List<ColumnRangePredicatePB> columnRangePredicates = new ArrayList<ColumnRangePredicatePB>();

  /**
   * Maximum number of bytes to fetch at a time.\
   * @see #setMaxNumBytes
   */
  private int max_num_bytes = 1024*1024; // TODO

  /**
   * The tabletSlice currently being scanned.
   * If null, we haven't started scanning.
   * If == DONE, then we're done scanning.
   * Otherwise it contains a proper tabletSlice name, and we're currently scanning.
   */
  private Slice tablet;

  /**
   * This is the scanner ID we got from the RegionServer.
   * It's generated randomly so any value is possible.
   */
  private byte[] scanner_id;

  /**
   * The sequence ID of this call. The sequence ID should start at 0
   * with the request for a new scanner, and after each successful request,
   * the client should increment it by 1. When retrying a request, the client
   * should _not_ increment this value. If the server detects that the client
   * missed a chunk of rows from the middle of a scan, it will respond with an
   * error.
   */
  private int sequenceId;

  /**
   * The maximum number of rows to scan.
   */
  private long limit = Long.MAX_VALUE;

  private boolean closed = false;

  private Schema schema;

  private boolean hasMore = true;

  private boolean prefetching;

  /**
   * Constructor.
   * <strong>This byte array will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   */
  KuduScanner(final KuduClient client, final KuduTable table) {
    this.client = client;
    this.table = table;
  }

  /**
   * Sets the maximum number of bytes returned at once by the scanner.
   * <p>
   * HBase may actually return more than this many bytes because it will not
   * truncate a rowResult in the middle.
   * <p>
   * This value is only used when communicating with HBase 0.95 and newer.
   * For older versions of HBase this value is silently ignored.
   * @param max_num_bytes A strictly positive number of bytes.
   * @since 1.5
   * @throws IllegalStateException if scanning already started.
   * @throws IllegalArgumentException if {@code max_num_bytes <= 0}
   */
  public void setMaxNumBytes(final int max_num_bytes) {
    if (max_num_bytes <= 0) {
      throw new IllegalArgumentException("Need a strictly positive number of"
          + " bytes, got " + max_num_bytes);
    }
    checkScanningNotStarted();
    this.max_num_bytes = max_num_bytes;
  }

  /**
   * Returns the maximum number of bytes returned at once by the scanner.
   * @see #setMaxNumBytes
   * @since 1.5
   */
  public long getMaxNumBytes() {
    return max_num_bytes;
  }

  /**
   * TODO
   * Scans a number of rows.  Calling this method is equivalent to:
   * <pre>
   *   this.{@link #nextRows() nextRows}();
   * </pre>
   * @param nrows The maximum number of rows to retrieve.
   * @return A deferred list of rows.
   * @see #nextRows()
   */
  /*public Deferred<List<RowResult>> nextRows(final int nrows) {
    return nextRows();
  }*/

  /**
   * Scans a number of rows.
   * <p>
   * Once this method returns {@code null} once (which indicates that this
   * {@code Scanner} is done scanning), calling it again leads to an undefined
   * behavior.
   * @return A deferred list of rows.
   */
  public Deferred<RowResultIterator> nextRows() {
    if (closed) {  // We're already done scanning.
      return Deferred.fromResult(null);
    } else if (tablet == null) {  // We need to open the scanner first.
      return client.openScanner(this).addCallbackDeferring(
          new Callback<Deferred<RowResultIterator>, Object>() {
            public Deferred<RowResultIterator> call(final Object response) {
              if (! (response instanceof Response)) {
                throw new IllegalStateException("WTF? Scanner open callback"
                    + " invoked with impossible"
                    + " argument: " + response);
              }
              final Response resp = (Response) response;
              if (resp.error.hasCode()) {
                // TODO more specific error parsing
                LOG.error(resp.error.getStatus().getMessage());
                throw new NonRecoverableException(resp.error.getStatus().getMessage());
              }
              if (!resp.more || resp.scanner_id == null) {
                hasMore = false;
                closed = true; // the scanner is closed on the other side at this point
                return Deferred.fromResult(resp.data); // there might be data to return
              }
              scanner_id = resp.scanner_id;
              sequenceId++;
              hasMore = resp.more;
              if (LOG.isDebugEnabled()) {
                LOG.debug("Scanner " + Bytes.pretty(scanner_id) + " opened on " + tablet);
              }
              //LOG.info("Scan.open is returning rows: " + resp.data.getNumRows());
              return Deferred.fromResult(resp.data);
            }
            public String toString() {
              return "scanner opened";
            }
          });
    } else if (prefetching && prefetcherDeferred != null) {
      prefetcherDeferred.chain(new Deferred<RowResultIterator>().addCallback(prefetch));
      return prefetcherDeferred;
    }
    // Need to silence this warning because the callback `got_next_row'
    // declares its return type to be Object, because its return value
    // may or may not be deferred.
    @SuppressWarnings("unchecked")
    final Deferred<RowResultIterator> d = (Deferred)
        client.scanNextRows(this).addCallbacks(got_next_row, nextRowErrback());
    if (prefetching) {
      d.chain(new Deferred<RowResultIterator>().addCallback(prefetch));
    }
    return d;
  }

  private final Callback<RowResultIterator, RowResultIterator> prefetch = new Callback() {
    @Override
    public RowResultIterator call(Object arg) throws Exception {
      if (hasMoreRows()) {
        prefetcherDeferred = (Deferred)client.scanNextRows(KuduScanner.this).addCallbacks
            (got_next_row, nextRowErrback());
      }
      return null;
    }
  };

  /**
   * Singleton callback to handle responses of "next" RPCs.
   * This returns an {@code ArrayList<ArrayList<KeyValue>>} (possibly inside a
   * deferred one).
   */
  private final Callback<Object, Object> got_next_row =
      new Callback<Object, Object>() {
        public Object call(final Object response) {
          //System.out.println("got_next_row");
          if (!(response instanceof Response)) {
            throw new InvalidResponseException(Response.class, response);
          }
          Response resp = (Response) response;
          if (resp.error.hasCode()) {
            // TODO more specific error parsing
            LOG.error(resp.error.getStatus().getMessage());
            throw new NonRecoverableException(resp.error.getStatus().getMessage());
          }
          sequenceId++;
          hasMore = resp.more;
          if (!hasMore) {  // We're done scanning this tablet.
            closed = true;
            // TODO go to the next tabletSlice instead
          }
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
  private final Callback<Object, Object> nextRowErrback() {
    return new Callback<Object, Object>() {
      public Object call(final Object error) {
        final Slice old_tablet = tablet;  // Save before invalidate().
        String message = old_tablet + " pretends to not know " + KuduScanner.this;
        if (error instanceof Exception) {
          LOG.warn(message, (Exception)error);
        } else {
          LOG.warn(message + " because of " + error);
        }
        invalidate();  // If there was an error, don't assume we're still OK.
        return error;  // Let the error propagate.
      }
      public String toString() {
        return "NextRow errback";
      }
    };
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
    // Need to silence this warning because the callback `got_next_row'
    // declares its return type to be Object, because its return value
    // may or may not be deferred.
    @SuppressWarnings("unchecked")
    final Deferred<RowResultIterator> d = (Deferred)
       client.closeScanner(this).addBoth(closedCallback());
    return d;
  }

  /** Callback+Errback invoked when the RegionServer closed our scanner.  */
  private Callback<Object, Object> closedCallback() {
    return new Callback<Object, Object>() {
      public Object call(Object response) {
        Object returnedObj = null;
        if (response instanceof Exception) {
          returnedObj = response;
        } else if (response instanceof Response) {
          returnedObj = ((Response)response).data;
        }
        closed = true;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner " + Bytes.pretty(scanner_id) + " closed on "
              + tablet);
        }
        tablet = null;
        scanner_id = "client debug closed".getBytes();   // Make debugging easier.
        //LOG.info("Scan.close is returning rows: " + resp.data.getNumRows());
        return returnedObj;
      }
      public String toString() {
        return "scanner closed";
      }
    };
  }

  public String toString() {
    final String tablet = this.tablet == null ? "null" : this.tablet.toString();
    final StringBuilder buf = new StringBuilder();
    buf.append("KuduScanner(table=");
    buf.append(table.getName());
    buf.append("}")
        .append(", tabletSlice=").append(tablet);
    buf.append(", scanner_id=").append(Bytes.pretty(scanner_id))
        .append(')');
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
  void setTablet(final Slice tablet) {
    this.tablet = tablet;
  }

  /**
   * Invalidates this scanner and makes it assume it's no longer opened.
   * When a RegionServer goes away while we're scanning it, or some other type
   * of access problem happens, this method should be called so that the
   * scanner will have to re-locate the RegionServer and re-open itself.
   */
  void invalidate() {
    tablet = null;
    hasMore = false;
  }

  /**
   * Returns the tabletSlice currently being scanned, if any.
   */
  Slice currentTablet() {
    return tablet;
  }

  /**
   *
   * @return
   */
  public long getLimit() {
    return limit;
  }

  /**
   *
   * @param limit
   */
  public void setLimit(long limit) {
    this.limit = limit;
  }

  /**
   *
   * @param schema
   */
  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public void addColumnRangePredicate(ColumnRangePredicatePB crppb) {
    this.columnRangePredicates.add(crppb);
  }

  /**
   * Tells if the last rpc returned that there might be more rows to scan
   * @return true if there might be more data to scan, else false
   */
  public boolean hasMoreRows() {
    return this.hasMore;
  }

  public void setPrefetching(boolean prefetching) {
    this.prefetching = prefetching;
  }

  /**
   * Returns an RPC to open this scanner.
   */
  KuduRpc getOpenRequest() {
    return new ScanRequest(table, State.OPENING);
  }

  /**
   * Returns an RPC to fetch the next rows.
   */
  KuduRpc getNextRowsRequest() {
    return new ScanRequest(table, State.NEXT);
  }

  /**
   * Returns an RPC to close this scanner.
   */
  KuduRpc getCloseRequest() {
    return new ScanRequest(table, State.CLOSING);
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
   *  TODO
   */
  final static class Response {
    /** The ID associated with the scanner that issued the request.  */
    private final byte[] scanner_id;
    /** The actual payload of the response.  */
    private final RowResultIterator data;

    private final TabletServerErrorPB error;
    /**
     * If false, the filter we use decided there was no more data to scan.
     * In this case, the server has automatically closed the scanner for us,
     * so we don't need to explicitly close it.
     */
    private final boolean more;

    Response(final byte[] scanner_id,
             final RowResultIterator data,
             final boolean more,
             final TabletServerErrorPB error) {
      this.scanner_id = scanner_id;
      this.data = data;
      this.more = more;
      this.error = error;
    }

    public String toString() {
      return "KuduScanner$Response(scanner_id=" + Bytes.pretty(scanner_id)
          + ", data=" + data + ", more=" + more + ", error = " + error+  ") ";
    }
  }

  private enum State {
    OPENING,
    NEXT,
    CLOSING
  }

  /**
   * RPC sent out to fetch the next rows from the RegionServer.
   */
  private final class ScanRequest extends KuduRpc {

    State state;

    ScanRequest(KuduTable table, State state) {
      super(table);
      this.state = state;
    }

    @Override
    String method() {
      return "Scan";
    }

    /** Serializes this request.  */
    ChannelBuffer serialize(Message header) {
      final ScanRequestPB.Builder builder = ScanRequestPB.newBuilder();
      switch (state) {
        case OPENING:
          // Save the tablet in the KuduScanner.  This kind of a kludge but it really
          // is the easiest way.
          KuduScanner.this.tablet = super.getTablet();
          NewScanRequestPB.Builder newBuilder = NewScanRequestPB.newBuilder();
          newBuilder.setLimit(limit); // currently ignored
          newBuilder.addAllProjectedColumns(ProtobufHelper.schemaToListPb(schema));
          newBuilder.setTabletId(ByteString.copyFrom(tablet.getBytes()));
          if (!columnRangePredicates.isEmpty()) {
            newBuilder.addAllRangePredicates(columnRangePredicates);
          }
          builder.setNewScanRequest(newBuilder.build())
                 .setBatchSizeBytes(max_num_bytes);
          break;
        case NEXT:
          builder.setScannerId(ByteString.copyFrom(scanner_id))
                 .setCallSeqId(sequenceId)
                 .setBatchSizeBytes(max_num_bytes);
          break;
        case CLOSING:
          builder.setScannerId(ByteString.copyFrom(scanner_id))
                 .setBatchSizeBytes(0)
                 .setCloseScanner(true);
      }
      return toChannelBuffer(header, builder.build());
    }

    @Override
    Response deserialize(final ChannelBuffer buf) {
      ScanResponsePB.Builder builder = ScanResponsePB.newBuilder();
      readProtobuf(buf, builder);
      ScanResponsePB resp = builder.build();
      final byte[] id = resp.getScannerId().toByteArray();
      TabletServerErrorPB error = resp.getError();
      //List<RowResult> rows = ProtobufHelper.extractRowsFromRowBlockPB(schema, resp.getData());
      RowResultIterator iterator = new RowResultIterator(schema, resp.getData());

      boolean hasMore = resp.getHasMoreResults();
      if (id.length  != 0 && scanner_id != null && !Bytes.equals(scanner_id, id)) {
        throw new InvalidResponseException("Scan RPC response was for scanner"
            + " ID " + Bytes.pretty(id) + " but we expected "
            + Bytes.pretty(scanner_id), resp);
      }
      Response response = new Response(id, iterator, hasMore, error);
      if (LOG.isDebugEnabled()) {
        LOG.debug(response.toString());
      }
      return response;
    }

    public String toString() {
      return "ScanRequest(scanner_id=" + Bytes.pretty(scanner_id)
          + (tablet != null? ", tabletSlice=" + tablet.toString(Charset.defaultCharset()) : "")
          + ", attempt=" + attempt + ')';
    }

  }

  /**
   * TODO
   */
  public class RowResultIterator implements Iterator<RowResult> {

    private final Schema schema;
    private final byte[] bs;
    private final byte[] indirectBs;
    private final int numRows;
    private final RowResult rowResult;
    private int currentRow = 0;

    /**
     * Private constructor, only meant to be instantiated from KuduScanner.
     * @param schema Schema used to parse the rows
     * @param data PB containing the data
     */
    private RowResultIterator(Schema schema, WireProtocol.RowwiseRowBlockPB data) {
      this.schema = schema;
      if (data == null || data.getNumRows() == 0) {
        this.bs = this.indirectBs = null;
        this.rowResult = null;
        this.numRows = 0;
        return;
      }
      // Hack, asynchbase and hbase do the same
      this.bs = ZeroCopyLiteralByteString.zeroCopyGetBytes(data.getRows());
      this.indirectBs =  ZeroCopyLiteralByteString.zeroCopyGetBytes(data.getIndirectData());
      this.numRows = data.getNumRows();

      // Integrity check
      int rowSize = schema.getRowSize();
      int expectedSize = numRows * rowSize;
      if (expectedSize != bs.length) {
        throw new NonRecoverableException("RowResult block has " + bs.length + " bytes of data " +
            "but expected " + expectedSize + "  for " + numRows + " rows");
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
      throw new NotImplementedException();
    }

    /**
     * Get the number of rows in this iterator. If all you want is to count
     * rows, call this and skip the rest.
     * @return number of rows in this iterator
     */
    public int getNumRows() {
      return this.numRows;
    }
  }
}

// TODO when multiple tablets
  /*private Deferred<ArrayList<ArrayList<KeyValue>>> scanFinished(final Response resp) {
    final byte[] region_stop_key = tabletSlice.stopKey();
    // Check to see if this tabletSlice is the last we should scan (either
    // because (1) it's the last tabletSlice or (3) because its stop_key is
    // greater than or equal to the stop_key of this scanner provided
    // that (2) we're not trying to scan until the end of the table).
    if (region_stop_key == EMPTY_ARRAY                           // (1)
        || (stop_key != EMPTY_ARRAY                              // (2)
        && Bytes.memcmp(stop_key, region_stop_key) <= 0)) {  // (3)
      get_next_rows_request = null;        // free();
      families = null;                     // free();
      qualifiers = null;                   // free();
      start_key = stop_key = EMPTY_ARRAY;  // free() but mustn't be null.
      if (resp != null && !resp.more) {
        return null;  // The server already closed the scanner for us.
      }
      return close()  // Auto-close the scanner.
          .addCallback(new Callback<ArrayList<ArrayList<KeyValue>>, Object>() {
            public ArrayList<ArrayList<KeyValue>> call(final Object arg) {
              return null;  // Tell the user there's nothing more to scan.
            }
            public String toString() {
              return "auto-close scanner " + Bytes.hex(scanner_id);
            }
          });
    }
    return continueScanOnNextRegion();
  }*/

/**
 * TODO when multiple tablets
 * Continues scanning on the next tabletSlice.
 * <p>
 * This method is called when we tried to get more rows but we reached the
 * end of the current tabletSlice and need to move on to the next tabletSlice.
 * <p>
 * This method closes the scanner on the current tabletSlice, updates the start
 * key of this scanner and resumes scanning on the next tabletSlice.
 * @return The deferred results from the next tabletSlice.
 */
  /*private Deferred<ArrayList<ArrayList<KeyValue>>> continueScanOnNextRegion() {
    // Copy those into local variables so we can still refer to them in the
    // "closure" below even after we've changed them.
    final long old_scanner_id = scanner_id;
    final String old_region = tabletSlice;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Scanner " + Bytes.hex(old_scanner_id) + " done scanning "
          + old_region);
    }
    client.closeScanner(this).addCallback(new Callback<Object, Object>() {
      public Object call(final Object arg) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Scanner " + Bytes.hex(old_scanner_id) + " closed on "
              + old_region);
        }
        return arg;
      }
      public String toString() {
        return "scanner moved";
      }
    });
    // Continue scanning from the next tabletSlice's start key.
    start_key = tabletSlice.stopKey();
    scanner_id = 0xDEAD000AA000DEADL;   // Make debugging easier.
    invalidate();
    return nextRows();
  }*/