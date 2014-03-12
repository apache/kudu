// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.tserver.Tserver;
import kudu.util.Slice;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A KuduSession belongs to a specific KuduClient, and represents a context in
 * which all read/write data access should take place. Within a session,
 * multiple operations may be accumulated and batched together for better
 * efficiency. Settings like timeouts, priorities, and trace IDs are also set
 * per session.
 *
 * KuduSession is separate from KuduTable because a given batch or transaction
 * may span multiple tables. This is particularly important in the future when
 * we add ACID support, but even in the context of batching, we may be able to
 * coalesce writes to different tables hosted on the same server into the same
 * RPC.
 *
 * KuduSession is separate from KuduClient because, in a multi-threaded
 * application, different threads may need to concurrently execute
 * transactions. Similar to a JDBC "session", transaction boundaries will be
 * delineated on a per-session basis -- in between a "BeginTransaction" and
 * "Commit" call on a given session, all operations will be part of the same
 * transaction. Meanwhile another concurrent Session object can safely run
 * non-transactional work or other transactions without interfering.
 *
 * Additionally, there is a guarantee that writes from different sessions do not
 * get batched together into the same RPCs -- this means that latency-sensitive
 * clients can run through the same KuduClient object as throughput-oriented
 * clients, perhaps by setting the latency-sensitive session's timeouts low and
 * priorities high. Without the separation of batches, a latency-sensitive
 * single-row insert might get batched along with 10MB worth of inserts from the
 * batch writer, thus delaying the response significantly.
 *
 * Though we currently do not have transactional support, users will be forced
 * to use a KuduSession to instantiate reads as well as writes.  This will make
 * it more straight-forward to add RW transactions in the future without
 * significant modifications to the API.
 */
public class KuduSession {

  public static final Logger LOG = LoggerFactory.getLogger(KuduSession.class);

  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  public enum FlushMode {
    // Every write will be sent to the server in-band with the Apply()
    // call. No batching will occur. This is the default flush mode. In this
    // mode, the Flush() call never has any effect, since each Apply() call
    // has already flushed the buffer.
    AUTO_FLUSH_SYNC,

    // Apply() calls will return immediately, but the writes will be sent in
    // the background, potentially batched together with other writes from
    // the same session. If there is not sufficient buffer space, then Apply()
    // may block for buffer space to be available.
    //
    // Because writes are applied in the background, any errors will be stored
    // in a session-local buffer. Call CountPendingErrors() or GetPendingErrors()
    // to retrieve them.
    //
    // The Flush() call can be used to block until the buffer is empty.
    AUTO_FLUSH_BACKGROUND,

    // Apply() calls will return immediately, and the writes will not be
    // sent until the user calls Flush(). If the buffer runs past the
    // configured space limit, then Apply() will return an error.
    MANUAL_FLUSH
  }

  private final KuduClient client;
  private int interval = 1000;
  private int mutationBufferSpace = 1000;
  private FlushMode flushMode;

  @GuardedBy("this")
  private final Map <Slice, Batch> operations = new HashMap<Slice, Batch>();

  /**
   * Package-private constructor meant to be used via KuduClient
   * @param client client that creates this session
   */
  KuduSession(KuduClient client) {
    this.client = client;
    this.flushMode = FlushMode.AUTO_FLUSH_SYNC;
  }

  /**
   * Set the new flush mode for this session
   * @param flushMode new flush mode, can be the same as the previous one
   * @throws IllegalArgumentException if the buffer isn't empty
   */
  public void setFlushMode(FlushMode flushMode) {
    synchronized (this) {
      if (!this.operations.isEmpty()) {
        throw new IllegalArgumentException("Cannot change flush mode when writes are buffered");
      }
    }
    this.flushMode = flushMode;
  }

  /**
   * Set the number of operations that can be buffered.
   * @param size number of ops
   * @throws IllegalArgumentException if the buffer isn't empty
   */
  public void setMutationBufferSpace(int size) {
    synchronized (this) {
      if (!this.operations.isEmpty()) {
        throw new IllegalArgumentException("Cannot change the buffer" +
            " size when operations are buffered");
      }
    }
    this.mutationBufferSpace = size;
  }

  /**
   * Set the flush interval, which will be used for the next scheduling decision
   * @param interval interval in milliseconds
   */
  public void setFlushInterval(int interval) {
    this.interval = interval;
  }

  /**
   * Flushes the buffered operations
   * @return a Deferred if operations needed to be flushed, else null
   */
  public Deferred<Object> close() {
    timer.stop();
    return flush();
  }

  /**
   * Flushes the buffered operations
   * This method doesn't guarantee that all the apply()'d operations will be flushed since some
   * might still be trying to locate their tablet.
   * @return a Deferred whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  public Deferred<Object> flush() {
    final ArrayList<Deferred<Object>> d =
        new ArrayList<Deferred<Object>>(operations.size());
    Map<Slice, Batch> oldOperations = new HashMap<Slice, Batch>(operations.size());
    synchronized (this) {
      oldOperations.putAll(operations);
      operations.clear();
    }
    for (Batch inserts : oldOperations.values()) {
      d.add(flushTablet(inserts));
    }
    return (Deferred) Deferred.group(d);
  }

  /**
   * TODO
   * @return
   */
  public boolean hasPendingOperations() {
    return false; // TODO
  }

  /**
   * Apply the given operation.
   * The behavior of this function depends on the current flush mode. Regardless
   * of flush mode, however, Apply may begin to perform processing in the background
   * for the call (e.g looking up the tablet, etc).
   * @param operation operation to apply
   * @return a Deferred to track this operation
   */
  public Deferred<Object> apply(final Operation operation) {
    if (operation == null) {
      throw new NullPointerException("Cannot apply a null operation");
    }

    if (KuduClient.cannotRetryRequest(operation)) {
      return KuduClient.tooManyAttempts(operation, null);
    }

    // If we autoflush, just send it to the TS
    if (flushMode == FlushMode.AUTO_FLUSH_SYNC) {
      return client.sendRpcToTablet(operation);
    }
    String table = operation.getTable().getName();
    byte[] rowkey = operation.key();
    KuduClient.RemoteTablet tablet = client.getTablet(table, rowkey);
    // We go straight to the buffer if we know the tabletSlice
    if (tablet != null) {
      operation.setTablet(tablet);
      // Handles the difference between manual and auto flush
      Deferred<Object> d = addToBuffer(tablet.getTabletId(), operation);
      return d;
    }

    // TODO starts looking a lot like sendRpcToTablet
    operation.attempt++;
    if (client.isTableNotServed(table)) {
      return client.delayedIsCreateTableDone(table, operation, getRetryRpcCB(operation));
    }

    return client.locateTablet(table, rowkey).addBothDeferring(getRetryRpcCB(operation));
  }

  Callback<Deferred<Object>, Object> getRetryRpcCB(final Operation operation) {
    final class RetryRpcCB implements Callback<Deferred<Object>, Object> {
      public Deferred<Object> call(final Object arg) {
        Deferred<Object> d = client.handleLookupExceptions(operation, arg);
        return d == null ? apply(operation):  // Retry the RPC.
                           d; // something went wrong
      }
      public String toString() {
        return "retry RPC";
      }
    }
    return new RetryRpcCB();
  }

  /**
   * For manual and background flushing, this will batch the given operation
   * with the others, if any, for the specified tablet.
   * @param tablet tablet used to for batching
   * @param operation operation to batch
   * @return Deffered to track the operation
   */
  private Deferred<Object> addToBuffer(Slice tablet, Operation operation) {
    Batch batchToSend = null;
    boolean scheduleFlush = false;

    // Fat lock but none of the operations will block
    synchronized (this) {
      Batch batch = operations.get(tablet);

      // doing + 1 to see if the current insert would push us over the limit
      // TODO obviously wrong, not same size thing
      if (batch != null && (batch.ops.size() + 1) > mutationBufferSpace) {
        if (flushMode == FlushMode.MANUAL_FLUSH) {
          // TODO copy message from C++
          throw new NonRecoverableException("MANUAL_FLUSH is enabled but the buffer is too big");
        }
        batchToSend = batch;
        batch = null; // Set it to null so that a new one is put in operations
        operations.remove(tablet);
      }
      if (batch == null) {
        // We found a tablet that needs batching, this is the only place where
        // we schedule a flush
        batch = new Batch(operation.getTable());
        operations.put(tablet, batch);
        addBatchCallbacks(batch);
        // If we forced flush, it means we already have an outstanding flush scheduled
        // If we're unlucky it could be the very next thing that happens and we'd send
        // only 1 row.
        scheduleFlush = batchToSend == null;
      }
      batch.ops.add(operation);
    }
    if (flushMode == FlushMode.AUTO_FLUSH_BACKGROUND && scheduleFlush) {
      // Accumulated a first insert but we're not in manual mode,
      // schedule the flush
      LOG.debug("Scheduling a flush");
      scheduleNextPeriodicFlush(tablet);
    } else if (batchToSend != null) {
      // Buffer space is too big, send to the tablet
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to force a flush since " + batchToSend.ops.size());
      }
      client.sendRpcToTablet(batchToSend);
    }
    // Get here if we accumulated an insert, regardless of if it scheduled
    // a flush
    return operation.getDeferred();
  }

  /**
   * Creates callbacks to handle a multi-put and adds them to the request.
   * @param request The request for which we must handle the response.
   */
  private void addBatchCallbacks(final Batch request) {
    final class BatchCallback implements Callback<Object, Object> {
      // TODO add more handling
      public Object call(final Object response) {
        //LOG.info("Got a InsertsBatch response for " + request.operations.size() + " rows");
        if (!(response instanceof Tserver.WriteResponsePB)) {
          throw new InvalidResponseException(Tserver.WriteResponsePB.class, response);
        }
        Tserver.WriteResponsePB resp = (Tserver.WriteResponsePB) response;
        if (resp.getError().hasCode()) {
          // TODO more specific error parsing, same as KuduScanner
          LOG.error(resp.getError().getStatus().getMessage());
          throw new NonRecoverableException(resp.getError().getStatus().getMessage());
        }
        // the row errors come in a list that we assume is in the same order as we sent them
        // TODO verify
        // we increment this index every time we send an error
        int errorsIndex = 0;
        for (int i = 0; i < request.ops.size(); i++) {
          Object callbackObj = null;
          if (errorsIndex + 1 < resp.getPerRowErrorsCount() &&
              resp.getPerRowErrors(errorsIndex).getRowIndex() == i) {
            errorsIndex++;
            callbackObj = resp.getPerRowErrors(errorsIndex);
          }
          request.ops.get(i).callback(callbackObj);
        }
        return null;
      }

      public String toString() {
        return "Inserts response";
      }
    };
    request.getDeferred().addBoth(new BatchCallback());
  }

  /**
   * TODO
   * @param priority
   */
  public void setPriority(int priority) {
    // TODO
  }

  /**
   * Schedules the next periodic flush of buffered edits.
   */
  private void scheduleNextPeriodicFlush(Slice tablet) {
   /* if (interval > 0) { TODO ?
      // Since we often connect to many regions at the same time, we should
      // try to stagger the flushes to avoid flushing too many different
      // RegionClient concurrently.
      // To this end, we "randomly" adjust the time interval using the
      // system's time.  nanoTime uses the machine's most precise clock, but
      // often nanoseconds (the lowest bits) aren't available.  Most modern
      // machines will return microseconds so we can cheaply extract some
      // random adjustment from that.
      short adj = (short) (System.nanoTime() & 0xF0);
      if (interval < 3 * adj) {  // Is `adj' too large compared to `interval'?
        adj >>>= 2;  // Reduce the adjustment to not be too far off `interval'.
      }
      if ((adj & 0x10) == 0x10) {  // if some arbitrary bit is set...
        if (adj < interval) {
          adj = (short) -adj;      // ... use a negative adjustment instead.
        } else {
          adj = (short) (interval / -2);
        }
      }*/
    timer.newTimeout(new FlusherTask(tablet), interval, MILLISECONDS);
  }

  /**
   * Flushes the edits for the given tablet
   */
  private Deferred<Object> flushTablet(Batch inserts) {
    if (inserts != null && inserts.ops.size() != 0) {
      //final Deferred<Object> d = operations.getDeferred();
      LOG.debug("Sending " + inserts.ops.size());
      return client.sendRpcToTablet(inserts);
    }
    return null;
  }

  /**
   * A FlusherTask is created for each scheduled flush per tabletSlice.
   */
  class FlusherTask implements TimerTask {
    final Slice tabletSlice;

    FlusherTask(Slice tabletSlice) {
      this.tabletSlice = tabletSlice;
    }

    public void run(final Timeout timeout) {
      LOG.debug("Flushing " + this.tabletSlice.toString(Charset.defaultCharset()));
      // Copy the batch to a local variable and null it out.
      final Batch inserts;
      synchronized (KuduSession.this) {
        inserts = operations.remove(tabletSlice);
      }
      flushTablet(inserts);
    }
    public String toString() {
      return "flush commits of session " + KuduSession.this +
          " for tabletSlice " + this.tabletSlice.toString(Charset.defaultCharset());
    }
  };
}
