// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.WireProtocol;
import kudu.tserver.Tserver;
import kudu.util.Slice;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static kudu.rpc.ExternalConsistencyMode.NO_CONSISTENCY;

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
 *
 * Timeouts are handled differently depending on the flush mode.
 * With AUTO_FLUSH_SYNC, the timeout is set on each apply()'d operation.
 * With AUTO_FLUSH_BACKGROUND and MANUAL_FLUSH, the timeout is assigned to a whole batch of
 * operations upon flush()'ing. It means that in a situation with a timeout of 500ms and a flush
 * interval of 1000ms, an operation can be oustanding for up to 1500ms before being timed out.
 */
public class KuduSession implements SessionConfiguration {

  public static final Logger LOG = LoggerFactory.getLogger(KuduSession.class);
  private static final Range<Float> PERCENTAGE_RANGE = Ranges.closed(0.0f, 1.0f);

  private final KuduClient client;
  private final Random randomizer = new Random();
  private int interval = 1000;
  private int mutationBufferSpace = 1000;
  private int mutationBufferLowWatermark;
  private FlushMode flushMode;
  private ExternalConsistencyMode consistencyMode;
  private long currentTimeout = 0;

  /**
   * The following two maps are to be handled together when batching is enabled. The first one is
   * where the batching happens, the second one is where we keep track of what's been sent but
   * hasn't come back yet. A batch cannot be in both maps at the same time. A batch cannot be
   * added to the in flight map if there's already another batch for the same tablet. If this
   * happens, and the batch in the first map is full, then we fail fast and send it back to the
   * client.
   * The second map stores Deferreds because KuduRpc.callback clears out the Deferred it contains
   * (as a way to reset the RPC), so we want to store the Deferred that's with the RPC that's
   * sent out.
   */
  @GuardedBy("this")
  private final Map<Slice, Batch> operations = new HashMap<Slice, Batch>();

  @GuardedBy("this")
  private final Map<Slice, Deferred<Object>> operationsInFlight = new HashMap<Slice,
      Deferred<Object>>();

  /**
   * This List is used when not in AUTO_FLUSH_SYNC mode in order to keep track of the operations
   * that are looking up their tablet, meaning that they aren't in any of the maps above. This is
   * not expected to grow a lot except when a client starts and only for a short amount of time.
   *
   * The locking is somewhat tricky since when calling flush() we need to be able to grab
   * operations that already have found their tablets plus those that are going to start batching.
   */
  @GuardedBy("this")
  private final List<Operation> operationsInLookup = new ArrayList<Operation>();

  /**
   * Tracks whether the session has been closed.
   */
  volatile boolean closed;

  /**
   * Package-private constructor meant to be used via KuduClient
   * @param client client that creates this session
   */
  KuduSession(KuduClient client) {
    this.client = client;
    this.flushMode = FlushMode.AUTO_FLUSH_SYNC;
    this.consistencyMode = NO_CONSISTENCY;
    setMutationBufferLowWatermark(0.5f);
  }

  @Override
  public FlushMode getFlushMode() {
    return this.flushMode;
  }

  @Override
  public void setFlushMode(FlushMode flushMode) {
    if (hasPendingOperations()) {
      throw new IllegalArgumentException("Cannot change flush mode when writes are buffered");
    }
    this.flushMode = flushMode;
  }

  @Override
  public void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode) {
    if (hasPendingOperations()) {
      throw new IllegalArgumentException("Cannot change consistency mode "
          + "when writes are buffered");
    }
    this.consistencyMode = consistencyMode;
  }

  @Override
  public void setMutationBufferSpace(int size) {
    if (hasPendingOperations()) {
      throw new IllegalArgumentException("Cannot change the buffer" +
          " size when operations are buffered");
    }
    this.mutationBufferSpace = size;
  }

  @Override
  public void setMutationBufferLowWatermark(float mutationBufferLowWatermark) {
    if (hasPendingOperations()) {
      throw new IllegalArgumentException("Cannot change the buffer" +
          " low watermark when operations are buffered");
    } else if (!PERCENTAGE_RANGE.contains(mutationBufferLowWatermark)) {
      throw new IllegalArgumentException("The low watermark must be between 0 and 1 inclusively");
    }
    this.mutationBufferLowWatermark = (int)(mutationBufferLowWatermark * mutationBufferSpace);
  }

  /**
   * Lets us set a specific seed for tests
   * @param seed
   */
  @VisibleForTesting
  void setRandomSeed(long seed) {
    this.randomizer.setSeed(seed);
  }

  @Override
  public void setFlushInterval(int interval) {
    this.interval = interval;
  }

  @Override
  public void setTimeoutMillis(long timeout) {
    this.currentTimeout = timeout;
  }

  @Override
  public long getTimeoutMillis() {
    return this.currentTimeout;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * Flushes the buffered operations
   * @return a Deferred if operations needed to be flushed, else null
   */
  public Deferred<Object> close() {
    closed = true;
    client.removeSession(this);
    return flush();
  }

  /**
   * Flushes the buffered operations
   * @return a Deferred whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   */
  public Deferred<Object> flush() {
    LOG.trace("Flushing all tablets");
    final ArrayList<Deferred<Object>> d =
        new ArrayList<Deferred<Object>>(operations.size());
    // Make a copy of the map since we're going to remove objects within the iteration
    HashMap<Slice, Batch> copyOfOps;
    synchronized (this) {
      copyOfOps = new HashMap<Slice, Batch>(operations);
      List<Operation> copyOfOpsInLookup = new ArrayList<Operation>(operationsInLookup);
      operationsInLookup.clear();
      for (Operation op : copyOfOpsInLookup) {
        // Safe to do this here because we removed it from operationsInLookup. Lookup may not be
        // over, but it will be finished down in KuduClient. It means we may be missing a few
        // batching opportunities.
        d.add(client.sendRpcToTablet(op));
      }
    }
    for (Map.Entry<Slice, Batch> entry: copyOfOps.entrySet()) {
      d.add(flushTablet(entry.getKey(), entry.getValue()));
    }
    return (Deferred) Deferred.group(d);
  }

  @Override
  public boolean hasPendingOperations() {
    synchronized (this) {
      return !this.operations.isEmpty() || !this.operationsInFlight.isEmpty() ||
          !this.operationsInLookup.isEmpty();
    }
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
      return KuduClient.tooManyAttemptsOrTimeout(operation, null);
    }

    // If we autoflush, just send it to the TS
    if (flushMode == FlushMode.AUTO_FLUSH_SYNC) {
      if (currentTimeout != 0) {
        operation.setTimeoutMillis(currentTimeout);
      }
      operation.setExternalConsistencyMode(this.consistencyMode);
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

    synchronized (this) {
      operationsInLookup.add(operation);
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
        // Important that we lock down everything here so that when we're done at the end of this
        // method either we put back the operation into operationsInLookup down in apply(),
        // or we succesfully batch it and then flush() will see it in the operations map.
        synchronized (KuduSession.this) {
          boolean removed = operationsInLookup.remove(operation);
          if (!removed) {
            LOG.trace("Won't continue applying this operation as it should be handled by a call " +
                "to flush(): " + operation);
            return Deferred.fromResult(null);
          }

          Deferred<Object> d = client.handleLookupExceptions(operation, arg);
          if (d == null) {
            try {
              return apply(operation); // Retry the RPC.
            } catch(PleaseThrottleException pte) {
              // We're not really "done" doing the lookup since we couldn't apply. When we come
              // back we'll call handleLookupExceptions again but it will be a no-op.
              operationsInLookup.add(operation);
              return pte.getDeferred().addBothDeferring(getRetryRpcCB(operation));
            }
          } else {
            return d; // something went wrong
          }
        }
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
    boolean scheduleFlush = false;

    // Fat lock but none of the operations will block
    synchronized (this) {
      Batch batch = operations.get(tablet);

      // doing + 1 to see if the current insert would push us over the limit
      // TODO obviously wrong, not same size thing
      if (batch != null) {
        if (batch.ops.size() + 1 > mutationBufferSpace) {
          if (flushMode == FlushMode.MANUAL_FLUSH) {
            throw new NonRecoverableException("MANUAL_FLUSH is enabled but the buffer is too big");
          }
          flushTablet(tablet, batch);
          if (operations.containsKey(tablet)) {
            // This means we didn't flush because there was another batch in flight.
            // We cannot continue here, we have to send this back to the client.
            // This is our high watermark.
            throw new PleaseThrottleException("The RPC cannot be buffered because the current " +
                "buffer is full and the previous buffer hasn't been flushed yet", null,
                operation, operationsInFlight.get(tablet));
          }
          // Below will take care of creating the new batch and adding the operation.
          batch = null;
        } else if (operationsInFlight.containsKey(tablet) &&
            batch.ops.size() + 1 > mutationBufferLowWatermark) {
          // This is our low watermark, we throw PleaseThrottleException before hitting the high
          // mark. As we get fuller past the watermark it becomes likelier to trigger it.
          int randomWatermark = batch.ops.size() + 1 + randomizer.nextInt(mutationBufferSpace -
              mutationBufferLowWatermark);
          if (randomWatermark > mutationBufferSpace) {
            throw new PleaseThrottleException("The previous buffer hasn't been flushed and the " +
                "current one is over the low watermark, please retry later", null, operation,
                operationsInFlight.get(tablet));
          }
        }
      }
      if (batch == null) {
        // We found a tablet that needs batching, this is the only place where
        // we schedule a flush
        batch = new Batch(operation.getTable());
        batch.setExternalConsistencyMode(this.consistencyMode);
        Batch oldBatch = operations.put(tablet, batch);
        assert (oldBatch == null);
        addBatchCallbacks(batch);
        scheduleFlush = true;
      }
      batch.ops.add(operation);
      if (flushMode == FlushMode.AUTO_FLUSH_BACKGROUND && scheduleFlush) {
        // Accumulated a first insert but we're not in manual mode,
        // schedule the flush
        LOG.trace("Scheduling a flush");
        scheduleNextPeriodicFlush(tablet, batch);
      }
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
        LOG.trace("Got a InsertsBatch response for " + request.ops.size() + " rows");
        if (!(response instanceof Tserver.WriteResponsePB)) {
          throw new InvalidResponseException(Tserver.WriteResponsePB.class, response);
        }
        Tserver.WriteResponsePB resp = (Tserver.WriteResponsePB) response;
        if (resp.getError().hasCode()) {
          // TODO more specific error parsing, same as KuduScanner
          if (resp.getError().getStatus().getCode().equals(WireProtocol.AppStatusPB.ErrorCode
              .SERVICE_UNAVAILABLE)) {
            client.handleRetryableError(request,
                new TabletServerErrorException(resp.getError().getStatus()));
            return null;
          }
          LOG.error(resp.getError().getStatus().getMessage());
          throw new NonRecoverableException(resp.getError().getStatus().getMessage());
        }
        if (resp.hasWriteTimestamp()) {
          KuduSession.this.client.updateLastPropagatedTimestamp(resp.getWriteTimestamp());
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
        return resp;
      }

      public String toString() {
        return "Inserts response";
      }
    };
    request.getDeferred().addBoth(new BatchCallback());
  }

  /**
   * Schedules the next periodic flush of buffered edits.
   */
  private void scheduleNextPeriodicFlush(Slice tablet, Batch batch) {
    client.newTimeout(new FlusherTask(tablet, batch), interval);
  }

  /**
   * Flushes the edits for the given tablet. It will also check that the Batch we're flushing is
   * the one that was requested. This is mostly done so that the FlusherTask doesn't trigger
   * lots of small flushes under a write-heavy scenario where we're able to fill a Batch multiple
   * times per interval.
   *
   * Also, if there's already a Batch in flight for the given tablet,
   * the flush will be delayed and the returned Deferred will be chained to it.
   */
  private Deferred<Object> flushTablet(Slice tablet, Batch expectedBatch) {
    assert (expectedBatch != null);

    synchronized (this) {
      // Check this first, no need to wait after anyone if the batch we were supposed to flush
      // was already flushed.
      if (operations.get(tablet) != expectedBatch) {
        LOG.trace("Had to flush a tablet but it was already flushed: " + Bytes.getString(tablet));
        return Deferred.fromResult(null);
      }

      if (operationsInFlight.containsKey(tablet)) {
        LOG.trace("This tablet is already in flight, attaching a callback to retry later: " +
            Bytes.getString(tablet));
        return operationsInFlight.get(tablet).addBothDeferring(
            new FlushRetryCallback(tablet, operations.get(tablet)));
      }

      Batch batch = operations.remove(tablet);
      if (batch == null) {
        LOG.trace("Had to flush a tablet but there was nothing to flush: " +
            Bytes.getString(tablet));
        return Deferred.fromResult(null);
      }
      Deferred<Object> batchDeferred = batch.getDeferred();
      batchDeferred.addBoth(new OpInFlightCallback(tablet));
      Deferred<Object> oldBatch = operationsInFlight.put(tablet, batchDeferred);
      assert (oldBatch == null);
      if (currentTimeout != 0) {
        batch.deadlineTracker.reset();
        batch.setTimeoutMillis(currentTimeout);
      }
      return client.sendRpcToTablet(batch);
    }
  }


  /**
   * Simple callback so that we try to flush this tablet again if we were waiting on the previous
   * Batch to finish.
   */
  class FlushRetryCallback implements Callback<Deferred<Object>, Object> {
    private final Slice tablet;
    private final Batch expectedBatch;
    public FlushRetryCallback(Slice tablet, Batch expectedBatch) {
      this.tablet = tablet;
      this.expectedBatch = expectedBatch;
    }

    @Override
    public Deferred<Object> call(Object o) throws Exception {
      if (o instanceof Exception) {
        LOG.warn("Encountered exception sending a batch to " + Bytes.getString(tablet),
            (Exception) o);
      }
      synchronized (KuduSession.this) {
        LOG.trace("Previous batch in flight is done, flushing this tablet again: " +
            Bytes.getString(tablet));
        return flushTablet(tablet, expectedBatch);
      }
    }
  }

  /**
   * Simple callback that removes the tablet from the in flight operations map once it completed.
   * TODO we're not handling errors.
   */
  class OpInFlightCallback implements Callback<Object, Object> {
    private final Slice tablet;
    public OpInFlightCallback(Slice tablet) {
      this.tablet = tablet;
    }

    @Override
    public Object call(Object o) throws Exception {
      if (o instanceof Exception) {
        LOG.warn("Encountered exception sending a batch to " + Bytes.getString(tablet),
            (Exception) o);
      }
      synchronized (KuduSession.this) {
        LOG.trace("Unmarking this tablet as in flight: " + Bytes.getString(tablet));
        operationsInFlight.remove(tablet);
      }
      return o;
    }
  }

  /**
   * A FlusherTask is created for each scheduled flush per tabletSlice.
   */
  class FlusherTask implements TimerTask {
    final Slice tabletSlice;
    final Batch expectedBatch;

    FlusherTask(Slice tabletSlice, Batch expectedBatch) {
      this.tabletSlice = tabletSlice;
      this.expectedBatch = expectedBatch;
    }

    public void run(final Timeout timeout) {
      if (isClosed()) {
        return; // we ran too late, no-op
      }
      LOG.trace("Timed flushing: " + Bytes.getString(tabletSlice));
      flushTablet(this.tabletSlice, this.expectedBatch);
    }
    public String toString() {
      return "flush commits of session " + KuduSession.this +
          " for tabletSlice " + Bytes.getString(tabletSlice);
    }
  };
}
