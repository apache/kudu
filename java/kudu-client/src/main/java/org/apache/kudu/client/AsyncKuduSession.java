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

import static org.apache.kudu.client.ExternalConsistencyMode.CLIENT_PROPAGATED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.kudu.client.AsyncKuduClient.LookupType;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.util.AsyncUtil;
import org.apache.kudu.util.Slice;

/**
 * An {@code AsyncKuduSession} belongs to a specific {@link AsyncKuduClient}, and represents a
 * context in which all write data access should take place. Within a session,
 * multiple operations may be accumulated and batched together for better
 * efficiency. Settings like timeouts, priorities, and trace IDs are also set
 * per session.
 *
 * <p>{@code AsyncKuduSession} is separate from {@link AsyncKuduClient} because, in a multi-threaded
 * application, different threads may need to concurrently execute
 * transactions. Similar to a JDBC "session", transaction boundaries will be
 * delineated on a per-session basis -- in between a "BeginTransaction" and
 * "Commit" call on a given session, all operations will be part of the same
 * transaction. Meanwhile another concurrent session object can safely run
 * non-transactional work or other transactions without interfering.
 *
 * <p>Therefore, this class is <b>not</b> thread-safe.
 *
 * <p>Additionally, there is a guarantee that writes from different sessions do not
 * get batched together into the same RPCs -- this means that latency-sensitive
 * clients can run through the same {@link AsyncKuduClient} object as throughput-oriented
 * clients, perhaps by setting the latency-sensitive session's timeouts low and
 * priorities high. Without the separation of batches, a latency-sensitive
 * single-row insert might get batched along with 10MB worth of inserts from the
 * batch writer, thus delaying the response significantly.
 *
 * <p>Timeouts are handled differently depending on the flush mode.
 * With {@link SessionConfiguration.FlushMode#AUTO_FLUSH_SYNC AUTO_FLUSH_SYNC}, the timeout is set
 * on each {@linkplain #apply apply}()'d operation.
 * With {@link SessionConfiguration.FlushMode#AUTO_FLUSH_BACKGROUND AUTO_FLUSH_BACKGROUND} and
 * {@link SessionConfiguration.FlushMode#MANUAL_FLUSH MANUAL_FLUSH}, the timeout is assigned to a
 * whole batch of operations upon {@linkplain #flush flush}()'ing. It means that in a situation
 * with a timeout of 500ms and a flush interval of 1000ms, an operation can be outstanding for up to
 * 1500ms before being timed out.
 *
 * <p><strong>Warning: a note on out-of-order operations</strong>
 *
 * <p>When using {@code AsyncKuduSession}, it is not difficult to trigger concurrent flushes on
 * the same session. The result is that operations applied in a particular order within a single
 * session may be applied in a different order on the server side, even for a single tablet. To
 * prevent this behavior, ensure that only one flush is outstanding at a given time (the maximum
 * concurrent flushes per {@code AsyncKuduSession} is hard-coded to 2).
 *
 * <p>If operation interleaving would be unacceptable for your application, consider using one of
 * the following strategies to avoid it:
 *
 * <ol>
 * <li>When using {@link SessionConfiguration.FlushMode#MANUAL_FLUSH MANUAL_FLUSH} mode,
 * wait for one {@link #flush flush()} to {@code join()} before triggering another flush.
 * <li>When using {@link SessionConfiguration.FlushMode#AUTO_FLUSH_SYNC AUTO_FLUSH_SYNC}
 * mode, wait for each {@link #apply apply()} to {@code join()} before applying another operation.
 * <li>Consider not using
 * {@link SessionConfiguration.FlushMode#AUTO_FLUSH_BACKGROUND AUTO_FLUSH_BACKGROUND} mode.
 * <li>Make your application resilient to out-of-order application of writes.
 * <li>Avoid applying an {@link Operation} on a particular row until any previous write to that
 * row has been successfully flushed.
 * </ol>
 *
 * <p>For more information on per-session operation interleaving, see
 * <a href="https://issues.apache.org/jira/browse/KUDU-1767">KUDU-1767</a>.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public class AsyncKuduSession implements SessionConfiguration {

  public static final Logger LOG = LoggerFactory.getLogger(AsyncKuduSession.class);

  private final AsyncKuduClient client;
  private final Random randomizer = new Random();
  private final ErrorCollector errorCollector;
  private int flushIntervalMillis = 1000;
  private int mutationBufferMaxOps = 1000; // TODO express this in terms of data size.
  private FlushMode flushMode;
  private ExternalConsistencyMode consistencyMode;
  private long timeoutMillis;

  /**
   * Protects internal state from concurrent access. {@code AsyncKuduSession} is not threadsafe
   * from the application's perspective, but because internally async timers and async flushing
   * tasks may access the session concurrently with the application, synchronization is still
   * needed.
   */
  private final Object monitor = new Object();

  /**
   * Tracks the currently active buffer.
   *
   * When in mode {@link FlushMode#AUTO_FLUSH_BACKGROUND} or {@link FlushMode#AUTO_FLUSH_SYNC},
   * {@code AsyncKuduSession} uses double buffering to improve write throughput. While the
   * application is {@link #apply}ing operations to one buffer (the {@code activeBuffer}), the
   * second buffer is either being flushed, or if it has already been flushed, it waits in the
   * {@link #inactiveBuffers} queue. When the currently active buffer is flushed,
   * {@code activeBuffer} is set to {@code null}. On the next call to {@code apply}, an inactive
   * buffer is taken from {@code inactiveBuffers} and made the new active buffer. If both
   * buffers are still flushing, then the {@code apply} call throws {@link PleaseThrottleException}.
   */
  @GuardedBy("monitor")
  private Buffer activeBuffer;

  /**
   * The buffers. May either be active (pointed to by {@link #activeBuffer},
   * inactive (in the {@link #inactiveBuffers}) queue, or flushing.
   */
  private final Buffer bufferA = new Buffer();
  private final Buffer bufferB = new Buffer();

  /**
   * Queue containing flushed, inactive buffers. May be accessed from callbacks (I/O threads).
   * We restrict the session to only two buffers, so {@link BlockingQueue#add} can
   * be used without chance of failure.
   */
  private final BlockingQueue<Buffer> inactiveBuffers = new ArrayBlockingQueue<>(2, false);

  /**
   * Deferred used to notify on flush events. Atomically swapped and completed every time a buffer
   * is flushed. This can be used to notify handlers of {@link PleaseThrottleException} that more
   * capacity may be available in the active buffer.
   */
  private final AtomicReference<Deferred<Void>> flushNotification =
      new AtomicReference<>(new Deferred<Void>());

  /**
   * Tracks whether the session has been closed.
   */
  private volatile boolean closed = false;

  private boolean ignoreAllDuplicateRows = false;
  private boolean ignoreAllNotFoundRows = false;

  /**
   * Package-private constructor meant to be used via AsyncKuduClient
   * @param client client that creates this session
   */
  AsyncKuduSession(AsyncKuduClient client) {
    this.client = client;
    flushMode = FlushMode.AUTO_FLUSH_SYNC;
    consistencyMode = CLIENT_PROPAGATED;
    timeoutMillis = client.getDefaultOperationTimeoutMs();
    inactiveBuffers.add(bufferA);
    inactiveBuffers.add(bufferB);
    errorCollector = new ErrorCollector(mutationBufferMaxOps);
  }

  @Override
  public FlushMode getFlushMode() {
    return this.flushMode;
  }

  // TODO(wdberkeley): KUDU-1944. Don't let applications change the flush mode. Use a new session.
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
      throw new IllegalArgumentException("Cannot change consistency mode " +
          "when writes are buffered");
    }
    this.consistencyMode = consistencyMode;
  }

  @Override
  public void setMutationBufferSpace(int numOps) {
    if (hasPendingOperations()) {
      throw new IllegalArgumentException("Cannot change the buffer" +
          " size when operations are buffered");
    }
    this.mutationBufferMaxOps = numOps;
  }

  @Deprecated
  @Override
  public void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage) {
    LOG.warn("setMutationBufferLowWatermark is deprecated");
  }

  /**
   * Lets us set a specific seed for tests
   * @param seed
   */
  @InterfaceAudience.LimitedPrivate("Test")
  void setRandomSeed(long seed) {
    this.randomizer.setSeed(seed);
  }

  @Override
  public void setFlushInterval(int flushIntervalMillis) {
    this.flushIntervalMillis = flushIntervalMillis;
  }

  @Override
  public void setTimeoutMillis(long timeout) {
    this.timeoutMillis = timeout;
  }

  @Override
  public long getTimeoutMillis() {
    return this.timeoutMillis;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public boolean isIgnoreAllDuplicateRows() {
    return ignoreAllDuplicateRows;
  }

  @Override
  public void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows) {
    this.ignoreAllDuplicateRows = ignoreAllDuplicateRows;
  }

  @Override
  public boolean isIgnoreAllNotFoundRows() {
    return ignoreAllNotFoundRows;
  }

  @Override
  public void setIgnoreAllNotFoundRows(boolean ignoreAllNotFoundRows) {
    this.ignoreAllNotFoundRows = ignoreAllNotFoundRows;
  }

  @Override
  public int countPendingErrors() {
    return errorCollector.countErrors();
  }

  @Override
  public RowErrorsAndOverflowStatus getPendingErrors() {
    return errorCollector.getErrors();
  }

  /**
   * Flushes the buffered operations and marks this session as closed.
   * See the javadoc on {@link #flush()} on how to deal with exceptions coming out of this method.
   * @return a Deferred whose callback chain will be invoked when.
   * everything that was buffered at the time of the call has been flushed.
   */
  public Deferred<List<OperationResponse>> close() {
    if (!closed) {
      closed = true;
      client.removeSession(this);
    }
    return flush();
  }

  /**
   * Returns a buffer to the inactive queue after flushing.
   * @param buffer the buffer to return to the inactive queue.
   */
  private void queueBuffer(Buffer buffer) {
    buffer.callbackFlushNotification();
    Deferred<Void> localFlushNotification = flushNotification.getAndSet(new Deferred<Void>());
    inactiveBuffers.add(buffer);
    localFlushNotification.callback(null);
  }

  /**
   * Callback which waits for all tablet location lookups to complete, groups all operations into
   * batches by tablet, and dispatches them. When all of the batches are complete, a deferred is
   * fired and the buffer is added to the inactive queue.
   */
  private final class TabletLookupCB implements Callback<Void, Object> {
    private final AtomicInteger lookupsOutstanding;
    private final Buffer buffer;
    private final Deferred<List<BatchResponse>> deferred;

    public TabletLookupCB(Buffer buffer, Deferred<List<BatchResponse>> deferred) {
      this.lookupsOutstanding = new AtomicInteger(buffer.getOperations().size());
      this.buffer = buffer;
      this.deferred = deferred;
    }

    @Override
    public Void call(Object unused) throws Exception {
      if (lookupsOutstanding.decrementAndGet() != 0) {
        return null;
      }

      // The final tablet lookup is complete. Batch all of the buffered
      // operations into their respective tablet, and then send the batches.

      // Group the operations by tablet.
      Map<Slice, Batch> batches = new HashMap<>();
      List<OperationResponse> opsFailedInLookup = new ArrayList<>();
      List<Integer> opsFailedIndexesList = new ArrayList<>();

      int currentIndex = 0;
      for (BufferedOperation bufferedOp : buffer.getOperations()) {
        Operation operation = bufferedOp.getOperation();
        if (bufferedOp.tabletLookupFailed()) {
          Exception failure = bufferedOp.getTabletLookupFailure();
          RowError error;
          if (failure instanceof NonCoveredRangeException) {
            // TODO: this should be something different than NotFound so that
            // applications can distinguish from updates on missing rows.
            error = new RowError(Status.NotFound(failure.getMessage()), operation);
          } else {
            LOG.warn("unexpected tablet lookup failure for operation {}", operation, failure);
            error = new RowError(Status.RuntimeError(failure.getMessage()), operation);
          }
          OperationResponse response = new OperationResponse(0, null, 0, operation, error);
          // Add the row error to the error collector if the session is in background flush mode,
          // and complete the operation's deferred with the error response. The ordering between
          // adding to the error collector and completing the deferred should not matter since
          // applications should be using one or the other method for error handling, not both.
          if (flushMode == FlushMode.AUTO_FLUSH_BACKGROUND) {
            errorCollector.addError(error);
          }
          operation.callback(response);
          opsFailedInLookup.add(response);
          opsFailedIndexesList.add(currentIndex++);
          continue;
        }
        LocatedTablet tablet = bufferedOp.getTablet();
        Slice tabletId = new Slice(tablet.getTabletId());

        Batch batch = batches.get(tabletId);
        if (batch == null) {
          batch = new Batch(operation.getTable(), tablet, ignoreAllDuplicateRows,
              ignoreAllNotFoundRows);
          batches.put(tabletId, batch);
        }
        batch.add(operation, currentIndex++);
      }

      List<Deferred<BatchResponse>> batchResponses = new ArrayList<>(batches.size() + 1);
      if (!opsFailedInLookup.isEmpty()) {
        batchResponses.add(
            Deferred.fromResult(new BatchResponse(opsFailedInLookup, opsFailedIndexesList)));
      }

      for (Batch batch : batches.values()) {
        if (timeoutMillis != 0) {
          batch.resetTimeoutMillis(client.getTimer(), timeoutMillis);
        }
        addBatchCallbacks(batch);
        batchResponses.add(client.sendRpcToTablet(batch));
      }

      // On completion of all batches, fire the completion deferred, and add the buffer
      // back to the inactive buffers queue. This frees it up for new inserts.
      AsyncUtil.addBoth(
          Deferred.group(batchResponses),
          new Callback<Void, Object>() {
            @Override
            public Void call(Object responses) {
              queueBuffer(buffer);
              deferred.callback(responses);
              return null;
            }
          });

      return null;
    }
  }

  /**
   * Flush buffered writes.
   * @return a {@link Deferred} whose callback chain will be invoked when all applied operations at
   *         the time of the call have been flushed.
   */
  public Deferred<List<OperationResponse>> flush() {
    Buffer buffer;
    Deferred<Void> nonActiveBufferFlush;
    synchronized (monitor) {
      nonActiveBufferFlush = getNonActiveFlushNotificationUnlocked();
      buffer = retireActiveBufferUnlocked();
    }

    // TODO(wdb): If there is a buffer flushing already, this code will wait for it to finish before
    //            flushing 'buffer'. This is less performant but has less surprising semantics than
    //            simultaneously flushing two buffers. Even though we don't promise those semantics,
    //            I'm going to leave it this way for now because it's never caused any trouble.
    return AsyncUtil.addBothDeferring(nonActiveBufferFlush, _unused -> doFlush(buffer));
  }

  /**
   * Flushes a write buffer. This method takes ownership of 'buffer', no other concurrent access
   * is allowed. 'buffer' is allowed to be null.
   *
   * @param buffer the buffer to flush, must not be modified once passed to this method
   * @return the operation responses
   */
  private Deferred<List<OperationResponse>> doFlush(Buffer buffer) {
    if (buffer == null || buffer.getOperations().isEmpty()) {
      return Deferred.fromResult(ImmutableList.of());
    }
    LOG.debug("flushing buffer: {}", buffer);

    Deferred<List<BatchResponse>> batchResponses = new Deferred<>();
    Callback<Void, Object> tabletLookupCB = new TabletLookupCB(buffer, batchResponses);

    for (BufferedOperation bufferedOperation : buffer.getOperations()) {
      AsyncUtil.addBoth(bufferedOperation.getTabletLookup(), tabletLookupCB);
    }

    return batchResponses.addCallback(ConvertBatchToListOfResponsesCB.getInstance());
  }

  /**
   * Callback used to send a list of OperationResponse instead of BatchResponse since the
   * latter is an implementation detail.
   */
  private static class ConvertBatchToListOfResponsesCB implements Callback<List<OperationResponse>,
                                                                           List<BatchResponse>> {
    private static final ConvertBatchToListOfResponsesCB INSTANCE =
        new ConvertBatchToListOfResponsesCB();

    @Override
    public List<OperationResponse> call(List<BatchResponse> batchResponses) throws Exception {
      // First compute the size of the union of all the lists so that we don't trigger expensive
      // list growths while adding responses to it.
      int size = 0;
      for (BatchResponse batchResponse : batchResponses) {
        size += batchResponse.getIndividualResponses().size();
      }

      OperationResponse[] responses = new OperationResponse[size];
      for (BatchResponse batchResponse : batchResponses) {
        List<OperationResponse> responseList = batchResponse.getIndividualResponses();
        List<Integer> indexList = batchResponse.getResponseIndexes();
        for (int i = 0; i < indexList.size(); i++) {
          int index = indexList.get(i);
          assert responses[index] == null;
          responses[index] = responseList.get(i);
        }
      }

      return Arrays.asList(responses);
    }

    @Override
    public String toString() {
      return "ConvertBatchToListOfResponsesCB";
    }

    public static ConvertBatchToListOfResponsesCB getInstance() {
      return INSTANCE;
    }
  }

  @Override
  public boolean hasPendingOperations() {
    synchronized (monitor) {
      return activeBuffer == null ? inactiveBuffers.size() < 2 :
             activeBuffer.getOperations().size() > 0 || !inactiveBufferAvailable();
    }
  }

  // TODO(wdberkeley): Get rid of the idea of an Operation as a distinct way to do a write. Replace
  //                   it with a single-operation Batch.
  private Deferred<OperationResponse> doAutoFlushSync(final Operation operation) {
    if (timeoutMillis != 0) {
      operation.resetTimeoutMillis(client.getTimer(), timeoutMillis);
    }
    operation.setExternalConsistencyMode(consistencyMode);
    operation.setIgnoreAllDuplicateRows(ignoreAllDuplicateRows);
    operation.setIgnoreAllNotFoundRows(ignoreAllNotFoundRows);

    return client.sendRpcToTablet(operation)
        .addCallbackDeferring(resp -> {
          client.updateLastPropagatedTimestamp(resp.getWriteTimestampRaw());
          return Deferred.fromResult(resp);
        })
        .addErrback(new SingleOperationErrCallback(operation));
  }

  /**
   * Apply the given operation.
   * <p>
   * The behavior of this method depends on the configured
   * {@link SessionConfiguration.FlushMode FlushMode}. Regardless
   * of flush mode, however, {@code apply()} may begin to perform processing in the background
   * for the call (e.g looking up the tablet location, etc).
   * @param operation operation to apply
   * @return a Deferred to track this operation
   * @throws KuduException if an error happens or {@link PleaseThrottleException} is triggered
   * @see SessionConfiguration.FlushMode FlushMode
   */
  public Deferred<OperationResponse> apply(final Operation operation) throws KuduException {
    Preconditions.checkNotNull(operation, "Cannot apply a null operation");
    Preconditions.checkArgument(operation.getTable().getAsyncClient() == client,
        "Applied operations must be created from a KuduTable instance opened " +
        "from the same client that opened this KuduSession");
    if (closed) {
      // Ideally this would be a precondition, but that may break existing
      // clients who have grown to rely on this unsafe behavior.
      LOG.warn("Applying an operation in a closed session; this is unsafe");
    }

    // Freeze the row so that the client cannot concurrently modify it while it is in flight.
    operation.getRow().freeze();

    // If immediate flush mode, send the operation directly.
    if (flushMode == FlushMode.AUTO_FLUSH_SYNC) {
      return doAutoFlushSync(operation);
    }

    // Kick off a location lookup.
    Deferred<LocatedTablet> tablet = client.getTabletLocation(operation.getTable(),
                                                              operation.partitionKey(),
                                                              LookupType.POINT,
                                                              timeoutMillis);

    // Holds a buffer that should be flushed outside the synchronized block, if necessary.
    Buffer fullBuffer = null;
    try {
      synchronized (monitor) {
        Deferred<Void> notification = flushNotification.get();
        if (activeBuffer == null) {
          // If the active buffer is null then we recently flushed. Check if there
          // is an inactive buffer available to replace as the active.
          if (inactiveBufferAvailable()) {
            refreshActiveBufferUnlocked();
          } else {
            Status statusServiceUnavailable =
                Status.ServiceUnavailable("all buffers are currently flushing");
            // This can happen if the user writes into a buffer, flushes it, writes
            // into the second, flushes it, and immediately tries to write again.
            throw new PleaseThrottleException(statusServiceUnavailable,
                                              null, operation, notification);
          }
        }

        int activeBufferSize = activeBuffer.getOperations().size();
        switch (flushMode) {
          case AUTO_FLUSH_SYNC: {
            // This case is handled above and is impossible here.
            // TODO(wdberkeley): Handle AUTO_FLUSH_SYNC just like other flush modes.
            assert false;
            break;
          }
          case MANUAL_FLUSH: {
            if (activeBufferSize >= mutationBufferMaxOps) {
              Status statusIllegalState =
                  Status.IllegalState("MANUAL_FLUSH is enabled but the buffer is too big");
              throw new NonRecoverableException(statusIllegalState);
            }
            activeBuffer.getOperations().add(new BufferedOperation(tablet, operation));
            break;
          }
          case AUTO_FLUSH_BACKGROUND: {
            if (activeBufferSize >= mutationBufferMaxOps) {
              // If the active buffer is full or overflowing, be sure to kick off a flush.
              fullBuffer = retireActiveBufferUnlocked();
              activeBufferSize = 0;

              if (!inactiveBufferAvailable()) {
                Status statusServiceUnavailable =
                    Status.ServiceUnavailable("All buffers are currently flushing");
                throw new PleaseThrottleException(statusServiceUnavailable,
                    null, operation, notification);
              }
              refreshActiveBufferUnlocked();
            }

            // Add the operation to the active buffer, and:
            // 1. If it's the first operation in the buffer, start a background flush timer.
            // 2. If it filled or overflowed the buffer, kick off a flush.
            activeBuffer.getOperations().add(new BufferedOperation(tablet, operation));
            if (activeBufferSize == 0) {
              AsyncKuduClient.newTimeout(client.getTimer(), activeBuffer.getFlusherTask(), flushIntervalMillis);
            }
            if (activeBufferSize + 1 >= mutationBufferMaxOps && inactiveBufferAvailable()) {
              fullBuffer = retireActiveBufferUnlocked();
            }
            break;
          }
        }
      }
    } finally {
      // Flush the buffer outside of the synchronized block, if required.
      doFlush(fullBuffer);
    }
    return operation.getDeferred();
  }

  /**
   * Returns {@code true} if there is an inactive buffer available.
   * @return true if there is currently an inactive buffer available
   */
  private boolean inactiveBufferAvailable() {
    return inactiveBuffers.peek() != null;
  }

  /**
   * Refreshes the active buffer. This should only be called after a
   * {@link #flush()} when the active buffer is {@code null}, there is an
   * inactive buffer available (see {@link #inactiveBufferAvailable()}, and
   * {@link #monitor} is locked.
   */
  @GuardedBy("monitor")
  private void refreshActiveBufferUnlocked() {
    Preconditions.checkState(activeBuffer == null);
    activeBuffer = inactiveBuffers.remove();
    activeBuffer.resetUnlocked();
  }

  /**
   * Retires the active buffer and returns it. Returns null if there is no active buffer.
   * This should only be called if {@link #monitor} is locked.
   */
  @GuardedBy("monitor")
  private Buffer retireActiveBufferUnlocked() {
    Buffer buffer = activeBuffer;
    activeBuffer = null;
    return buffer;
  }

  /**
   * Returns a flush notification for the currently non-active buffers.
   * This is used during manual {@link #flush} calls to ensure that all buffers (not just the active
   * buffer) are fully flushed before completing.
   */
  @GuardedBy("monitor")
  private Deferred<Void> getNonActiveFlushNotificationUnlocked() {
    final Deferred<Void> notificationA = bufferA.getFlushNotification();
    final Deferred<Void> notificationB = bufferB.getFlushNotification();
    if (activeBuffer == null) {
      // Both buffers are either flushing or inactive.
      return AsyncUtil.addBothDeferring(notificationA, _unused -> notificationB);
    } else if (activeBuffer == bufferA) {
      return notificationB;
    } else {
      return notificationA;
    }
  }

  /**
   * Creates callbacks to handle a multi-put and adds them to the request.
   * @param request the request for which we must handle the response
   */
  private void addBatchCallbacks(final Batch request) {
    final class BatchCallback implements Callback<BatchResponse, BatchResponse> {
      @Override
      public BatchResponse call(final BatchResponse response) {
        LOG.trace("Got a Batch response for {} rows", request.operations.size());
        AsyncKuduSession.this.client.updateLastPropagatedTimestamp(response.getWriteTimestamp());

        // Send individualized responses to all the operations in this batch.
        for (OperationResponse operationResponse : response.getIndividualResponses()) {
          if (flushMode == FlushMode.AUTO_FLUSH_BACKGROUND && operationResponse.hasRowError()) {
            errorCollector.addError(operationResponse.getRowError());
          }

          // Fire the callback after collecting the errors so that the errors are visible should the
          // callback interrogate the error collector.
          operationResponse.getOperation().callback(operationResponse);
        }

        return response;
      }

      @Override
      public String toString() {
        return "apply batch response";
      }
    }

    final class BatchErrCallback implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) {
        // If the exception we receive is a KuduException we're going to build OperationResponses.
        Status status = null;
        List<OperationResponse> responses = null;
        boolean handleKuduException = e instanceof KuduException;
        if (handleKuduException) {
          status = ((KuduException) e).getStatus();
          responses = new ArrayList<>(request.operations.size());
        }

        for (Operation operation : request.operations) {
          // Same comment as in BatchCallback regarding the ordering of when to callback.
          if (handleKuduException) {
            RowError rowError = new RowError(status, operation);
            OperationResponse response = new OperationResponse(0, null, 0, operation, rowError);
            errorCollector.addError(rowError);
            responses.add(response);

            operation.callback(response);
          } else {
            // We have no idea what the exception is so we'll just send it up.
            operation.errback(e);
          }
        }

        // Note that returning an object that's not an exception will make us leave the
        // errback chain. Effectively, the BatchResponse below will end up as part of the list
        // passed to ConvertBatchToListOfResponsesCB.
        return handleKuduException ? new BatchResponse(responses, request.operationIndexes) : e;
      }

      @Override
      public String toString() {
        return "apply batch error response";
      }
    }

    request.getDeferred().addCallbacks(new BatchCallback(), new BatchErrCallback());
  }

  /**
   * Analogous to BatchErrCallback above but for AUTO_FLUSH_SYNC which doesn't handle lists of
   * operations and responses.
   */
  private static final class SingleOperationErrCallback implements Callback<Object, Exception> {

    private final Operation operation;

    private SingleOperationErrCallback(Operation operation) {
      this.operation = operation;
    }

    @Override
    public Object call(Exception e) throws Exception {
      if (e instanceof KuduException) {
        Status status = ((KuduException) e).getStatus();
        RowError rowError = new RowError(status, operation);
        return new OperationResponse(0, null, 0, operation, rowError);
      }
      return e;
    }
  }

  /**
   * A FlusherTask is created for each active buffer in mode
   * {@link FlushMode#AUTO_FLUSH_BACKGROUND}.
   */
  private final class FlusherTask implements TimerTask {
    @Override
    public void run(final Timeout timeout) {
      Buffer buffer = null;
      synchronized (monitor) {
        if (activeBuffer == null) {
          return;
        }
        if (activeBuffer.getFlusherTask() == this) {
          buffer = retireActiveBufferUnlocked();
        }
      }

      doFlush(buffer);
    }
  }

  /**
   * The {@code Buffer} consists of a list of operations, an optional pointer to a flush task,
   * and a flush notification.
   *
   * The {@link #flusherTask} is used in mode {@link FlushMode#AUTO_FLUSH_BACKGROUND} to point to
   * the background flusher task assigned to the buffer when it becomes active and the first
   * operation is applied to it. When the flusher task executes after the timeout, it checks
   * that the currently active buffer's flusher task points to itself before executing the flush.
   * This protects against the background task waking up after one or more manual flushes and
   * attempting to flush the active buffer.
   *
   * The {@link #flushNotification} deferred is used when executing manual {@link #flush}es to
   * ensure that non-active buffers are fully flushed. {@code flushNotification} is completed
   * when this buffer is successfully flushed. When the buffer is promoted from inactive to active,
   * the deferred is replaced with a new one to indicate that the buffer is not yet flushed.
   *
   * Buffer is externally synchronized. When the active buffer, {@link #monitor}
   * synchronizes access to it.
   */
  private final class Buffer {
    private final List<BufferedOperation> operations = new ArrayList<>();

    private FlusherTask flusherTask = null;

    private Deferred<Void> flushNotification = Deferred.fromResult(null);

    public List<BufferedOperation> getOperations() {
      return operations;
    }

    @GuardedBy("monitor")
    FlusherTask getFlusherTask() {
      if (flusherTask == null) {
        flusherTask = new FlusherTask();
      }
      return flusherTask;
    }

    /**
     * Returns a {@link Deferred} which will be completed when this buffer is flushed. If the buffer
     * is inactive (its flush is complete and it has been enqueued into {@link #inactiveBuffers}),
     * then the deferred will already be complete.
     */
    Deferred<Void> getFlushNotification() {
      return flushNotification;
    }

    /**
     * Completes the buffer's flush notification. Should be called when the buffer has been
     * successfully flushed.
     */
    void callbackFlushNotification() {
      LOG.trace("buffer flush notification fired: {}", this);
      flushNotification.callback(null);
    }

    /**
     * Resets the buffer's internal state. Should be called when the buffer is promoted from
     * inactive to active.
     */
    @GuardedBy("monitor")
    void resetUnlocked() {
      LOG.trace("buffer resetUnlocked: {}", this);
      operations.clear();
      flushNotification = new Deferred<>();
      flusherTask = null;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("operations", operations.size())
                        .add("flusherTask", flusherTask)
                        .add("flushNotification", flushNotification)
                        .toString();
    }
  }

  /**
   * Container class holding all the state associated with a buffered operation.
   */
  private static final class BufferedOperation {
    /** Holds either a {@link LocatedTablet} or the failure exception if the lookup failed. */
    private Object tablet = null;
    private final Deferred<Void> tabletLookup;
    private final Operation operation;

    public BufferedOperation(Deferred<LocatedTablet> tablet,
                             Operation operation) {
      tabletLookup = AsyncUtil.addBoth(tablet, new Callback<Void, Object>() {
        @Override
        public Void call(final Object tablet) {
          BufferedOperation.this.tablet = tablet;
          return null;
        }
      });
      this.operation = Preconditions.checkNotNull(operation);
    }

    /**
     * @return {@code true} if the tablet lookup failed.
     */
    public boolean tabletLookupFailed() {
      return !(tablet instanceof LocatedTablet);
    }

    /**
     * @return the located tablet
     * @throws ClassCastException if the tablet lookup failed,
     *         check with {@link #tabletLookupFailed} before calling
     */
    public LocatedTablet getTablet() {
      return (LocatedTablet) tablet;
    }

    /**
     * @return the cause of the failed lookup
     * @throws ClassCastException if the tablet lookup succeeded,
     *         check with {@link #tabletLookupFailed} before calling
     */
    public Exception getTabletLookupFailure() {
      return (Exception) tablet;
    }

    public Deferred<Void> getTabletLookup() {
      return tabletLookup;
    }

    public Operation getOperation() {
      return operation;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("tablet", tablet)
                        .add("operation", operation)
                        .toString();
    }
  }
}
