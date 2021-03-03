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

import java.io.IOException;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.transactions.Transactions;
import org.apache.kudu.transactions.Transactions.TxnTokenPB;

/**
 * A handle for a multi-row transaction in Kudu.
 * <p>
 * Once created using {@link KuduClient#newTransaction} or
 * {@link KuduTransaction#deserialize} methods, an instance of this class
 * can be used to commit or rollback the underlying multi-row transaction. To
 * issue write operations as a part of the transaction, use the
 * {@link KuduTransaction#newKuduSession} or
 * {@link KuduTransaction#newAsyncKuduSession} methods to create a new
 * transactional session and apply write operations using it.
 * <p>
 * The {@link KuduTransaction} implements {@link AutoCloseable} and should be
 * used with try-with-resource code construct. Once an object of this class
 * is constructed, it starts sending automatic keep-alive heartbeat messages
 * to keep the underlying transaction open. Once the object goes out of scope
 * and {@link KuduTransaction#close} is automatically called by the Java
 * runtime (or the method is called explicitly), the heartbeating stops and the
 * transaction is automatically aborted by the system after not receiving
 * heartbeat messages for a few keep-alive intervals.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class KuduTransaction implements AutoCloseable {

  /**
   * A utility class to help with the serialization of {@link KuduTransaction}.
   * <p>
   * As of now, the single purpose of this class is to control the keepalive
   * behavior for the {@link KuduTransaction} handle once it's deserialized from
   * a token. In future, the list of configurable parameters might be extended
   * (e.g., add commit and abort permissions, i.e. whether a handle can be used
   * to commit and/or abort the underlying transaction).
   */
  public static class SerializationOptions {

    private boolean enableKeepalive;

    /**
     * Construct an object with default settings.
     */
    SerializationOptions() {
      this.enableKeepalive = false;
    }

    /**
     * @return whether the transaction handle produced from an instance of
     *         {@link KuduTransaction} by the {@link KuduTransaction#serialize},
     *         {@link KuduTransaction#deserialize} call sequence will send
     *         keepalive messages to avoid automatic rollback of the underlying
     *         transaction.
     */
    public boolean isKeepaliveEnabled() {
      return enableKeepalive;
    }

    /**
     * Toggle the automatic sending of keepalive messages for transaction handle.
     * <p>
     * This method toggles the automatic sending of keepalive messages for a
     * deserialized transaction handle that is created from the result serialized
     * token upon calling {@link KuduTransaction#serialize} method.
     * <p>
     * No keepalive heartbeat messages are sent from a transaction handle whose
     * source token was created with the default "keepalive disabled" setting.
     * The idea here is that the most common use case for using transaction
     * tokens is of the "star topology" (see below), so it is enough to have
     * just one top-level handle sending keepalive messages. Overall, having more
     * than one actor sending keepalive messages for a transaction is acceptable
     * but it puts needless load on a cluster.
     * <p>
     * The most common use case for a transaction's handle
     * serialization/deserialization is of the "star topology": a transaction is
     * started by a top-level application which sends the transaction token
     * produced by serializing the original transaction handle to other worker
     * applications running concurrently, where the latter write their data
     * in the context of the same transaction and report back to the top-level
     * application, which in its turn initiates committing the transaction
     * as needed. The important point is that the top-level application keeps the
     * transaction handle around all the time from the start of the transaction
     * to the very point when transaction is committed. Under the hood, the
     * original transaction handle sends keepalive messages as required until
     * commit phase is initiated, so the deserialized transaction handles which
     * are used by the worker applications don't need to send keepalive messages.
     * <p>
     * The other (less common) use case is of the "ring topology": a chain of
     * applications work sequentially as a part of the same transaction, where
     * the very first application starts the transaction, writes its data, and
     * hands over the responsibility of managing the lifecycle of the transaction
     * to other application down the chain. After doing so it may exit, so now
     * only the next application has the active transaction handle, and so on it
     * goes until the transaction is committed by the application in the end
     * of the chain. In this scenario, every deserialized handle has to send
     * keepalive messages to avoid automatic rollback of the transaction,
     * and every application in the chain should call
     * {@link SerializationOptions#setEnableKeepalive} when serializing
     * its transaction handle into a transaction token to pass to the application
     * next in the chain.
     *
     * @param enableKeepalive whether to enable sending keepalive messages for
     *                        the {@link KuduTransaction} object once it is
     *                        deserialized from the bytes to be produced by the
     *                        {@link KuduTransaction#serialize} method.
     */
    public SerializationOptions setEnableKeepalive(boolean enableKeepalive) {
      this.enableKeepalive = enableKeepalive;
      return this;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(KuduTransaction.class);
  private static final SerializationOptions defaultSerializationOptions =
      new SerializationOptions();

  private final AsyncKuduClient client;
  private long txnId = AsyncKuduClient.INVALID_TXN_ID;
  private int keepaliveMillis = 0;
  private boolean keepaliveEnabled = true;
  private boolean isInFlight = false;
  private final Object isInFlightSync = new Object();
  private Timeout keepaliveTaskHandle = null;
  private final Object keepaliveTaskHandleSync = new Object();

  /**
   * Create an instance of a transaction handle bound to the specified client.
   * <p>
   * This constructor is used exclusively for the control paths involving
   * {@link KuduClient#newTransaction} method.
   *
   * @param client client instance to operate with the underlying transaction
   */
  KuduTransaction(AsyncKuduClient client) {
    Preconditions.checkArgument(client != null);
    this.client = client;
  }

  /**
   * Create an instance of a transaction handle for the specified parameters.
   * <p>
   * This constructor is used exclusively for the control paths involving
   * {@link KuduTransaction#deserialize}.
   *
   * @param client client instance to operate with the underlying transaction
   * @param txnId transaction identifier
   * @param keepaliveMillis keepalive timeout interval: if the backend isn't
   *                        receiving keepalive messages at least every
   *                        keepaliveMillis time interval, it automatically
   *                        aborts the underlying transaction
   * @param keepaliveEnabled whether the handle should automatically send
   *                         keepalive messages to the backend
   */
  KuduTransaction(AsyncKuduClient client,
                  long txnId,
                  int keepaliveMillis,
                  boolean keepaliveEnabled) {
    Preconditions.checkArgument(client != null);
    Preconditions.checkArgument(txnId > AsyncKuduClient.INVALID_TXN_ID);
    Preconditions.checkArgument(keepaliveMillis >= 0);
    this.client = client;
    this.txnId = txnId;
    this.keepaliveMillis = keepaliveMillis;
    this.keepaliveEnabled = keepaliveEnabled;

    startKeepaliveHeartbeating();

    this.isInFlight = true;
  }

  /**
   * Start a transaction.
   * <p>
   * This method isn't a part of the public API, it's used only internally.
   *
   * @throws KuduException if something went wrong
   */
  void begin() throws KuduException {
    synchronized (isInFlightSync) {
      Preconditions.checkState(!isInFlight);
    }

    // Make corresponding call to TxnManager and process the response,
    // in a synchronous way.
    doBeginTransaction();

    startKeepaliveHeartbeating();

    // Once the heavy-lifting has successfully completed, mark this instance
    // as a handle for an in-flight transaction.
    synchronized (isInFlightSync) {
      isInFlight = true;
    }
  }

  /**
   * Create a new {@link AsyncKuduSession} based on this transaction.
   * <p>
   * All write operations using the result session will be performed in the
   * context of this transaction.
   *
   * @return a new {@link AsyncKuduSession} instance
   */
  public AsyncKuduSession newAsyncKuduSession() {
    synchronized (isInFlightSync) {
      Preconditions.checkState(isInFlight);
    }
    return new AsyncKuduSession(client, txnId);
  }

  /**
   * Create a new {@link KuduSession} based on this transaction.
   * <p>
   * All write operations using the result session will be performed in the
   * context of this transaction.
   *
   * @return a new {@link KuduSession} instance
   */
  public KuduSession newKuduSession() {
    synchronized (isInFlightSync) {
      Preconditions.checkState(isInFlight);
    }
    return new KuduSession(new AsyncKuduSession(client, txnId));
  }

  /**
   * Commit the multi-row distributed transaction represented by this handle.
   *
   * @param wait whether to wait for the transaction's commit phase to finalize.
   *             If {@code true}, this method blocks until the commit is
   *             finalized, otherwise it starts committing the transaction and
   *             returns. In the latter case, it's possible to check for the
   *             transaction status using the
   *             {@link KuduTransaction#isCommitComplete} method.
   * @throws KuduException if something went wrong
   */
  public void commit(boolean wait) throws KuduException {
    Preconditions.checkState(isInFlight, "transaction is not open for this handle");
    CommitTransactionRequest req = doCommitTransaction();
    // Now, there is no need to continue sending keepalive messages: the
    // transaction should be in COMMIT_IN_PROGRESS state after successful
    // completion of the calls above, and the backend takes care of everything
    // else: nothing is required from the client side to successfully complete
    // the commit phase of the transaction past this point.
    synchronized (keepaliveTaskHandleSync) {
      if (keepaliveTaskHandle != null) {
        LOG.debug("stopping keepalive heartbeating after commit (txn ID {})", txnId);
        keepaliveTaskHandle.cancel();
      }
    }

    if (wait) {
      Deferred<GetTransactionStateResponse> txnState =
          getDelayedIsTransactionCommittedDeferred(req);
      KuduClient.joinAndHandleException(txnState);
    }

    // Once everything else is completed successfully, mark the transaction as
    // no longer in flight.
    synchronized (isInFlightSync) {
      isInFlight = false;
    }
  }

  /**
   * Check whether the commit phase for a transaction is complete.
   *
   * @return {@code true} if transaction has finalized, otherwise {@code false}
   * @throws KuduException if an error happens while querying the system about
   *                       the state of the transaction
   */
  public boolean isCommitComplete() throws KuduException {
    Deferred<GetTransactionStateResponse> d = isTransactionCommittedAsync();
    GetTransactionStateResponse resp = KuduClient.joinAndHandleException(d);
    final Transactions.TxnStatePB txnState = resp.txnState();
    switch (txnState) {
      case ABORT_IN_PROGRESS:
        throw new NonRecoverableException(Status.Aborted("transaction is being aborted"));
      case ABORTED:
        throw new NonRecoverableException(Status.Aborted("transaction was aborted"));
      case OPEN:
        throw new NonRecoverableException(Status.IllegalState("transaction is still open"));
      case COMMITTED:
        return true;
      case FINALIZE_IN_PROGRESS:
      case COMMIT_IN_PROGRESS:
        return false;
      default:
        throw new NonRecoverableException(Status.NotSupported(
            "unexpected transaction state: " + txnState.toString()));
    }
  }

  /**
   * Rollback the multi-row distributed transaction represented by this object.
   * <p>
   * This method initiates rolling back the transaction and returns right after
   * that. The system takes care of the rest. Once the control returns and
   * no exception is thrown, a client have a guarantee that all write
   * operations issued in the context of this transaction cannot be seen seen
   * outside.
   *
   * @throws KuduException if something went wrong
   */
  public void rollback() throws KuduException {
    Preconditions.checkState(isInFlight, "transaction is not open for this handle");
    doRollbackTransaction();
    // Now, there is no need to continue sending keepalive messages.
    synchronized (keepaliveTaskHandleSync) {
      if (keepaliveTaskHandle != null) {
        LOG.debug("stopping keepalive heartbeating after rollback (txn ID {})", txnId);
        keepaliveTaskHandle.cancel();
      }
    }

    // Once everything else is completed successfully, mark the transaction as
    // no longer in flight.
    synchronized (isInFlightSync) {
      isInFlight = false;
    }
  }

  /**
   * Export information on the underlying transaction in a serialized form.
   * <p>
   * This method transforms this handle into its serialized representation.
   * <p>
   * The serialized information on a Kudu transaction can be passed among
   * different Kudu clients running at multiple nodes, so those separate
   * Kudu clients can perform operations to be a part of the same distributed
   * transaction. The resulting string is referred as "transaction token" and
   * it can be deserialized into a transaction handle (i.e. an object of this
   * class) via the {@link KuduTransaction#deserialize} method.
   * <p>
   * This method doesn't perform any RPC under the hood.
   * <p>
   * The representation of the data in the serialized form (i.e. the format of
   * a Kudu transaction token) is an implementation detail, not a part of the
   * public API and can be changed without notice.
   *
   * @return the serialized form of this transaction handle
   * @throws IOException if serialization fails
   */
  public byte[] serialize(SerializationOptions options) throws IOException {
    LOG.debug("serializing handle (txn ID {})", txnId);
    Preconditions.checkState(
        txnId != AsyncKuduClient.INVALID_TXN_ID,
        "invalid transaction handle");
    TxnTokenPB.Builder b = TxnTokenPB.newBuilder();
    b.setTxnId(txnId);
    b.setEnableKeepalive(options.isKeepaliveEnabled());
    b.setKeepaliveMillis(keepaliveMillis);
    TxnTokenPB message = b.build();
    byte[] buf = new byte[message.getSerializedSize()];
    CodedOutputStream cos = CodedOutputStream.newInstance(buf);
    message.writeTo(cos);
    cos.flush();
    return buf;
  }

  /**
   * A shortcut for the {@link KuduTransaction#serialize(SerializationOptions)}
   * method invoked with default-constructed {@link SerializationOptions}.
   */
  public byte[] serialize() throws IOException {
    return serialize(defaultSerializationOptions);
  }

  /**
   * Re-create KuduTransaction object given its serialized representation.
   * <p>
   * This method doesn't perform any RPC under the hood. The newly created
   * object automatically does or does not send keep-alive messages depending
   * on the {@link SerializationOptions#isKeepaliveEnabled} setting when
   * the original {@link KuduTransaction} object was serialized using
   * {@link KuduTransaction#serialize} method.
   * <p>
   * @param client Client instance to bound the result object to
   * @param buf serialized representation of a {@link KuduTransaction} object
   * @return Operation result status.
   * @throws IOException if deserialization fails
   */
  public static KuduTransaction deserialize(
      byte[] buf, AsyncKuduClient client) throws IOException {
    TxnTokenPB pb = TxnTokenPB.parseFrom(CodedInputStream.newInstance(buf));
    final long txnId = pb.getTxnId();
    final int keepaliveMillis = pb.getKeepaliveMillis();
    final boolean keepaliveEnabled =
        pb.hasEnableKeepalive() && pb.getEnableKeepalive();
    return new KuduTransaction(client, txnId, keepaliveMillis, keepaliveEnabled);
  }

  /**
   * Stop keepalive heartbeating, if any was in progress for this transaction
   * handle.
   * <p>
   * This method is called automatically when the object goes out of scope
   * as prescribed for {@link AutoCloseable}.
   * <p>
   * This method doesn't throw according to the recommendations for
   * {@link AutoCloseable#close}. In case of an error, this method just logs
   * the corresponding error message.
   */
  @Override
  public void close() {
    try {
      synchronized (keepaliveTaskHandleSync) {
        if (keepaliveTaskHandle != null) {
          LOG.debug("stopping keepalive heartbeating (txn ID {})", txnId);
          keepaliveTaskHandle.cancel();
        }
      }
    } catch (Exception e) {
      LOG.error("exception while automatically rolling back a transaction", e);
    }
  }

  private void doBeginTransaction() throws KuduException {
    BeginTransactionRequest request = new BeginTransactionRequest(
        client.getMasterTable(),
        client.getTimer(),
        client.getDefaultAdminOperationTimeoutMs());
    Deferred<BeginTransactionResponse> d = client.sendRpcToTablet(request);
    BeginTransactionResponse resp = KuduClient.joinAndHandleException(d);
    txnId = resp.txnId();
    keepaliveMillis = resp.keepaliveMillis();
  }

  private void doRollbackTransaction() throws KuduException {
    AbortTransactionRequest request = new AbortTransactionRequest(
        client.getMasterTable(),
        client.getTimer(),
        client.getDefaultAdminOperationTimeoutMs(),
        txnId);
    Deferred<AbortTransactionResponse> d = client.sendRpcToTablet(request);
    KuduClient.joinAndHandleException(d);
  }

  private CommitTransactionRequest doCommitTransaction() throws KuduException {
    CommitTransactionRequest request = new CommitTransactionRequest(
        client.getMasterTable(),
        client.getTimer(),
        client.getDefaultAdminOperationTimeoutMs(),
        txnId);
    Deferred<CommitTransactionResponse> d = client.sendRpcToTablet(request);
    KuduClient.joinAndHandleException(d);
    return request;
  }

  private Deferred<GetTransactionStateResponse> isTransactionCommittedAsync() {
    GetTransactionStateRequest request = new GetTransactionStateRequest(
        client.getMasterTable(),
        client.getTimer(),
        client.getDefaultAdminOperationTimeoutMs(),
        txnId);
    return client.sendRpcToTablet(request);
  }

  Deferred<GetTransactionStateResponse> getDelayedIsTransactionCommittedDeferred(
      KuduRpc<?> parent) {
    // TODO(aserbin): By scheduling even the first RPC via timer, the sequence of
    // RPCs is delayed by at least one timer tick, which is unfortunate for the
    // case where the transaction is fully committed.
    //
    // Eliminating the delay by sending the first RPC immediately (and
    // scheduling the rest via timer) would also allow us to replace this "fake"
    // RPC with a real one.
    KuduRpc<GetTransactionStateResponse> fakeRpc = client.buildFakeRpc(
        "GetTransactionState", parent);

    // Store the Deferred locally; callback() or errback() on the RPC will
    // reset it and we'd return a different, non-triggered Deferred.
    Deferred<GetTransactionStateResponse> fakeRpcD = fakeRpc.getDeferred();

    delayedIsTransactionCommitted(
        fakeRpc,
        isTransactionCommittedCb(fakeRpc),
        isTransactionCommittedErrb(fakeRpc));
    return fakeRpcD;
  }

  private void delayedIsTransactionCommitted(
      final KuduRpc<GetTransactionStateResponse> rpc,
      final Callback<Deferred<GetTransactionStateResponse>,
          GetTransactionStateResponse> callback,
      final Callback<Exception, Exception> errback) {
    final class RetryTimer implements TimerTask {
      @Override
      public void run(final Timeout timeout) {
        isTransactionCommittedAsync().addCallbacks(callback, errback);
      }
    }

    long sleepTimeMillis = client.getSleepTimeForRpcMillis(rpc);
    if (rpc.timeoutTracker.wouldSleepingTimeoutMillis(sleepTimeMillis)) {
      AsyncKuduClient.tooManyAttemptsOrTimeout(rpc, null);
      return;
    }
    AsyncKuduClient.newTimeout(client.getTimer(), new RetryTimer(), sleepTimeMillis);
  }

  /**
   * Returns a callback to be called upon completion of GetTransactionState RPC.
   * If the transaction is committed, triggers the provided RPC's callback chain
   * with 'txnResp' as its value. Otherwise, sends another GetTransactionState
   * RPC after sleeping.
   * <p>
   * @param rpc RPC that initiated this sequence of operations
   * @return callback that will eventually return 'txnResp'
   */
  private Callback<Deferred<GetTransactionStateResponse>, GetTransactionStateResponse>
      isTransactionCommittedCb(final KuduRpc<GetTransactionStateResponse> rpc) {
    return resp -> {
      // Store the Deferred locally; callback() below will reset it and we'd
      // return a different, non-triggered Deferred.
      Deferred<GetTransactionStateResponse> d = rpc.getDeferred();
      if (resp.isCommitted()) {
        rpc.callback(resp);
      } else if (resp.isAborted()) {
        rpc.errback(new NonRecoverableException(
            Status.Aborted("transaction was aborted")));
      } else {
        rpc.attempt++;
        delayedIsTransactionCommitted(
            rpc,
            isTransactionCommittedCb(rpc),
            isTransactionCommittedErrb(rpc));
      }
      return d;
    };
  }

  private <R> Callback<Exception, Exception> isTransactionCommittedErrb(
      final KuduRpc<R> rpc) {
    return e -> {
      rpc.errback(e);
      return e;
    };
  }

  /**
   * Return period for sending keepalive messages for the specified keepalive
   * timeout (both in milliseconds). The latter is dictated by the backend
   * which can automatically rollback a transaction after not receiving
   * keepalive messages for longer than the specified timeout interval.
   * Ideally, it would be enough to send a heartbeat message every
   * {@code keepaliveMillis} interval, but given scheduling irregularities,
   * client node timer's precision, and various network delays and latencies,
   * it's safer to schedule sending keepalive messages from the client side
   * more frequently.
   *
   * @param keepaliveMillis the keepalive timeout interval
   * @return a proper period for sending keepalive messages from the client side
   */
  private static long keepalivePeriodForTimeout(long keepaliveMillis) {
    Preconditions.checkArgument(keepaliveMillis > 0,
        "keepalive timeout must be a positive number");
    long period = keepaliveMillis / 2;
    if (period <= 0) {
      period = 1;
    }
    return period;
  }

  private void startKeepaliveHeartbeating() {
    if (keepaliveEnabled) {
      LOG.debug("starting keepalive heartbeating with period {} ms (txn ID {})",
          txnId, keepalivePeriodForTimeout(keepaliveMillis));
      doStartKeepaliveHeartbeating();
    } else {
      LOG.debug("keepalive heartbeating disabled for this handle (txn ID {})", txnId);
    }
  }

  private final class SendKeepaliveTask implements TimerTask {
    /**
     * Send keepalive heartbeat message for the transaction represented by
     * this {@link KuduTransaction} handle and re-schedule itself
     * (i.e. this task) to send next heartbeat interval
     *
     * @param timeout a handle which is associated with this task
     */
    @Override
    public void run(Timeout timeout) throws Exception {
      if (timeout.isCancelled()) {
        LOG.debug("terminating keepalive task (txn ID {})", txnId);
        return;
      }
      try {
        doSendKeepalive();
      } catch (RecoverableException e) {
        // Just continue sending heartbeats as required: the recoverable
        // exception means the condition is transient.
        // TODO(aserbin): should we send next heartbeat sooner? E.g., retry
        //                immediately, and do such retry only once after a
        //                failure like this. The idea is to avoid missing
        //                heartbeats in situations where the second attempt
        //                after keepaliveMillis/2 would as well due to a network
        //                issue, but immediate retry could succeed.
        LOG.debug("continuing keepalive heartbeating (txn ID {}): {}",
            txnId, e.toString());
      } catch (Exception e) {
        LOG.debug("terminating keepalive task (txn ID {}) due to exception {}",
            txnId, e.toString());
        return;
      }
      synchronized (keepaliveTaskHandleSync) {
        // Re-schedule the task, refreshing the task handle.
        keepaliveTaskHandle = AsyncKuduClient.newTimeout(
            timeout.timer(), this, keepalivePeriodForTimeout(keepaliveMillis));
      }
    }

    private void doSendKeepalive() throws KuduException {
      KeepTransactionAliveRequest request = new KeepTransactionAliveRequest(
          client.getMasterTable(),
          client.getTimer(),
          client.getDefaultAdminOperationTimeoutMs(),
          txnId);
      Deferred<KeepTransactionAliveResponse> d = client.sendRpcToTablet(request);
      KuduClient.joinAndHandleException(d);
    }
  }

  void doStartKeepaliveHeartbeating() {
    Preconditions.checkState(keepaliveEnabled);
    synchronized (keepaliveTaskHandleSync) {
      Preconditions.checkState(keepaliveTaskHandle == null,
          "keepalive heartbeating has already started");
      keepaliveTaskHandle = AsyncKuduClient.newTimeout(
          client.getTimer(),
          new SendKeepaliveTask(),
          keepalivePeriodForTimeout(keepaliveMillis));
    }
  }
}
