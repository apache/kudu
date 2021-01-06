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

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    // TODO(aserbin): implement

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
    Preconditions.checkState(isInFlight);

    // TODO(aserbin): implement

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
    Preconditions.checkState(!isInFlight);
    // TODO(aserbin): implement
    return false;
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

    // TODO(aserbin): implement

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
    LOG.debug("serializing handle for transaction ID {}", txnId);
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
      if (keepaliveEnabled) {
        LOG.debug("stopping keepalive heartbeating for transaction ID {}", txnId);
        // TODO(aserbin): stop sending keepalive heartbeats to TxnManager
      }
    } catch (Exception e) {
      LOG.error("exception while automatically rolling back a transaction", e);
    }
  }
}
