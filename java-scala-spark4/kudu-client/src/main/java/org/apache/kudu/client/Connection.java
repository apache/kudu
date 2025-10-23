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

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;

/**
 * Class representing a connection from the client to a Kudu server (master or tablet server):
 * a high-level wrapper for the TCP connection between the client and the server.
 * <p>
 * It's a stateful handler that manages a connection to a Kudu server.
 * <p>
 * This handler manages the RPC IDs, and keeps track of the RPCs in flight for which
 * a response is currently awaited, as well as temporarily buffered RPCs that are waiting
 * to be sent to the server.
 * <p>
 * Acquiring the monitor on an object of this class will prevent it from
 * accepting write requests as well as buffering requests if the underlying
 * channel isn't connected.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class Connection extends SimpleChannelInboundHandler<Object> {
  /**
   * Authentication credentials policy for negotiating outbound connections. Some requests
   * (e.g. {@link ConnectToMasterRequest}) behave differently depending on the type of credentials
   * used for authentication when negotiating on the underlying connection. If some particular
   * behavior is required, it's necessary to specify appropriate credentials policy while creating
   * an instance of this object.
   */
  public enum CredentialsPolicy {
    /** It's acceptable to use authentication credentials of any type, primary or secondary ones. */
    ANY_CREDENTIALS,

    /**
     * Only primary credentials are acceptable. Primary credentials are Kerberos tickets,
     * TLS certificate. Secondary credentials are authentication tokens: they are 'derived'
     * in the sense that it's possible to acquire them using 'primary' credentials.
     */
    PRIMARY_CREDENTIALS,
  }

  /** Information on the target server. */
  private final ServerInfo serverInfo;

  /** Security context to use for connection negotiation. */
  private final SecurityContext securityContext;

  /** Credentials policy to use when authenticating. */
  private final CredentialsPolicy credentialsPolicy;

  /** The Netty client bootstrap used to configure and initialize a connected channel. */
  private final Bootstrap bootstrap;

  private final String saslProtocolName;

  private final boolean requireAuthentication;

  private final boolean requireEncryption;

  private final boolean encryptLoopback;

  /** The underlying Netty's socket channel. */
  private SocketChannel channel;

  /**
   * Set to true when disconnect initiated explicitly from the client side. The channelDisconnected
   * event handler then knows not to log any warning about unexpected disconnection from the peer.
   */
  private volatile boolean explicitlyDisconnected = false;

  /** Logger: a sink for the log messages originated from this class. */
  private static final Logger LOG = LoggerFactory.getLogger(Connection.class);

  private static final byte RPC_CURRENT_VERSION = 9;

  /** Initial header sent by the client upon connection establishment. */
  private static final byte[] CONNECTION_HEADER = new byte[]{'h', 'r', 'p', 'c',
      RPC_CURRENT_VERSION,     // RPC version.
      0,
      0
  };

  private static final String NEGOTIATION_TIMEOUT_HANDLER = "negotiation-timeout-handler";
  private final long negotiationTimeoutMs;

  /** Lock to guard access to some of the fields below. */
  private final ReentrantLock lock = new ReentrantLock();

  /** The current state of this Connection object. */
  @GuardedBy("lock")
  private State state;

  /**
   * A hash table to store { callId, statusReportCallback } pairs, representing messages which have
   * already been sent and pending responses from the server side. Once the server responds to a
   * message, the corresponding entry is removed from the container and the response callback
   * is invoked with the results represented by {@link CallResponseInfo}.
   */
  @GuardedBy("lock")
  private HashMap<Integer, Callback<Void, CallResponseInfo>> inflightMessages = new HashMap<>();

  /** Messages enqueued while the connection was not ready to start sending them over the wire. */
  @GuardedBy("lock")
  private ArrayList<QueuedMessage> queuedMessages = Lists.newArrayList();

  /** The result of the successful connection negotiation. */
  @GuardedBy("lock")
  private Negotiator.Success negotiationResult = null;

  /** The result of failed connection negotiation. */
  @GuardedBy("lock")
  private Negotiator.Failure negotiationFailure = null;

  /** A monotonically increasing counter for RPC IDs. */
  @GuardedBy("lock")
  private int nextCallId = 0;

  /** The future for the connection attempt. Set only once connect() is called. */
  @Nullable
  @GuardedBy("lock")
  private ChannelFuture connectFuture;

  /**
   * Create a new Connection object to the specified destination.
   *
   * @param serverInfo the destination server
   * @param securityContext security context to use for connection negotiation
   * @param bootstrap Netty bootstrap to create corresponding Netty channel
   * @param credentialsPolicy policy controlling which credentials to use while negotiating on the
   *                          connection to the target server:
   *                          if {@link CredentialsPolicy#PRIMARY_CREDENTIALS}, the authentication
   * @param saslProtocolName SASL protocol name used when connecting to secure
   *                         clusters. Must match the servers' service principal name.
   */
  Connection(ServerInfo serverInfo,
             SecurityContext securityContext,
             Bootstrap bootstrap,
             CredentialsPolicy credentialsPolicy,
             String saslProtocolName,
             boolean requireAuthentication,
             boolean requireEncryption,
             boolean encryptLoopback,
             long negotiationTimeoutMs) {
    this.serverInfo = serverInfo;
    this.securityContext = securityContext;
    this.saslProtocolName = saslProtocolName;
    this.state = State.NEW;
    this.credentialsPolicy = credentialsPolicy;
    this.bootstrap = bootstrap.clone();
    this.bootstrap.handler(new ConnectionChannelInitializer());
    this.requireAuthentication = requireAuthentication;
    this.requireEncryption = requireEncryption;
    this.encryptLoopback = encryptLoopback;
    this.negotiationTimeoutMs = negotiationTimeoutMs;
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void channelActive(final ChannelHandlerContext ctx) {
    lock.lock();
    try {
      if (state == State.TERMINATED) {
        return;
      }
      Preconditions.checkState(state == State.CONNECTING);
      state = State.NEGOTIATING;
    } finally {
      lock.unlock();
    }
    ctx.writeAndFlush(Unpooled.wrappedBuffer(CONNECTION_HEADER), ctx.voidPromise());
    Negotiator negotiator = new Negotiator(serverInfo.getAndCanonicalizeHostname(), securityContext,
        (credentialsPolicy == CredentialsPolicy.PRIMARY_CREDENTIALS), saslProtocolName,
        requireAuthentication, requireEncryption, encryptLoopback);
    ctx.pipeline().addBefore(ctx.name(), "negotiation", negotiator);
    negotiator.sendHello(ctx);
  }

  /** {@inheritDoc} */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) {
    LOG.debug("{} handling channelInactive", getLogPrefix());
    String msg = "connection closed";
    // Connection failures are reported as channelClosed() before exceptionCaught() is called.
    // We can detect this case by looking at whether connectFuture has been marked complete
    // and grabbing the exception from there.
    lock.lock();
    try {
      if (connectFuture != null && connectFuture.cause() != null) {
        msg = connectFuture.cause().toString();
      }
    } finally {
      lock.unlock();
    }
    // No need to call super.channelInactive(ctx, e) -- there should be nobody in the upstream
    // pipeline after Connection itself. So, just handle the close event ourselves.
    cleanup(new RecoverableException(Status.NetworkError(msg)));
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void channelRead0(ChannelHandlerContext ctx, Object m) throws Exception {
    // Process the results of a successful negotiation.
    if (m instanceof Negotiator.Success) {
      lock.lock();
      try {
        negotiationResult = (Negotiator.Success) m;
        Preconditions.checkState(state == State.TERMINATED || inflightMessages.isEmpty());

        // Before switching to the READY state, it's necessary to empty the queuedMessages. There
        // might be concurrent activity on adding new messages into the queue if enqueueMessage()
        // is called in the middle.
        while (state != State.TERMINATED && !queuedMessages.isEmpty()) {

          // Register the messages into the inflightMessages before sending them to the wire. This
          // is to be able to invoke appropriate callback when the response received. This should
          // be done under the lock since the inflightMessages itself does not provide any
          // concurrency guarantees.
          List<QueuedMessage> queued = queuedMessages;
          for (final QueuedMessage qm : queued) {
            Callback<Void, CallResponseInfo> empty = inflightMessages.put(
                qm.message.getHeaderBuilder().getCallId(), qm.cb);
            Preconditions.checkState(empty == null);
          }
          queuedMessages = Lists.newArrayList();

          lock.unlock();
          try {
            // Send out the enqueued messages while not holding the lock. This is to avoid
            // deadlock if channelDisconnected/channelClosed event happens and cleanup() is called.
            for (final QueuedMessage qm : queued) {
              sendCallToWire(qm.message);
            }
          } finally {
            lock.lock();
          }
        }
        // The connection may have been terminated while the lock was dropped.
        if (state == State.TERMINATED) {
          return;
        }

        Preconditions.checkState(state == State.NEGOTIATING);

        queuedMessages = null;

        // Drop the negotiation timeout handler from the pipeline.
        ctx.pipeline().remove(NEGOTIATION_TIMEOUT_HANDLER);

        // Set the state to READY -- that means the incoming messages should be no longer put into
        // the queuedMessages, but sent to wire right away (see the enqueueMessage() for details).
        state = State.READY;
      } finally {
        lock.unlock();
      }
      return;
    }

    // Process the results of a failed negotiation.
    if (m instanceof Negotiator.Failure) {
      lock.lock();
      try {
        if (state == State.TERMINATED) {
          return;
        }
        Preconditions.checkState(state == State.NEGOTIATING);
        Preconditions.checkState(inflightMessages.isEmpty());

        state = State.NEGOTIATION_FAILED;
        negotiationFailure = (Negotiator.Failure) m;
      } finally {
        lock.unlock();
      }
      // Calling close() triggers the cleanup() which will handle the negotiation
      // failure appropriately.
      ctx.close();
      return;
    }

    // Some other event which the connection does not handle.
    if (!(m instanceof CallResponse)) {
      ctx.fireChannelRead(m);
      return;
    }

    final CallResponse response = (CallResponse) m;
    final RpcHeader.ResponseHeader header = response.getHeader();
    if (!header.hasCallId()) {
      final int size = response.getTotalResponseSize();
      final String msg = getLogPrefix() +
          " RPC response (size: " + size + ") doesn't" + " have callID: " + header;
      LOG.error(msg);
      throw new NonRecoverableException(Status.Incomplete(msg));
    }

    final int callId = header.getCallId();
    Callback<Void, CallResponseInfo> responseCbk;
    lock.lock();
    try {
      if (state == State.TERMINATED) {
        return;
      }
      Preconditions.checkState(state == State.READY);
      responseCbk = inflightMessages.remove(callId);
    } finally {
      lock.unlock();
    }

    if (responseCbk == null) {
      final String msg = getLogPrefix() + " invalid callID: " + callId;
      LOG.error(msg);
      // If we get a bad RPC ID back, we are probably somehow misaligned from
      // the server. So, we disconnect the connection.
      throw new NonRecoverableException(Status.IllegalState(msg));
    }

    if (!header.hasIsError() || !header.getIsError()) {
      // The success case.
      responseCbk.call(new CallResponseInfo(response, null));
      return;
    }

    final RpcHeader.ErrorStatusPB.Builder errorBuilder = RpcHeader.ErrorStatusPB.newBuilder();
    KuduRpc.readProtobuf(response.getPBMessage(), errorBuilder);
    final RpcHeader.ErrorStatusPB error = errorBuilder.build();
    RpcHeader.ErrorStatusPB.RpcErrorCodePB code = error.getCode();
    if (code.equals(RpcHeader.ErrorStatusPB.RpcErrorCodePB.ERROR_SERVER_TOO_BUSY) ||
        code.equals(RpcHeader.ErrorStatusPB.RpcErrorCodePB.ERROR_UNAVAILABLE)) {
      responseCbk.call(new CallResponseInfo(
          response, new RecoverableException(Status.ServiceUnavailable(error.getMessage()))));
      return;
    }
    if (code.equals(RpcHeader.ErrorStatusPB.RpcErrorCodePB.ERROR_INVALID_AUTHORIZATION_TOKEN)) {
      responseCbk.call(new CallResponseInfo(
          response, new InvalidAuthzTokenException(Status.NotAuthorized(error.getMessage()))));
      return;
    }

    final String message = getLogPrefix() + " server sent error " + error.getMessage();
    LOG.error(message); // can be useful
    responseCbk.call(new CallResponseInfo(
        response, new RpcRemoteException(Status.RemoteError(message), error)));
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
    KuduException error;
    if (e instanceof KuduException) {
      error = (KuduException) e;
    } else if (e instanceof RejectedExecutionException) {
      String message = String.format("%s RPC rejected by the executor (ignore if shutting down)",
                                     getLogPrefix());
      error = new RecoverableException(Status.NetworkError(message), e);
      LOG.warn(message, e);
    } else if (e instanceof ReadTimeoutException) {
      String message = String.format("%s encountered a read timeout; closing the channel",
                                     getLogPrefix());
      error = new RecoverableException(Status.NetworkError(message), e);
      LOG.debug(message);
    } else if (e instanceof ClosedChannelException) {
      String message = String.format(
          explicitlyDisconnected ? "%s disconnected from peer" : "%s lost connection to peer",
          getLogPrefix());
      error = new RecoverableException(Status.NetworkError(message), e);
      LOG.info(message);
    } else if (e instanceof ConnectException) {
      String message = "Failed to connect to peer " + serverInfo + ": " + e.getMessage();
      error = new RecoverableException(Status.NetworkError(message), e);
      LOG.info(message);
    } else if (e instanceof SSLException && explicitlyDisconnected) {
      // There's a race in Netty where, when we call Channel.close(), it tries
      // to send a TLS 'shutdown' message and enters a shutdown state. If another
      // thread races to send actual data on the channel, then Netty will get a
      // bit confused that we are trying to send data and misinterpret it as a
      // renegotiation attempt, and throw an SSLException. So, we just ignore any
      // SSLException if we've already attempted to close, otherwise log the error.
      error = new RecoverableException(Status.NetworkError(
          String.format("%s disconnected from peer", getLogPrefix())));
    } else if (e instanceof SSLPeerUnverifiedException) {
      String m = String.format("unable to verify identity of peer %s: %s",
          serverInfo, e.getMessage());
      error = new NonRecoverableException(Status.NetworkError(m), e);
      LOG.error(m, e);
    } else {
      // If the connection was explicitly disconnected via a call to disconnect(), we should
      // have either gotten a ClosedChannelException or an SSLException.
      assert !explicitlyDisconnected;
      String channelInfo = ctx == null ? "" : String.format(" on %s", ctx.channel());
      String message = String.format(
          "%s unexpected exception from downstream%s", getLogPrefix(), channelInfo);
      error = new RecoverableException(Status.NetworkError(message), e);
      LOG.error(message, e);
    }

    cleanup(error);
    // `ctx` is null when `exceptionCaught` is called from the `connectFuture`
    // listener in `connect()`.
    if (ctx != null) {
      ctx.close();
    }
  }

  /** Getter for the peer's end-point information */
  public ServerInfo getServerInfo() {
    return serverInfo;
  }

  /** The credentials policy used for the connection negotiation. */
  CredentialsPolicy getCredentialsPolicy() {
    return credentialsPolicy;
  }

  /** @return true iff the connection is in the TERMINATED state */
  boolean isTerminated() {
    lock.lock();
    try {
      return state == State.TERMINATED;
    } finally {
      lock.unlock();
    }
  }

  /**
   * TODO(aserbin) make it possible to avoid calling this when the server features are not known yet
   *
   * @return the set of server's features, if known; null otherwise
   */
  @Nullable
  Set<RpcFeatureFlag> getPeerFeatures() {
    Set<RpcFeatureFlag> features = null;
    lock.lock();
    try {
      if (negotiationResult != null) {
        features = negotiationResult.serverFeatures;
      }
    } finally {
      lock.unlock();
    }
    return features;
  }

  /** @return string representation of the peer information suitable for logging */
  String getLogPrefix() {
    return "[peer " + serverInfo + "]";
  }

  /**
   * Enqueue outbound message for sending to the remote server via Kudu RPC. The enqueueMessage()
   * accepts messages even if the connection hasn't yet been established: the enqueued messages
   * are sent out as soon as the connection to the server is ready. The connection is initiated upon
   * enqueuing the very first outbound message.
   */
  void enqueueMessage(RpcOutboundMessage msg, Callback<Void, CallResponseInfo> cb)
      throws RecoverableException {
    lock.lock();
    try {
      if (state == State.TERMINATED) {
        // The upper-level caller should handle the exception and retry using a new connection.
        throw new RecoverableException(Status.IllegalState("connection is terminated"));
      }

      if (state == State.NEW) {
        // Schedule connecting to the server.
        connect();
      }

      // Set the call identifier for the outgoing RPC.
      final int callId = nextCallId++;
      RpcHeader.RequestHeader.Builder headerBuilder = msg.getHeaderBuilder();
      headerBuilder.setCallId(callId);

      // Amend the timeout for the call, if necessary.
      final int timeoutMs = headerBuilder.getTimeoutMillis();
      if (timeoutMs > 0) {
        headerBuilder.setTimeoutMillis(timeoutMs);
      }

      // If the connection hasn't been negotiated yet, add the message into the queuedMessages list.
      // The elements of the queuedMessages list will be processed when the negotiation either
      // succeeds or fails.
      if (state != State.READY) {
        queuedMessages.add(new QueuedMessage(msg, cb));
        return;
      }

      assert state == State.READY;
      // Register the message into the inflightMessages before sending it to the wire.
      final Callback<Void, CallResponseInfo> empty = inflightMessages.put(callId, cb);
      Preconditions.checkState(empty == null);
    } finally {
      lock.unlock();
    }

    // It's time to initiate sending the message over the wire. This is done outside of the lock
    // to prevent deadlocks due to the reverse order of locking while working with Connection.lock
    // and the lower-level Netty locks. The other order of taking those two locks could happen
    // upon receiving ChannelDisconnected or ChannelClosed events. Upon receiving those events,
    // the low-level Netty lock is held and the channelDisconnected()/channelClosed() methods
    // would call the cleanup() method. In its turn, the cleanup() method tries to acquire the
    // Connection.lock lock, while the low-level Netty lock might be already acquired.
    //
    // More details and an example of a stack trace is available in KUDU-1894 comments.
    sendCallToWire(msg);
  }

  /**
   * Triggers the channel to be disconnected, which will asynchronously cause all
   * queued and in-flight RPCs to be failed. This method is idempotent.
   *
   * @return future object to wait on the disconnect completion, if necessary
   */
  ChannelFuture disconnect() {
    lock.lock();
    try {
      LOG.debug("{} disconnecting while in state {}", getLogPrefix(), state);
      explicitlyDisconnected = true;
      // No connection has been made yet.
      if (state == State.NEW) {
        // Use an EmbeddedChannel to return a valid and immediately completed ChannelFuture.
        return new EmbeddedChannel().disconnect();
      } else {
        return connectFuture.channel().disconnect();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * If open, forcefully shut down the connection to the server. This is the same as
   * {@link #disconnect}, but it returns Deferred instead of ChannelFuture.
   *
   * @return deferred object for tracking the shutting down of this connection
   */
  Deferred<Void> shutdown() {
    final ChannelFuture disconnectFuture = disconnect();
    final Deferred<Void> d = new Deferred<>();
    disconnectFuture.addListener(new ShutdownListener(d));
    return d;
  }

  /**
   * A ChannelFutureListener that completes the passed deferred on completion.
   */
  private static class ShutdownListener implements ChannelFutureListener {

    private final Deferred<Void> deferred;

    public ShutdownListener(Deferred<Void> deferred) {
      this.deferred = deferred;
    }

    @Override
    public void operationComplete(final ChannelFuture future) {
      if (future.isSuccess()) {
        deferred.callback(null);
        return;
      }
      final Throwable t = future.cause();
      if (t instanceof Exception) {
        deferred.callback(t);
      } else {
        deferred.callback(new NonRecoverableException(
            Status.IllegalState("failed to shutdown: " + this), t));
      }
    }
  }

  /** @return string representation of this object (suitable for printing into the logs, etc.) */
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("Connection@")
        .append(hashCode())
        .append("(channel=")
        .append(channel)
        .append(", uuid=")
        .append(serverInfo.getUuid());
    int queuedMessagesNum = 0;
    int inflightMessagesNum = 0;
    lock.lock();
    try {
      queuedMessagesNum = queuedMessages == null ? 0 : queuedMessages.size();
      inflightMessagesNum = inflightMessages == null ? 0 : inflightMessages.size();
    } finally {
      lock.unlock();
    }
    buf.append(", #queued=").append(queuedMessagesNum)
        .append(", #inflight=").append(inflightMessagesNum)
        .append(")");
    return buf.toString();
  }

  /**
   * This is test-only method.
   *
   * @return true iff the connection is in the READY state
   */
  @InterfaceAudience.LimitedPrivate("Test")
  boolean isReady() {
    lock.lock();
    try {
      return state == State.READY;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Start sending the message to the server over the wire. It's crucial to not hold the lock
   * while doing so: see enqueueMessage() and KUDU-1894 for details.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void sendCallToWire(final RpcOutboundMessage msg) {
    assert !lock.isHeldByCurrentThread();
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} sending {}", getLogPrefix(), msg);
    }
    channel.writeAndFlush(msg, channel.voidPromise());
  }

  /**
   * Process the fact that the connection has been disconnected: update the state of this object and
   * clean up any outstanding or lingering messages, notifying on the error via their status
   * callbacks. The callee is supposed to handle the error and retry sending the messages,
   * if needed.
   *
   * @param error the exception which caused the connection cleanup
   */
  private void cleanup(KuduException error) {
    List<QueuedMessage> queued;
    Map<Integer, Callback<Void, CallResponseInfo>> inflight;

    boolean needNewAuthnToken = false;
    lock.lock();
    try {
      if (state == State.TERMINATED) {
        // The cleanup has already run.
        Preconditions.checkState(queuedMessages == null);
        Preconditions.checkState(inflightMessages == null);
        return;
      }
      if (state == State.NEGOTIATION_FAILED) {
        Preconditions.checkState(negotiationFailure != null);
        Preconditions.checkState(inflightMessages.isEmpty());
        needNewAuthnToken = negotiationFailure.status.getCode().equals(
            RpcHeader.ErrorStatusPB.RpcErrorCodePB.FATAL_INVALID_AUTHENTICATION_TOKEN);
      }
      LOG.debug("{} cleaning up while in state {} due to: {}",
                getLogPrefix(), state, error.getMessage());

      queued = queuedMessages;
      queuedMessages = null;

      inflight = inflightMessages;
      inflightMessages = null;

      state = State.TERMINATED;
    } finally {
      lock.unlock();
    }
    if (needNewAuthnToken) {
      error = new InvalidAuthnTokenException(error.getStatus());
    }

    for (Callback<Void, CallResponseInfo> cb : inflight.values()) {
      try {
        cb.call(new CallResponseInfo(null, error));
      } catch (Exception e) {
        LOG.warn("{} exception while aborting in-flight call: {}", getLogPrefix(), e);
      }
    }

    if (queued != null) {
      for (QueuedMessage qm : queued) {
        try {
          qm.cb.call(new CallResponseInfo(null, error));
        } catch (Exception e) {
          LOG.warn("{} exception while aborting enqueued call: {}", getLogPrefix(), e);
        }
      }
    }
  }

  /** Initiate opening TCP connection to the server. */
  @GuardedBy("lock")
  private void connect() {
    LOG.debug("{} connecting to peer", getLogPrefix());
    Preconditions.checkState(lock.isHeldByCurrentThread());
    Preconditions.checkState(state == State.NEW);
    state = State.CONNECTING;
    connectFuture = bootstrap.connect(serverInfo.getResolvedAddress());
    connectFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
      @Override
      public void operationComplete(Future<? super Void> future) throws Exception {
        if (future.isSuccess()) {
          LOG.debug("{} Successfully connected to peer", getLogPrefix());
          return;
        }
        // If the connection failed, pass the exception to exceptionCaught to be handled.
        final Throwable t = future.cause();
        exceptionCaught(null, t);
      }
    });
    channel = (SocketChannel) connectFuture.channel();
  }

  /** Enumeration to represent the internal state of the Connection object. */
  private enum State {
    /** The object has just been created. */
    NEW,

    /** The establishment of TCP connection to the server has started. */
    CONNECTING,

    /** The connection negotiation has started. */
    NEGOTIATING,

    /**
     * The underlying TCP connection has been dropped off due to negotiation error and there are
     * enqueued messages to handle. Once connection negotiation fails, the Connection object
     * handles the affected queued RPCs appropriately. If the negotiation failed due to invalid
     * authn token error, the upper-level code may attempt to acquire a new authentication token
     * in that case. The connection transitions into the TERMINATED state upon notifying the
     * affected RPCs on the connection negotiation failure.
     */
    NEGOTIATION_FAILED,

    /** The connection to the server is opened, negotiated, and ready to use. */
    READY,

    /**
     * The TCP connection has been dropped off, the proper clean-up procedure has run and no queued
     * nor in-flight messages are left. In this state, the object does not accept new messages,
     * throwing RecoverableException upon call of the enqueueMessage() method.
     */
    TERMINATED,
  }

  /**
   * The class to represent RPC response received from the remote server.
   * If the {@code exception} is null, then it's a success case and the {@code response} contains
   * the information on the response. Otherwise it's an error and the {@code exception} provides
   * information on the error. For the recoverable error case, the {@code exception} is of
   * {@link RecoverableException} type, otherwise it's of {@link NonRecoverableException} type.
   */
  static final class CallResponseInfo {
    public final CallResponse response;
    public final KuduException exception;

    CallResponseInfo(CallResponse response, KuduException exception) {
      this.response = response;
      this.exception = exception;
    }
  }

  /** Internal class representing an enqueued outgoing message. */
  private static final class QueuedMessage {
    private final RpcOutboundMessage message;
    private final Callback<Void, CallResponseInfo> cb;

    QueuedMessage(RpcOutboundMessage message, Callback<Void, CallResponseInfo> cb) {
      this.message = message;
      this.cb = cb;
    }
  }

  private final class ConnectionChannelInitializer extends ChannelInitializer<Channel> {
    @Override
    public void initChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addFirst("decode-frames", new LengthFieldBasedFrameDecoder(
              KuduRpc.MAX_RPC_SIZE,
              0, // length comes at offset 0
              4, // length prefix is 4 bytes long
              0, // no "length adjustment"
              4 /* strip the length prefix */));
      pipeline.addLast("decode-inbound", new CallResponse.Decoder());
      pipeline.addLast("encode-outbound", new RpcOutboundMessage.Encoder());
      // Add a socket read timeout handler to function as a timeout for negotiation.
      // The handler will be removed once the connection is negotiated.
      pipeline.addLast(NEGOTIATION_TIMEOUT_HANDLER,
              new ReadTimeoutHandler(negotiationTimeoutMs, TimeUnit.MILLISECONDS));
      pipeline.addLast("kudu-handler", Connection.this);
    }
  }
}
