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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.WireProtocol;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.client.Negotiator.Result;
import org.apache.kudu.master.Master;
import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

/**
 * Stateful handler that manages a connection to a specific TabletServer.
 * <p>
 * This handler manages the RPC IDs, the serialization and de-serialization of
 * RPC requests and responses, and keeps track of the RPC in flights for which
 * a response is currently awaited, as well as temporarily buffered RPCs that
 * are awaiting to be sent to the network.
 * <p>
 * This class needs careful synchronization. It's a non-sharable handler,
 * meaning there is one instance of it per Netty {@link Channel} and each
 * instance is only used by one Netty IO thread at a time.  At the same time,
 * {@link AsyncKuduClient} calls methods of this class from random threads at
 * random times. The bottom line is that any data only used in the Netty IO
 * threads doesn't require synchronization, everything else does.
 * <p>
 * Acquiring the monitor on an object of this class will prevent it from
 * accepting write requests as well as buffering requests if the underlying
 * channel isn't connected.
 */
@InterfaceAudience.Private
public class TabletClient extends SimpleChannelUpstreamHandler {

  public static final Logger LOG = LoggerFactory.getLogger(TabletClient.class);

  public static final byte RPC_CURRENT_VERSION = 9;
  /** Initial header sent by the client upon connection establishment */
  private static final byte[] CONNECTION_HEADER = new byte[] { 'h', 'r', 'p', 'c',
      RPC_CURRENT_VERSION,     // RPC version.
      0,
      0
  };

  /**
   * The channel we're connected to. This is set very soon after construction
   * before any other events can arrive.
   */
  private Channel chan;

  /** Lock for several of the below fields. */
  private final ReentrantLock lock = new ReentrantLock();

  /**
   * RPCs which are waiting to be sent once a connection has been
   * established.
   *
   * This is non-null until the connection is connected, at which point
   * any pending RPCs will be sent, and the variable set to null.
   */
  @GuardedBy("lock")
  ArrayList<KuduRpc<?>> pendingRpcs = Lists.newArrayList();

  /**
   * A monotonically increasing counter for RPC IDs.
   */
  @GuardedBy("lock")
  private int nextCallId = 0;

  private static enum State {
    NEGOTIATING,
    ALIVE,
    DISCONNECTED
  }

  @GuardedBy("lock")
  private State state = State.NEGOTIATING;

  @GuardedBy("lock")
  private HashMap<Integer, KuduRpc<?>> rpcsInflight = new HashMap<>();

  @GuardedBy("lock")
  private Result negotiationResult;

  private final AsyncKuduClient kuduClient;

  private final long socketReadTimeoutMs;

  private final RequestTracker requestTracker;

  private final ServerInfo serverInfo;

  public TabletClient(AsyncKuduClient client, ServerInfo serverInfo) {
    this.kuduClient = client;
    this.socketReadTimeoutMs = client.getDefaultSocketReadTimeoutMs();
    this.requestTracker = client.getRequestTracker();
    this.serverInfo = serverInfo;
  }

  <R> void sendRpc(KuduRpc<R> rpc) {
    Preconditions.checkArgument(rpc.hasDeferred());
    rpc.addTrace(
        new RpcTraceFrame.RpcTraceFrameBuilder(
            rpc.method(),
            RpcTraceFrame.Action.SEND_TO_SERVER)
            .serverInfo(serverInfo)
            .build());

    if (!rpc.deadlineTracker.hasDeadline()) {
      LOG.warn(getPeerUuidLoggingString() + " sending an rpc without a timeout " + rpc);
    }

    // Serialize the request outside the lock.
    Message req;
    try {
      req = rpc.createRequestPB();
    } catch (Exception e) {
      LOG.error("Uncaught exception while constructing RPC request: " + rpc, e);
      rpc.errback(e);  // Make the RPC fail with the exception.
      return;
    }

    lock.lock();
    boolean needsUnlock = true;
    try {
      // If we are disconnected, immediately fail the RPC
      if (state == State.DISCONNECTED) {
        lock.unlock();
        needsUnlock = false;
        Status statusNetworkError =
            Status.NetworkError(getPeerUuidLoggingString() + "Connection reset");
        failOrRetryRpc(rpc, new RecoverableException(statusNetworkError));
        return;
      }

      // We're still negotiating -- just add it to the pending list
      // which will be sent when the negotiation either completes or
      // fails.
      if (state == State.NEGOTIATING) {
        pendingRpcs.add(rpc);
        return;
      }

      // We are alive, in which case we must have a channel.
      assert state == State.ALIVE;
      assert chan != null;

      // Check that the server supports feature flags, if our RPC uses them.
      if (!rpc.getRequiredFeatures().isEmpty() &&
          !negotiationResult.serverFeatures.contains(
              RpcHeader.RpcFeatureFlag.APPLICATION_FEATURE_FLAGS)) {
        // We don't want to call out of this class while holding the lock.
        lock.unlock();
        needsUnlock = false;

        Status statusNotSupported = Status.NotSupported("the server does not support the" +
            "APPLICATION_FEATURE_FLAGS RPC feature");
        rpc.errback(new NonRecoverableException(statusNotSupported));
        return;
      }

      // Assign the call ID and write it to the wire.
      sendCallToWire(rpc, req);
    } finally {
      if (needsUnlock) {
        lock.unlock();
      }
    }
  }

  @GuardedBy("lock")
  private <R> void sendCallToWire(final KuduRpc<R> rpc, Message reqPB) {
    assert lock.isHeldByCurrentThread();
    assert state == State.ALIVE;
    assert chan != null;

    int callId = nextCallId++;

    final RpcHeader.RequestHeader.Builder headerBuilder = RpcHeader.RequestHeader.newBuilder()
        .addAllRequiredFeatureFlags(rpc.getRequiredFeatures())
        .setCallId(callId)
        .setRemoteMethod(
            RpcHeader.RemoteMethodPB.newBuilder()
            .setServiceName(rpc.serviceName())
            .setMethodName(rpc.method()));

    // If any timeout is set, find the lowest non-zero one, since this will be the deadline that
    // the server must respect.
    if (rpc.deadlineTracker.hasDeadline() || socketReadTimeoutMs > 0) {
      long millisBeforeDeadline = Long.MAX_VALUE;
      if (rpc.deadlineTracker.hasDeadline()) {
        millisBeforeDeadline = rpc.deadlineTracker.getMillisBeforeDeadline();
      }

      long localRpcTimeoutMs = Long.MAX_VALUE;
      if (socketReadTimeoutMs > 0) {
        localRpcTimeoutMs = socketReadTimeoutMs;
      }

      headerBuilder.setTimeoutMillis((int) Math.min(millisBeforeDeadline, localRpcTimeoutMs));
    }

    if (rpc.isRequestTracked()) {
      RpcHeader.RequestIdPB.Builder requestIdBuilder = RpcHeader.RequestIdPB.newBuilder();
      if (rpc.getSequenceId() == RequestTracker.NO_SEQ_NO) {
        rpc.setSequenceId(requestTracker.newSeqNo());
      }
      requestIdBuilder.setClientId(requestTracker.getClientId());
      requestIdBuilder.setSeqNo(rpc.getSequenceId());
      requestIdBuilder.setAttemptNo(rpc.attempt);
      requestIdBuilder.setFirstIncompleteSeqNo(requestTracker.firstIncomplete());
      headerBuilder.setRequestId(requestIdBuilder);
    }

    final KuduRpc<?> oldrpc = rpcsInflight.put(callId, rpc);
    if (oldrpc != null) {
      final String wtf = getPeerUuidLoggingString() +
          "Unexpected state: there was already an RPC in flight with" +
          " callId=" + callId + ": " + oldrpc +
          ".  This happened when sending out: " + rpc;
      LOG.error(wtf);
      Status statusIllegalState = Status.IllegalState(wtf);
      // Make it fail. This isn't an expected failure mode.
      oldrpc.errback(new NonRecoverableException(statusIllegalState));
      return;
    }

    RpcOutboundMessage outbound = new RpcOutboundMessage(headerBuilder, reqPB);
    Channels.write(chan, outbound);
  }

  /**
   * Triggers the channel to be disconnected, which will asynchronously cause all
   * pending and in-flight RPCs to be failed. This method is idempotent.
   */
  @VisibleForTesting
  ChannelFuture disconnect() {
    // 'chan' should never be null, because as soon as this object is created, it's
    // added to a ChannelPipeline, which synchronously fires the channelOpen()
    // event.
    Preconditions.checkNotNull(chan);
    return Channels.disconnect(chan);
  }

  /**
   * Forcefully shuts down the connection to this tablet server and fails all the outstanding RPCs.
   * Only use when shutting down a client.
   * @return deferred object to use to track the shutting down of this connection
   */
  public Deferred<Void> shutdown() {
    ChannelFuture disconnectFuture = disconnect();
    final Deferred<Void> d = new Deferred<Void>();
    disconnectFuture.addListener(new ChannelFutureListener() {
      public void operationComplete(final ChannelFuture future) {
        if (future.isSuccess()) {
          d.callback(null);
          return;
        }
        final Throwable t = future.getCause();
        if (t instanceof Exception) {
          d.callback(t);
        } else {
          // Wrap the Throwable because Deferred doesn't handle Throwables,
          // it only uses Exception.
          Status statusIllegalState = Status.IllegalState("Failed to shutdown: " +
              TabletClient.this);
          d.callback(new NonRecoverableException(statusIllegalState, t));
        }
      }
    });
    return d;
  }

  /**
   * The reason we are suppressing the unchecked conversions is because the KuduRpc is coming
   * from a collection that has RPCs with different generics, and there's no way to get "decoded"
   * casted correctly. The best we can do is to rely on the RPC to decode correctly,
   * and to not pass an Exception in the callback.
   */
  @Override
  @SuppressWarnings("unchecked")
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt) throws Exception {
    Object m = evt.getMessage();
    if (m instanceof Negotiator.Result) {
      ArrayList<KuduRpc<?>> queuedRpcs;
      lock.lock();
      try {
        assert chan != null;
        this.negotiationResult = (Result) m;
        state = State.ALIVE;
        queuedRpcs = pendingRpcs;
        pendingRpcs = null;
      } finally {
        lock.unlock();
      }
      // Send the queued RPCs after dropping the lock, so we don't end up calling
      // their callbacks/errbacks with the lock held.
      sendQueuedRpcs(queuedRpcs);
      return;
    }
    if (!(m instanceof CallResponse)) {
      ctx.sendUpstream(evt);
      return;
    }
    CallResponse response = (CallResponse)m;
    final long start = System.nanoTime();

    RpcHeader.ResponseHeader header = response.getHeader();
    if (!header.hasCallId()) {
      final int size = response.getTotalResponseSize();
      final String msg = getPeerUuidLoggingString() + "RPC response (size: " + size + ") doesn't" +
          " have a call ID: " + header;
      LOG.error(msg);
      Status statusIncomplete = Status.Incomplete(msg);
      throw new NonRecoverableException(statusIncomplete);
    }
    final int rpcid = header.getCallId();

    KuduRpc<Object> rpc;
    lock.lock();
    try {
      rpc = (KuduRpc<Object>) rpcsInflight.remove(rpcid);
    } finally {
      lock.unlock();
    }

    if (rpc == null) {
      final String msg = getPeerUuidLoggingString() + "Invalid rpcid: " + rpcid;
      LOG.error(msg);
      // If we get a bad RPC ID back, we are probably somehow misaligned from
      // the server. So, we disconnect the connection.
      throw new NonRecoverableException(Status.IllegalState(msg));
    }

    // Start building the trace, we'll finish it as we parse the response.
    RpcTraceFrame.RpcTraceFrameBuilder traceBuilder =
        new RpcTraceFrame.RpcTraceFrameBuilder(
            rpc.method(),
            RpcTraceFrame.Action.RECEIVE_FROM_SERVER)
            .serverInfo(serverInfo);

    Pair<Object, Object> decoded = null;
    KuduException exception = null;
    Status retryableHeaderError = Status.OK();
    if (header.hasIsError() && header.getIsError()) {
      RpcHeader.ErrorStatusPB.Builder errorBuilder = RpcHeader.ErrorStatusPB.newBuilder();
      KuduRpc.readProtobuf(response.getPBMessage(), errorBuilder);
      RpcHeader.ErrorStatusPB error = errorBuilder.build();
      if (error.getCode().equals(RpcHeader.ErrorStatusPB.RpcErrorCodePB.ERROR_SERVER_TOO_BUSY)) {
        // We can't return right away, we still need to remove ourselves from 'rpcsInflight', so we
        // populate 'retryableHeaderError'.
        retryableHeaderError = Status.ServiceUnavailable(error.getMessage());
      } else {
        String message = getPeerUuidLoggingString() +
            "Tablet server sent error " + error.getMessage();
        Status status = Status.RemoteError(message);
        exception = new RpcRemoteException(status, error);
        LOG.error(message); // can be useful
      }
    } else {
      try {
        decoded = rpc.deserialize(response, this.serverInfo.getUuid());
      } catch (KuduException ex) {
        exception = ex;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(getPeerUuidLoggingString() + " received RPC response: " +
          "rpcId=" + rpcid +
          ", response size=" + response.getTotalResponseSize() +
          ", rpc=" + rpc);
    }

    // This check is specifically for the ERROR_SERVER_TOO_BUSY case above.
    if (!retryableHeaderError.ok()) {
      rpc.addTrace(traceBuilder.callStatus(retryableHeaderError).build());
      kuduClient.handleRetryableError(rpc, new RecoverableException(retryableHeaderError));
      return;
    }

    // We can get this Message from within the RPC's expected type,
    // so convert it into an exception and nullify decoded so that we use the errback route.
    // Have to do it for both TS and Master errors.
    if (decoded != null) {
      if (decoded.getSecond() instanceof Tserver.TabletServerErrorPB) {
        Tserver.TabletServerErrorPB error = (Tserver.TabletServerErrorPB) decoded.getSecond();
        exception = dispatchTSErrorOrReturnException(rpc, error, traceBuilder);
        if (exception == null) {
          // It was taken care of.
          return;
        } else {
          // We're going to errback.
          decoded = null;
        }

      } else if (decoded.getSecond() instanceof Master.MasterErrorPB) {
        Master.MasterErrorPB error = (Master.MasterErrorPB) decoded.getSecond();
        exception = dispatchMasterErrorOrReturnException(rpc, error, traceBuilder);
        if (exception == null) {
          // Exception was taken care of.
          return;
        } else {
          decoded = null;
        }
      }
    }

    try {
      if (decoded != null) {
        assert !(decoded.getFirst() instanceof Exception);
        if (kuduClient.isStatisticsEnabled()) {
          rpc.updateStatistics(kuduClient.getStatistics(), decoded.getFirst());
        }
        rpc.addTrace(traceBuilder.callStatus(Status.OK()).build());
        rpc.callback(decoded.getFirst());
      } else {
        if (kuduClient.isStatisticsEnabled()) {
          rpc.updateStatistics(kuduClient.getStatistics(), null);
        }
        rpc.addTrace(traceBuilder.callStatus(exception.getStatus()).build());
        rpc.errback(exception);
      }
    } catch (Exception e) {
      LOG.debug(getPeerUuidLoggingString() + "Unexpected exception while handling RPC #" + rpcid +
          ", rpc=" + rpc, e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("------------------<< LEAVING  DECODE <<------------------" +
          " time elapsed: " + ((System.nanoTime() - start) / 1000) + "us");
    }
  }

  /**
   * Takes care of a few kinds of TS errors that we handle differently, like tablets or leaders
   * moving. Builds and returns an exception if we don't know what to do with it.
   * @param rpc the original RPC call that triggered the error
   * @param error the error the TS sent
   * @return an exception if we couldn't dispatch the error, or null
   */
  private KuduException dispatchTSErrorOrReturnException(
      KuduRpc<?> rpc, Tserver.TabletServerErrorPB error,
      RpcTraceFrame.RpcTraceFrameBuilder traceBuilder) {
    WireProtocol.AppStatusPB.ErrorCode code = error.getStatus().getCode();
    Status status = Status.fromTabletServerErrorPB(error);
    if (error.getCode() == Tserver.TabletServerErrorPB.Code.TABLET_NOT_FOUND) {
      kuduClient.handleTabletNotFound(rpc, new RecoverableException(status), this);
      // we're not calling rpc.callback() so we rely on the client to retry that RPC
    } else if (code == WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE) {
      kuduClient.handleRetryableError(rpc, new RecoverableException(status));
      // The following two error codes are an indication that the tablet isn't a leader.
    } else if (code == WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE ||
        code == WireProtocol.AppStatusPB.ErrorCode.ABORTED) {
      kuduClient.handleNotLeader(rpc, new RecoverableException(status), this);
    } else {
      return new NonRecoverableException(status);
    }
    rpc.addTrace(traceBuilder.callStatus(status).build());
    return null;
  }

  /**
   * Provides different handling for various kinds of master errors: re-uses the
   * mechanisms already in place for handling tablet server errors as much as possible.
   * @param rpc the original RPC call that triggered the error
   * @param error the error the master sent
   * @return an exception if we couldn't dispatch the error, or null
   */
  private KuduException dispatchMasterErrorOrReturnException(
      KuduRpc<?> rpc, Master.MasterErrorPB error, RpcTraceFrame.RpcTraceFrameBuilder traceBuilder) {
    WireProtocol.AppStatusPB.ErrorCode code = error.getStatus().getCode();
    Status status = Status.fromMasterErrorPB(error);
    if (error.getCode() == Master.MasterErrorPB.Code.NOT_THE_LEADER) {
      kuduClient.handleNotLeader(rpc, new RecoverableException(status), this);
    } else if (code == WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE) {
      if (rpc instanceof ConnectToMasterRequest) {
        // Special case:
        // We never want to retry this RPC, we only use it to poke masters to learn where the leader
        // is. If the error is truly non recoverable, it'll be handled later.
        return new RecoverableException(status);
      } else {
        // TODO: This is a crutch until we either don't have to retry RPCs going to the
        // same server or use retry policies.
        kuduClient.handleRetryableError(rpc, new RecoverableException(status));
      }
    } else {
      return new NonRecoverableException(status);
    }
    rpc.addTrace(traceBuilder.callStatus(status).build());
    return null;
  }

  /**
   * Tells whether or not this handler should be used.
   * <p>
   * @return true if this instance can be used, else false if this handler is known to have been
   * disconnected from the server and sending an RPC (via {@link #sendRpc(KuduRpc)}) will be
   * retried in the client right away
   */
  public boolean isAlive() {
    lock.lock();
    try {
      return state == State.ALIVE || state == State.NEGOTIATING;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
      throws Exception {
    this.chan = e.getChannel();
    super.channelOpen(ctx, e);
  }

  @Override
  public void channelConnected(final ChannelHandlerContext ctx,
                               final ChannelStateEvent e) {
    assert chan != null;
    Channels.write(chan, ChannelBuffers.wrappedBuffer(CONNECTION_HEADER));
    Negotiator negotiator = new Negotiator(serverInfo.getHostname(),
        kuduClient.getSecurityContext());
    ctx.getPipeline().addBefore(ctx.getName(), "negotiation", negotiator);
    negotiator.sendHello(chan);
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx,
                             final ChannelEvent e) throws Exception {
    if (LOG.isTraceEnabled()) {
      LOG.trace(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelDisconnected(final ChannelHandlerContext ctx,
                                  final ChannelStateEvent e) throws Exception {
    super.channelDisconnected(ctx, e);  // Let the ReplayingDecoder cleanup.
    cleanup("Connection disconnected");
  }

  @Override
  public void channelClosed(final ChannelHandlerContext ctx,
                            final ChannelStateEvent e) throws Exception {
    super.channelClosed(ctx, e);
    cleanup("Connection closed");
  }

  /**
   * Cleans up any outstanding or lingering RPC (used when shutting down).
   * <p>
   * All RPCs in flight will fail with a {@link RecoverableException} and
   * all edits buffered will be re-scheduled.
   *
   * @param errorMessage string to describe the cause of cleanup
   */
  private void cleanup(final String errorMessage) {
    final ArrayList<KuduRpc<?>> rpcsToFail = Lists.newArrayList();

    lock.lock();
    try {
      // Cleanup can be called multiple times, but we only want to run it once.
      if (state == State.DISCONNECTED) {
        assert pendingRpcs == null;
        return;
      }
      state = State.DISCONNECTED;

      // In case we were negotiating, we need to fail any that were waiting
      // for negotiation to complete.
      if (pendingRpcs != null) {
        rpcsToFail.addAll(pendingRpcs);
      }
      pendingRpcs = null;

      // Similarly, we need to fail any that were already sent and in-flight.
      rpcsToFail.addAll(rpcsInflight.values());
      rpcsInflight = null;
    } finally {
      lock.unlock();
    }
    Status statusNetworkError = Status.NetworkError(getPeerUuidLoggingString() +
        (errorMessage == null ? "Connection reset" : errorMessage));
    RecoverableException exception = new RecoverableException(statusNetworkError);

    failOrRetryRpcs(rpcsToFail, exception);
  }

  /**
   * Retry all the given RPCs.
   * @param rpcs a possibly empty but non-{@code null} collection of RPCs to retry or fail
   * @param exception an exception to propagate with the RPCs
   */
  private void failOrRetryRpcs(final Collection<KuduRpc<?>> rpcs,
                               final RecoverableException exception) {
    for (final KuduRpc<?> rpc : rpcs) {
      failOrRetryRpc(rpc, exception);
    }
  }

  /**
   * Retry the given RPC.
   * @param rpc an RPC to retry or fail
   * @param exception an exception to propagate with the RPC
   */
  private void failOrRetryRpc(final KuduRpc<?> rpc,
                              final RecoverableException exception) {
    rpc.addTrace(
        new RpcTraceFrame.RpcTraceFrameBuilder(
            rpc.method(),
            RpcTraceFrame.Action.RECEIVE_FROM_SERVER)
            .serverInfo(serverInfo)
            .callStatus(exception.getStatus())
            .build());

    RemoteTablet tablet = rpc.getTablet();
    // Note As of the time of writing (03/11/16), a null tablet doesn't make sense, if we see a null
    // tablet it's because we didn't set it properly before calling sendRpc().
    if (tablet == null) {  // Can't retry, dunno where this RPC should go.
      rpc.errback(exception);
    } else {
      kuduClient.handleTabletNotFound(rpc, exception, this);
    }
  }


  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent event) {
    final Throwable e = event.getCause();
    final Channel c = event.getChannel();

    if (e instanceof RejectedExecutionException) {
      LOG.warn(getPeerUuidLoggingString() + "RPC rejected by the executor," +
          " ignore this if we're shutting down", e);
    } else if (e instanceof ReadTimeoutException) {
      LOG.debug(getPeerUuidLoggingString() + "Encountered a read timeout, will close the channel");
    } else {
      LOG.error(getPeerUuidLoggingString() + "Unexpected exception from downstream on " + c, e);
    }
    if (c.isOpen()) {
      Channels.close(c);        // Will trigger channelClosed(), which will cleanup()
    } else {                    // else: presumably a connection timeout.
      cleanup(e.getMessage());  // => need to cleanup() from here directly.
    }
  }


  /**
   * Sends the queued RPCs to the server, once we're connected to it.
   * This gets called after {@link #channelConnected}, once we were able to
   * handshake with the server
   *
   * Must *not* be called with 'lock' held.
   */
  private void sendQueuedRpcs(List<KuduRpc<?>> rpcs) {
    assert !lock.isHeldByCurrentThread();
    for (final KuduRpc<?> rpc : rpcs) {
      LOG.debug(getPeerUuidLoggingString() + "Executing RPC queued: " + rpc);
      sendRpc(rpc);
    }
  }

  private String getPeerUuidLoggingString() {
    return "[Peer " + serverInfo.getUuid() + "] ";
  }

  ServerInfo getServerInfo() {
    return serverInfo;
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("TabletClient@")
        .append(hashCode())
        .append("(chan=")
        .append(chan)
        .append(", uuid=")
        .append(serverInfo.getUuid())
        .append(", #pending_rpcs=");
    int npendingRpcs;
    int nInFlight;
    lock.lock();
    try {
      npendingRpcs = pendingRpcs == null ? 0 : pendingRpcs.size();
      nInFlight = rpcsInflight == null ? 0 : rpcsInflight.size();
    } finally {
      lock.unlock();
    }
    buf.append(npendingRpcs);
    buf.append(", #rpcs_inflight=")
        .append(nInFlight)
        .append(')');
    return buf.toString();
  }

}
