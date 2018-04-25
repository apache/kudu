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

import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.stumbleupon.async.Callback;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.WireProtocol;
import org.apache.kudu.master.Master;
import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;


/**
 * This is a 'stateless' helper to send RPCs to a Kudu server ('stateless' in the sense that it
 * does not keep any state itself besides the references to the {@link AsyncKuduClient} and
 * {@link Connection} objects.
 * <p>
 * This helper serializes and de-serializes RPC requests and responses and provides handy
 * methods to send the serialized RPC to the underlying {@link Connection} and to handle the
 * response from it.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class RpcProxy {

  private static final Logger LOG = LoggerFactory.getLogger(RpcProxy.class);

  /** The reference to the top-level Kudu client object. */
  @Nonnull
  private final AsyncKuduClient client;

  /** The reference to the object representing connection to the target server. */
  @Nonnull
  private final Connection connection;

  /**
   * Construct RpcProxy object.
   *
   * @param client top-level Kudu client object
   * @param connection the connection associated with the target Kudu server
   */
  RpcProxy(AsyncKuduClient client, Connection connection) {
    this.client = Preconditions.checkNotNull(client);
    this.connection = Preconditions.checkNotNull(connection);
  }

  /**
   * Send the specified RPC using the connection to the Kudu server.
   *
   * @param <R> type of the RPC
   * @param rpc the RPC to send over the connection
   */
  <R> void sendRpc(final KuduRpc<R> rpc) {
    sendRpc(client, connection, rpc);
  }

  /**
   * Send the specified RPC using the connection to the Kudu server.
   *
   * @param <R> type of the RPC
   * @param client client object to handle response and sending retries, if needed
   * @param connection connection to send the request over
   * @param rpc the RPC to send over the connection
   */
  static <R> void sendRpc(final AsyncKuduClient client,
                          final Connection connection,
                          final KuduRpc<R> rpc) {
    try {
      if (!rpc.getRequiredFeatures().isEmpty()) {
        // An extra optimization: when the peer's features are already known, check that the server
        // supports feature flags, if those are required.
        Set<RpcFeatureFlag> features = connection.getPeerFeatures();
        if (features != null &&
            !features.contains(RpcHeader.RpcFeatureFlag.APPLICATION_FEATURE_FLAGS)) {
          throw new NonRecoverableException(Status.NotSupported(
              "the server does not support the APPLICATION_FEATURE_FLAGS RPC feature"));
        }
      }

      Preconditions.checkArgument(rpc.hasDeferred());
      rpc.addTrace(
          new RpcTraceFrame.RpcTraceFrameBuilder(
              rpc.method(),
              RpcTraceFrame.Action.SEND_TO_SERVER)
              .serverInfo(connection.getServerInfo())
              .build());

      if (!rpc.deadlineTracker.hasDeadline()) {
        LOG.warn("{} sending RPC with no timeout {}", connection.getLogPrefix(), rpc);
      }
      connection.enqueueMessage(rpcToMessage(client, rpc),
          new Callback<Void, Connection.CallResponseInfo>() {
            @Override
            public Void call(Connection.CallResponseInfo callResponseInfo) throws Exception {
              try {
                responseReceived(client, connection, rpc,
                    callResponseInfo.response, callResponseInfo.exception);
              } catch (Exception e) {
                rpc.errback(e);
              }
              return null;
            }
          });
    } catch (RecoverableException e) {
      // This is to handle RecoverableException(Status.IllegalState()) from
      // Connection.enqueueMessage() if the connection turned into the TERMINATED state.
      client.handleRetryableError(rpc, e);
    } catch (Exception e) {
      rpc.errback(e);
    }
  }

  /**
   * Build {@link RpcOutboundMessage} out from {@link KuduRpc}.
   *
   * @param <R> type of the RPC
   * @param client client object to handle response and sending retries, if needed
   * @param rpc the RPC to convert into outbound message
   * @return the result {@link RpcOutboundMessage}
   */
  private static <R> RpcOutboundMessage rpcToMessage(
      final AsyncKuduClient client,
      final KuduRpc<R> rpc) {
    // The callId is set by Connection.enqueueMessage().
    final RpcHeader.RequestHeader.Builder headerBuilder = RpcHeader.RequestHeader.newBuilder()
        .addAllRequiredFeatureFlags(rpc.getRequiredFeatures())
        .setRemoteMethod(
            RpcHeader.RemoteMethodPB.newBuilder()
                .setServiceName(rpc.serviceName())
                .setMethodName(rpc.method()));
    final Message reqPB = rpc.createRequestPB();

    if (rpc.deadlineTracker.hasDeadline()) {
      headerBuilder.setTimeoutMillis((int) rpc.deadlineTracker.getMillisBeforeDeadline());
    }

    if (rpc.isRequestTracked()) {
      RpcHeader.RequestIdPB.Builder requestIdBuilder = RpcHeader.RequestIdPB.newBuilder();
      final RequestTracker requestTracker = client.getRequestTracker();
      if (rpc.getSequenceId() == RequestTracker.NO_SEQ_NO) {
        rpc.setSequenceId(requestTracker.newSeqNo());
      }
      requestIdBuilder.setClientId(requestTracker.getClientId());
      requestIdBuilder.setSeqNo(rpc.getSequenceId());
      requestIdBuilder.setAttemptNo(rpc.attempt);
      requestIdBuilder.setFirstIncompleteSeqNo(requestTracker.firstIncomplete());
      headerBuilder.setRequestId(requestIdBuilder);
    }

    return new RpcOutboundMessage(headerBuilder, reqPB);
  }

  private static <R> void responseReceived(AsyncKuduClient client,
                                           Connection connection,
                                           final KuduRpc<R> rpc,
                                           CallResponse response,
                                           KuduException ex) {
    final long start = System.nanoTime();
    if (LOG.isTraceEnabled()) {
      if (response == null) {
        LOG.trace("{} received null response for RPC {}",
            connection.getLogPrefix(), rpc);
      } else {
        RpcHeader.ResponseHeader header = response.getHeader();
        LOG.trace("{} received response with rpcId {}, size {} for RPC {}",
            connection.getLogPrefix(), header.getCallId(),
            response.getTotalResponseSize(), rpc);
      }
    }

    RpcTraceFrame.RpcTraceFrameBuilder traceBuilder = new RpcTraceFrame.RpcTraceFrameBuilder(
        rpc.method(), RpcTraceFrame.Action.RECEIVE_FROM_SERVER).serverInfo(
            connection.getServerInfo());
    if (ex != null) {
      if (ex instanceof InvalidAuthnTokenException) {
        client.handleInvalidToken(rpc);
        return;
      }
      if (ex instanceof RecoverableException) {
        // This check is specifically for the ERROR_SERVER_TOO_BUSY, ERROR_UNAVAILABLE and alike.
        failOrRetryRpc(client, connection, rpc, (RecoverableException) ex);
        return;
      }
      rpc.addTrace(traceBuilder.callStatus(ex.getStatus()).build());
      rpc.errback(ex);
      return;
    }

    Pair<R, Object> decoded = null;
    KuduException exception = null;
    try {
      decoded = rpc.deserialize(response, connection.getServerInfo().getUuid());
    } catch (KuduException e) {
      exception = e;
    } catch (Exception e) {
      rpc.addTrace(traceBuilder.build());
      rpc.errback(e);
      return;
    }

    // We can get this Message from within the RPC's expected type,
    // so convert it into an exception and nullify decoded so that we use the errback route.
    // Have to do it for both TS and Master errors.
    if (decoded != null) {
      if (decoded.getSecond() instanceof Tserver.TabletServerErrorPB) {
        Tserver.TabletServerErrorPB error = (Tserver.TabletServerErrorPB) decoded.getSecond();
        exception = dispatchTSError(client, connection, rpc, error, traceBuilder);
        if (exception == null) {
          // It was taken care of.
          return;
        } else {
          // We're going to errback.
          decoded = null;
        }
      } else if (decoded.getSecond() instanceof Master.MasterErrorPB) {
        Master.MasterErrorPB error = (Master.MasterErrorPB) decoded.getSecond();
        exception = dispatchMasterError(client, connection, rpc, error, traceBuilder);
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
        Preconditions.checkState(!(decoded.getFirst() instanceof Exception));
        if (client.isStatisticsEnabled()) {
          rpc.updateStatistics(client.getStatistics(), decoded.getFirst());
        }
        rpc.addTrace(traceBuilder.callStatus(Status.OK()).build());
        rpc.callback(decoded.getFirst());
      } else {
        if (client.isStatisticsEnabled()) {
          rpc.updateStatistics(client.getStatistics(), null);
        }
        rpc.addTrace(traceBuilder.callStatus(exception.getStatus()).build());
        rpc.errback(exception);
      }
    } catch (Exception e) {
      RpcHeader.ResponseHeader header = response.getHeader();
      Preconditions.checkNotNull(header);
      LOG.debug("{} unexpected exception {} while handling call: callId {}, RPC {}",
          connection.getLogPrefix(), e, header.getCallId(), rpc);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("------------------<< LEAVING  DECODE <<------------------ time elapsed: {} us",
          ((System.nanoTime() - start) / 1000));
    }
  }

  /**
   * Takes care of a few kinds of TS errors that we handle differently, like tablets or leaders
   * moving. Builds and returns an exception if we don't know what to do with it.
   *
   * @param client client object to handle response and sending retries, if needed
   * @param connection connection to send the request over
   * @param rpc   the original RPC call that triggered the error
   * @param error the error the TS sent
   * @param tracer RPC trace builder to add a record on the error into the call history
   * @return an exception if we couldn't dispatch the error, or null
   */
  private static KuduException dispatchTSError(AsyncKuduClient client,
                                               Connection connection,
                                               KuduRpc<?> rpc,
                                               Tserver.TabletServerErrorPB error,
                                               RpcTraceFrame.RpcTraceFrameBuilder tracer) {
    Tserver.TabletServerErrorPB.Code errCode = error.getCode();
    WireProtocol.AppStatusPB.ErrorCode errStatusCode = error.getStatus().getCode();
    Status status = Status.fromTabletServerErrorPB(error);
    if (errCode == Tserver.TabletServerErrorPB.Code.TABLET_NOT_FOUND) {
      client.handleTabletNotFound(
          rpc, new RecoverableException(status), connection.getServerInfo());
      // we're not calling rpc.callback() so we rely on the client to retry that RPC
    } else if (errCode == Tserver.TabletServerErrorPB.Code.TABLET_NOT_RUNNING ||
        errStatusCode == WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE) {
      client.handleRetryableError(rpc, new RecoverableException(status));
      // The following two error codes are an indication that the tablet isn't a leader.
    } else if (errStatusCode == WireProtocol.AppStatusPB.ErrorCode.ILLEGAL_STATE ||
        errStatusCode == WireProtocol.AppStatusPB.ErrorCode.ABORTED) {
      client.handleNotLeader(rpc, new RecoverableException(status), connection.getServerInfo());
    } else {
      return new NonRecoverableException(status);
    }
    rpc.addTrace(tracer.callStatus(status).build());
    return null;
  }

  /**
   * Provides different handling for various kinds of master errors: re-uses the
   * mechanisms already in place for handling tablet server errors as much as possible.
   *
   * @param client client object to handle response and sending retries, if needed
   * @param connection connection to send the request over
   * @param rpc   the original RPC call that triggered the error
   * @param error the error the master sent
   * @param tracer RPC trace builder to add a record on the error into the call history
   * @return an exception if we couldn't dispatch the error, or null
   */
  private static KuduException dispatchMasterError(AsyncKuduClient client,
                                                   Connection connection,
                                                   KuduRpc<?> rpc,
                                                   Master.MasterErrorPB error,
                                                   RpcTraceFrame.RpcTraceFrameBuilder tracer) {

    WireProtocol.AppStatusPB.ErrorCode code = error.getStatus().getCode();
    Status status = Status.fromMasterErrorPB(error);
    if (error.getCode() == Master.MasterErrorPB.Code.NOT_THE_LEADER) {
      client.handleNotLeader(rpc, new RecoverableException(status), connection.getServerInfo());
    } else if (code == WireProtocol.AppStatusPB.ErrorCode.SERVICE_UNAVAILABLE) {
      if (rpc instanceof ConnectToMasterRequest) {
        // Special case:
        // We never want to retry this RPC, we only use it to poke masters to learn where the leader
        // is. If the error is truly non recoverable, it'll be handled later.
        return new RecoverableException(status);
      } else {
        // TODO: This is a crutch until we either don't have to retry RPCs going to the
        // same server or use retry policies.
        client.handleRetryableError(rpc, new RecoverableException(status));
      }
    } else {
      return new NonRecoverableException(status);
    }
    rpc.addTrace(tracer.callStatus(status).build());
    return null;
  }

  /**
   * Retry the given RPC.
   *
   * @param client client object to handle response and sending retries, if needed
   * @param connection connection to send the request over
   * @param rpc       an RPC to retry or fail
   * @param exception an exception to propagate with the RPC
   */
  private static void failOrRetryRpc(AsyncKuduClient client,
                                     Connection connection,
                                     final KuduRpc<?> rpc,
                                     final RecoverableException exception) {
    rpc.addTrace(new RpcTraceFrame.RpcTraceFrameBuilder(rpc.method(),
        RpcTraceFrame.Action.RECEIVE_FROM_SERVER)
        .serverInfo(connection.getServerInfo())
        .callStatus(exception.getStatus())
        .build());

    RemoteTablet tablet = rpc.getTablet();
    // Note As of the time of writing (03/11/16), a null tablet doesn't make sense, if we see a null
    // tablet it's because we didn't set it properly before calling sendRpc().
    if (tablet == null) {  // Can't retry, dunno where this RPC should go.
      rpc.errback(exception);
    } else {
      client.handleTabletNotFound(rpc, exception, connection.getServerInfo());
    }
  }

  /**
   * @return string representation of the object suitable for printing into logs, etc.
   */
  @Override
  public String toString() {
    return "RpcProxy@" + hashCode() + ", connection=" + connection;
  }

  /**
   * @return underlying {@link Connection} object representing TCP connection to the server
   */
  @InterfaceAudience.LimitedPrivate("Test")
  Connection getConnection() {
    return connection;
  }
}
