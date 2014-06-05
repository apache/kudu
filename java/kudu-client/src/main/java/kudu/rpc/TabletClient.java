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

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.master.Master;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import kudu.tserver.Tserver;
import kudu.util.Slice;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

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
 * {@link KuduClient} calls methods of this class from random threads at
 * random times. The bottom line is that any data only used in the Netty IO
 * threads doesn't require synchronization, everything else does.
 * <p>
 * Acquiring the monitor on an object of this class will prevent it from
 * accepting write requests as well as buffering requests if the underlying
 * channel isn't connected.
 */
public class TabletClient extends ReplayingDecoder<VoidEnum> {

  public static final Logger LOG = LoggerFactory.getLogger(TabletClient.class);

  private ArrayList<KuduRpc> pending_rpcs;

  public static final byte RPC_CURRENT_VERSION = 9;
  /** Initial part of the header for 0.95 and up.  */
  private static final byte[] RPC_HEADER = new byte[] { 'h', 'r', 'p', 'c',
      RPC_CURRENT_VERSION,     // RPC version.
      0,
      0
  };
  public static final int CONNECTION_CTX_CALL_ID = -3;

  /**
   * A monotonically increasing counter for RPC IDs.
   * RPCs can be sent out from any thread, so we need an atomic integer.
   * RPC IDs can be arbitrary.  So it's fine if this integer wraps around and
   * becomes negative.  They don't even have to start at 0, but we do it for
   * simplicity and ease of debugging.
   */
  private final AtomicInteger rpcid = new AtomicInteger(-1);

  /**
   * The channel we're connected to.
   * This will be {@code null} while we're not connected to the TabletServer.
   * This attribute is volatile because {@link #shutdown} may access it from a
   * different thread, and because while we connect various user threads will
   * test whether it's {@code null}.  Once we're connected and we know what
   * protocol version the server speaks, we'll set this reference.
   */
  private volatile Channel chan;

  /**
   * Set to {@code true} once we've disconnected from the server.
   * This way, if any thread is still trying to use this client after it's
   * been removed from the caches in the {@link KuduClient}, we will
   * immediately fail / reschedule its requests.
   * <p>
   * Manipulating this value requires synchronizing on `this'.
   */
  private boolean dead = false;

  /**
   * Maps an RPC ID to the in-flight RPC that was given this ID.
   * RPCs can be sent out from any thread, so we need a concurrent map.
   */
  private final ConcurrentHashMap<Integer, KuduRpc> rpcs_inflight =
      new ConcurrentHashMap<Integer, KuduRpc>();

  private final KuduClient kuduClient;

  private SecureRpcHelper secureRpcHelper;

  public TabletClient(KuduClient client) {
    this.kuduClient = client;
  }

  void sendRpc(KuduRpc rpc) {
    if (chan != null) {
      final ChannelBuffer serialized = encode(rpc);
      if (serialized == null) {  // Error during encoding.
        return;  // Stop here.  RPC has been failed already.
      }

      final Channel chan = this.chan;  // Volatile read.
      if (chan != null) {  // Double check if we disconnected during encode().
        Channels.write(chan, serialized);
        return;
      }
    }
    boolean tryagain = false;
    boolean copyOfDead;
    synchronized (this) {
      copyOfDead = this.dead;
      // Check if we got connected while entering this synchronized block.
      if (chan != null) {
        tryagain = true;
      } else if (!copyOfDead) {
        if (pending_rpcs == null) {
          pending_rpcs = new ArrayList<KuduRpc>();
        }
        pending_rpcs.add(rpc);
      }
    }
    if (copyOfDead) {
      if (rpc.getTablet() == null  // Can't retry, dunno where it should go.
          ) {
        rpc.callback(new ConnectionResetException(null));
      } else {
        kuduClient.sendRpcToTablet(rpc);  // Re-schedule the RPC.
      }
      return;
    } else if (tryagain) {
      // This recursion will not lead to a loop because we only get here if we
      // connected while entering the synchronized block above. So when trying
      // a second time,  we will either succeed to send the RPC if we're still
      // connected, or fail through to the code below if we got disconnected
      // in the mean time.
      sendRpc(rpc);
      return;
    }
  }

  private ChannelBuffer encode(final KuduRpc rpc) {
    final int rpcid = this.rpcid.incrementAndGet();
    ChannelBuffer payload;
    final String method = rpc.method();
    try {
      final RpcHeader.RequestHeader.Builder headerBuilder = RpcHeader.RequestHeader.newBuilder()
          .setCallId(rpcid)
          .setMethodName(method);
      if (rpc.deadlineTracker.hasDeadline()) {
        headerBuilder.setTimeoutMillis((int)rpc.deadlineTracker.getMillisBeforeDeadline());
      }
      payload = rpc.serialize(headerBuilder.build());
    } catch (Exception e) {
        LOG.error("Uncaught exception while serializing RPC: " + rpc, e);
        rpc.callback(e);  // Make the RPC fail with the exception.
        return null;
    }
    final KuduRpc oldrpc = rpcs_inflight.put(rpcid, rpc);
    if (oldrpc != null) {
      final String wtf = "WTF?  There was already an RPC in flight with"
          + " rpcid=" + rpcid + ": " + oldrpc
          + ".  This happened when sending out: " + rpc;
      LOG.error(wtf);
      // Make it fail.  This isn't an expected failure mode.
      oldrpc.callback(new NonRecoverableException(wtf));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug(chan + " Sending RPC #" + rpcid + ", payload=" + payload + ' '
          + Bytes.pretty(payload));
    }

    payload = secureRpcHelper.wrap(payload);

    return payload;
  }

  public Deferred shutdown() {
    final class RetryShutdown<T> implements Callback<Deferred<Object>, T> {
      private final int nrpcs;
      RetryShutdown(final int nrpcs) {
        this.nrpcs = nrpcs;
      }
      public Deferred<Object> call(final T ignored) {
        return shutdown();
      }
      public String toString() {
        return "wait until " + nrpcs + " RPCs complete";
      }
    };

    // First, check whether we have RPCs in flight.  If we do, we need to wait
    // until they complete.
    {
      final ArrayList<Deferred<Object>> inflight = getInflightRpcs();
      final int size = inflight.size();
      if (size > 0) {
        return Deferred.group(inflight)
            .addCallbackDeferring(new RetryShutdown<ArrayList<Object>>(size));
      }
    }

    {
      final ArrayList<Deferred<Object>> pending = getPendingRpcs();
      if (pending != null) {
        return Deferred.group(pending).addCallbackDeferring(
            new RetryShutdown<ArrayList<Object>>(pending.size()));
      }
    }

    final Channel chancopy = chan;
    if (chancopy == null) {
      return Deferred.fromResult(null);
    }
    if (chancopy.isConnected()) {
      Channels.disconnect(chancopy);   // ... this is going to set it to null.
      // At this point, all in-flight RPCs are going to be failed.
    }
    if (chancopy.isBound()) {
      Channels.unbind(chancopy);
    }
    // It's OK to call close() on a Channel if it's already closed.
    final ChannelFuture future = Channels.close(chancopy);
    // Now wrap the ChannelFuture in a Deferred.
    final Deferred<Object> d = new Deferred<Object>();
    // Opportunistically check if it's already completed successfully.
    if (future.isSuccess()) {
      d.callback(null);
    } else {
      // If we get here, either the future failed (yeah, that sounds weird)
      // or the future hasn't completed yet (heh).
      future.addListener(new ChannelFutureListener() {
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
            d.callback(new NonRecoverableException("Failed to shutdown: "
                + TabletClient.this, t));
          }
        }
      });
    }
    return d;
  }

  /**
   * Returns a possibly empty list of all the RPCs that are in-flight.
   */
  private ArrayList<Deferred<Object>> getInflightRpcs() {
    final ArrayList<Deferred<Object>> inflight =
        new ArrayList<Deferred<Object>>();
    for (final KuduRpc rpc : rpcs_inflight.values()) {
      inflight.add(rpc.getDeferred());
    }
    return inflight;
  }

  /**
   * Returns a possibly {@code null} list of all RPCs that are pending.
   * <p>
   * Pending RPCs are those that are scheduled to be sent as soon as we
   * are connected to the TabletServer and have done version negotiation.
   */
  private ArrayList<Deferred<Object>> getPendingRpcs() {
    synchronized (this) {
      if (pending_rpcs != null) {
        final ArrayList<Deferred<Object>> pending =
            new ArrayList<Deferred<Object>>(pending_rpcs.size());
        for (final KuduRpc rpc : pending_rpcs) {
          pending.add(rpc.getDeferred());
        }
        return pending;
      }
    }
    return null;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel chan, ChannelBuffer buf,
                          VoidEnum voidEnum) {
    final long start = System.nanoTime();
    final int rdx = buf.readerIndex();
    LOG.debug("------------------>> ENTERING DECODE >>------------------");

    buf = secureRpcHelper.handleResponse(buf, chan);
    if (buf == null) {
      return null;
    }
    final int size = buf.readInt();
    ensureReadable(buf, size);
    KuduRpc.checkArrayLength(buf, size);
    RpcHeader.ResponseHeader.Builder builder = RpcHeader.ResponseHeader.newBuilder();
    KuduRpc.readProtobuf(buf, builder);
    RpcHeader.ResponseHeader header = builder.build();
    if (!header.hasCallId()) {
      final String msg = "RPC response (size: " + size + ") doesn't"
          + " have a call ID: " + header + ", buf=" + Bytes.pretty(buf);
      LOG.error(msg);
      throw new NonRecoverableException(msg);
    }
    final int rpcid = header.getCallId();

    final KuduRpc rpc = rpcs_inflight.get(rpcid);

    if (rpc == null) {
      final String msg = "Invalid rpcid: " + rpcid + " found in "
          + buf + '=' + Bytes.pretty(buf);
      LOG.error(msg);
      // The problem here is that we don't know which Deferred corresponds to
      // this RPC, since we don't have a valid ID.  So we're hopeless, we'll
      // never be able to recover because responses are not framed, we don't
      // know where the next response will start...  We have to give up here
      // and throw this outside of our Netty handler, so Netty will call our
      // exception handler where we'll close this channel, which will cause
      // all RPCs in flight to be failed.
      throw new NonRecoverableException(msg);
    }

    Object decoded;
    if (header.hasIsError() && header.getIsError()) {
      RpcHeader.ErrorStatusPB.Builder errorBuilder = RpcHeader.ErrorStatusPB.newBuilder();
      KuduRpc.readProtobuf(buf, errorBuilder);
      RpcHeader.ErrorStatusPB error = errorBuilder.build();
      if (error.getCode().equals(RpcHeader.ErrorStatusPB.RpcErrorCodePB.ERROR_SERVER_TOO_BUSY)) {
        // we're not calling rpc.callback() so we rely on the client to retry that RPC
        kuduClient.handleRetryableError(rpc, new TabletServerErrorException(error));
        return null;
      }
      String message = "Tablet server sent error " + error.getMessage();
      Exception ex = new Exception(message);
      LOG.error(message); // can be useful
      decoded = ex;
    } else {
      decoded = rpc.deserialize(buf);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("rpcid=" + rpcid
          + ", response size=" + (buf.readerIndex() - rdx) + " bytes"
          + ", " + actualReadableBytes() + " readable bytes left"
          + ", rpc=" + rpc);
    }

    {
      final KuduRpc removed = rpcs_inflight.remove(rpcid);
      assert rpc == removed;
    }

    // The TS services will send this error, make it pretty.
    if (decoded instanceof Tserver.TabletServerErrorPB) {
      Tserver.TabletServerErrorPB error = (Tserver.TabletServerErrorPB) decoded;
      if (error.getCode().equals(Tserver.TabletServerErrorPB.Code.TABLET_NOT_FOUND)) {
        kuduClient.handleNSTE(rpc, new TabletServerErrorException(error.getStatus()), this);
        // we're not calling rpc.callback() so we rely on the client to retry that RPC
        return null;
      }
      decoded = new TabletServerErrorException(error.getStatus());
    }

    try {
      rpc.callback(decoded);
    } catch (Exception e) {
      LOG.debug("Unexpected exception while handling RPC #" + rpcid
          + ", rpc=" + rpc + ", buf=" + Bytes.pretty(buf), e);
    }
  if (LOG.isDebugEnabled()) {
    LOG.debug("------------------<< LEAVING  DECODE <<------------------"
        + " time elapsed: " + ((System.nanoTime() - start) / 1000) + "us");
  }
    return null;  // Stop processing here.  The Deferred does everything else.
  }

  /**
   * Decodes the response of an RPC and triggers its {@link Deferred}.
   * <p>
   * This method is used by FrameDecoder when the channel gets
   * disconnected.  The buffer for that channel is passed to this method in
   * case there's anything left in it.
   * @param ctx Unused.
   * @param chan The channel on which the response came.
   * @param buf The buffer containing the raw RPC response.
   * @return {@code null}, always.
   */
  @Override
  protected Object decodeLast(final ChannelHandlerContext ctx,
                              final Channel chan,
                              final ChannelBuffer buf,
                              final VoidEnum unused) {
    // When we disconnect, decodeLast is called instead of decode.
    // We simply check whether there's any data left in the buffer, in which
    // case we attempt to process it.  But if there's no data left, then we
    // don't even bother calling decode() as it'll complain that the buffer
    // doesn't contain enough data, which unnecessarily pollutes the logs.
    if (buf.readable()) {
      try {
        return decode(ctx, chan, buf, unused);
      } finally {
        if (buf.readable()) {
          LOG.error("After decoding the last message on " + chan
              + ", there was still some undecoded bytes in the channel's"
              + " buffer (which are going to be lost): "
              + buf + '=' + Bytes.pretty(buf));
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Only for the masters, right now since a master is considered a TabletClient then it needs to
   * be able to contact the master services.
   * @param tableName Table name to lookup
   * @param startKey Start row key to begin lookup from.
   * @param startKey End row key to end lookup at.
   * @return deferred action
   */
  public Deferred<Master.GetTableLocationsResponsePB> getTableLocations(final String tableName,
                                                                        final byte[] startKey,
                                                                        final byte[] endKey) {
    if (startKey != null && endKey != null && Bytes.memcmp(startKey, endKey) > 0) {
      throw new IllegalArgumentException("The start key needs to be smaller or equal to the end " +
          "key");
    }

    final class GetTableLocations extends KuduRpc {
      GetTableLocations(KuduTable table) {
        super(table);
      }

      @Override
      String method() {
        return "GetTableLocations";
      }

      @Override
      Object deserialize(final ChannelBuffer buf) {
        Master.GetTableLocationsResponsePB.Builder builder = Master.GetTableLocationsResponsePB
            .newBuilder();
        readProtobuf(buf, builder);
        return builder.build();
      }

      @Override
      ChannelBuffer serialize(Message header) {
        final Master.GetTableLocationsRequestPB.Builder builder = Master
            .GetTableLocationsRequestPB.newBuilder();
        builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
        if (startKey != null) {
          builder.setStartKey(ZeroCopyLiteralByteString.wrap(startKey));
        }
        if (endKey != null) {
          builder.setEndKey(ZeroCopyLiteralByteString.wrap(endKey));
        }
        return toChannelBuffer(header, builder.build());
      }
    };
    final KuduRpc rpc = new GetTableLocations(kuduClient.masterTableHack);
    rpc.setTablet(kuduClient.masterTabletHack);
    final Deferred<Master.GetTableLocationsResponsePB> d = rpc.getDeferred()
        .addCallback(getTableLocationsCB);
    sendRpc(rpc);
    return d;
  }

  private static final Callback<Master.GetTableLocationsResponsePB, Object>
      getTableLocationsCB =
      new Callback<Master.GetTableLocationsResponsePB, Object>() {
        public Master.GetTableLocationsResponsePB call(final Object response) {
          if (response == null) {  // No result.
            return null;
          } else if (response instanceof Master.GetTableLocationsResponsePB) {
            final Master.GetTableLocationsResponsePB pb = (Master.GetTableLocationsResponsePB)
                response;
            return pb;
          } else {
            throw new InvalidResponseException(Master.GetTableLocationsResponsePB.class, response);
          }
        }
        public String toString() {
          return "type getTableLocations response";
        }
      };

  public Deferred<Master.IsCreateTableDoneResponsePB> isCreateTableDone(final String tableName) {
    final class IsCreateTableDone extends KuduRpc {
      IsCreateTableDone(KuduTable table) {
        super(table);
      }

      @Override
      String method() {
        return "IsCreateTableDone";
      }

      @Override
      Object deserialize(final ChannelBuffer buf) {
        Master.IsCreateTableDoneResponsePB.Builder builder = Master.IsCreateTableDoneResponsePB
            .newBuilder();
        readProtobuf(buf, builder);
        return builder.build();
      }

      @Override
      ChannelBuffer serialize(Message header) {
        final Master.IsCreateTableDoneRequestPB.Builder builder = Master
            .IsCreateTableDoneRequestPB.newBuilder();
        builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
        return toChannelBuffer(header, builder.build());
      }
    };
    final KuduRpc rpc = new IsCreateTableDone(kuduClient.masterTableHack);
    rpc.setTablet(kuduClient.masterTabletHack);
    final Deferred<Master.IsCreateTableDoneResponsePB> d = rpc.getDeferred()
        .addCallback(isCreateTableDoneCB);
    sendRpc(rpc);
    return d;
  }

  private static final Callback<Master.IsCreateTableDoneResponsePB, Object>
      isCreateTableDoneCB =
      new Callback<Master.IsCreateTableDoneResponsePB, Object>() {
        public Master.IsCreateTableDoneResponsePB call(final Object response) {
          if (response == null) {  // No result.
            return null;
          } else if (response instanceof Master.IsCreateTableDoneResponsePB) {
            final Master.IsCreateTableDoneResponsePB pb = (Master.IsCreateTableDoneResponsePB)
                response;
            return pb;
          } else {
            throw new InvalidResponseException(Master.IsCreateTableDoneResponsePB.class, response);
          }
        }
        public String toString() {
          return "type isCreateTableDone response";
        }
      };

  /**
   * Tells whether or not this handler should be used.
   * <p>
   * This method is not synchronized.  You need to synchronize on this
   * instance if you need a memory visibility guarantee.  You may not need
   * this guarantee if you're OK with the RPC finding out that the connection
   * has been reset "the hard way" and you can retry the RPC.  In this case,
   * you can call this method as a hint.  After getting the initial exception
   * back, this thread is guaranteed to see this method return {@code false}
   * without synchronization needed.
   * @return {@code false} if this handler is known to have been disconnected
   * from the server and sending an RPC (via {@link #sendRpc} or any other
   * indirect mean such as {@link #getTableLocations}) will fail immediately
   * by having the RPC's {@link Deferred} called back immediately with a
   * {@link ConnectionResetException}.  This typically means that you got a
   * stale reference (or that the reference to this instance is just about to
   * be invalidated) and that you shouldn't use this instance.
   */
  public boolean isAlive() {
    return !dead;
  }

  /**
   * Ensures that at least a {@code nbytes} are readable from the given buffer.
   * If there aren't enough bytes in the buffer this will raise an exception
   * and cause the {@link ReplayingDecoder} to undo whatever we did thus far
   * so we can wait until we read more from the socket.
   * @param buf Buffer to check.
   * @param nbytes Number of bytes desired.
   */
  static void ensureReadable(final ChannelBuffer buf, final int nbytes) {
    buf.markReaderIndex();
    buf.skipBytes(nbytes); // can puke with Throwable
    buf.resetReaderIndex();
  }

  @Override
  public void channelConnected(final ChannelHandlerContext ctx,
                               final ChannelStateEvent e) {
    final Channel chan = e.getChannel();
    ChannelBuffer header = connectionHeaderPreamble();
    header.writerIndex(RPC_HEADER.length);
    Channels.write(chan, header);

    secureRpcHelper = new SecureRpcHelper(this);
    secureRpcHelper.sendHello(chan);
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx,
                             final ChannelEvent e) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelDisconnected(final ChannelHandlerContext ctx,
                                  final ChannelStateEvent e) throws Exception {
    chan = null;
    super.channelDisconnected(ctx, e);  // Let the ReplayingDecoder cleanup.
    cleanup(e.getChannel());
  }

  @Override
  public void channelClosed(final ChannelHandlerContext ctx,
                            final ChannelStateEvent e) {
    chan = null;
    // No need to call super.channelClosed() because we already called
    // super.channelDisconnected().  If we get here without getting a
    // DISCONNECTED event, then we were never connected in the first place so
    // the ReplayingDecoder has nothing to cleanup.
    cleanup(e.getChannel());
  }

  /**
   * Cleans up any outstanding or lingering RPC (used when shutting down).
   * <p>
   * All RPCs in flight will fail with a {@link ConnectionResetException} and
   * all edits buffered will be re-scheduled.
   */
  private void cleanup(final Channel chan) {
    final ConnectionResetException exception =
        new ConnectionResetException("Connection reset on " + chan);
    failOrRetryRpcs(rpcs_inflight.values(), exception);
    rpcs_inflight.clear();

    final ArrayList<KuduRpc> rpcs;
    synchronized (this) {
      dead = true;
      rpcs = pending_rpcs;
      pending_rpcs = null;
    }
    if (rpcs != null) {
      failOrRetryRpcs(rpcs, exception);
    }
  }

  /**
   * Fail all RPCs in a collection or attempt to reschedule them if possible.
   * @param rpcs A possibly empty but non-{@code null} collection of RPCs.
   * @param exception The exception with which to fail RPCs that can't be
   * retried.
   */
  private void failOrRetryRpcs(final Collection<KuduRpc> rpcs,
                               final ConnectionResetException exception) {
    for (final KuduRpc rpc : rpcs) {
      final KuduClient.RemoteTablet tablet = rpc.getTablet();
      if (tablet == null  // Can't retry, dunno where this RPC should go.
          ) {
        rpc.callback(exception);
      } else {
        kuduClient.handleNSTE(rpc, new ConnectionResetException(exception.getMessage(), exception),
            this);
      }
    }
  }


  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx,
                              final ExceptionEvent event) {
    final Throwable e = event.getCause();
    final Channel c = event.getChannel();

    if (e instanceof RejectedExecutionException) {
      LOG.warn("RPC rejected by the executor,"
          + " ignore this if we're shutting down", e);
    } else {
      LOG.error("Unexpected exception from downstream on " + c, e);
    }
    if (c.isOpen()) {
      Channels.close(c);  // Will trigger channelClosed(), which will cleanup()
    } else {              // else: presumably a connection timeout.
      cleanup(c);         // => need to cleanup() from here directly.
    }
  }


  private ChannelBuffer connectionHeaderPreamble() {
    return ChannelBuffers.wrappedBuffer(RPC_HEADER);
  }

  public void becomeReady(Channel chan) {
    this.chan = chan;
    sendQueuedRpcs();
  }

  /**
   * Sends the queued RPCs to the server, once we're connected to it.
   * This gets called after {@link #channelConnected}, once we were able to
   * handshake with the server
   */
  private void sendQueuedRpcs() {
    ArrayList<KuduRpc> rpcs;
    synchronized (this) {
      rpcs = pending_rpcs;
      pending_rpcs = null;
    }
    if (rpcs != null) {
      for (final KuduRpc rpc : rpcs) {
        LOG.debug("Executing RPC queued: " + rpc);
        sendRpc(rpc);
      }
    }
  }

  void sendContext(Channel channel) {
    Channels.write(channel,  header());
    becomeReady(channel);
  }

  private ChannelBuffer header() {
    RpcHeader.ConnectionContextPB.Builder builder = RpcHeader.ConnectionContextPB.newBuilder();
    builder.setServiceName("TabletServerService"); // TODO set the right one?
    RpcHeader.UserInformationPB.Builder userBuilder = RpcHeader.UserInformationPB.newBuilder();
    userBuilder.setEffectiveUser(SecureRpcHelper.USER_AND_PASSWORD); // TODO set real user
    userBuilder.setRealUser(SecureRpcHelper.USER_AND_PASSWORD);
    builder.setUserInfo(userBuilder.build());
    RpcHeader.ConnectionContextPB pb = builder.build();
    RpcHeader.RequestHeader header = RpcHeader.RequestHeader.newBuilder().setCallId
        (CONNECTION_CTX_CALL_ID).build();
    return KuduRpc.toChannelBuffer(header, pb);
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(13 + 10 + 6 + 64 + 16 + 1 + 17 + 2 + 1);
    buf.append("TabletClient@")           // =13
        .append(hashCode())                 // ~10
        .append("(chan=")                   // = 6
        .append(chan)                       // ~64 (up to 66 when using IPv4)
        .append(", #pending_rpcs=");        // =16
    int npending_rpcs;
    synchronized (this) {
      npending_rpcs = pending_rpcs == null ? 0 : pending_rpcs.size();
    }
    buf.append(npending_rpcs);             // = 1
    buf.append(", #rpcs_inflight=")       // =17
        .append(rpcs_inflight.size())       // ~ 2
        .append(')');                       // = 1
    return buf.toString();
  }

}
