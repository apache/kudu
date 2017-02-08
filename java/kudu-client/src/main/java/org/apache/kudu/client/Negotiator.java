/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the aabove copyright notice,
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

import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.util.SecurityUtil;

/**
 * Netty Pipeline handler which runs connection negotiation with
 * the server. When negotiation is complete, this removes itself
 * from the pipeline and fires a Negotiator.Result upstream.
 */
@InterfaceAudience.Private
public class Negotiator extends SimpleChannelUpstreamHandler {

  static final Logger LOG = LoggerFactory.getLogger(TabletClient.class);

  private static final SaslClientCallbackHandler SASL_CALLBACK = new SaslClientCallbackHandler();
  private static final Set<RpcHeader.RpcFeatureFlag> SUPPORTED_RPC_FEATURES =
      ImmutableSet.of(
          RpcHeader.RpcFeatureFlag.APPLICATION_FEATURE_FLAGS,
          RpcHeader.RpcFeatureFlag.TLS);

  /**
   * List of SASL mechanisms supported by the client, in descending priority order.
   * The client will pick the first of these mechanisms that is supported by
   * the server and also succeeds to initialize.
   */
  private static final String[] PRIORITIZED_MECHS = new String[] { "GSSAPI", "PLAIN" };

  static final String USER_AND_PASSWORD = "java_client";

  static final int CONNECTION_CTX_CALL_ID = -3;
  static final int SASL_CALL_ID = -33;

  private static enum State {
    INITIAL,
    AWAIT_NEGOTIATE,
    AWAIT_TLS_HANDSHAKE,
    AWAIT_SASL,
    FINISHED
  }

  /** Subject to authenticate as, when using Kerberos/GSSAPI */
  private final Subject subject;
  /** The remote hostname we're connecting to, used by TLS and GSSAPI */
  private final String remoteHostname;

  private State state = State.INITIAL;
  private SaslClient saslClient;

  /** The negotiated mechanism, set after NEGOTIATE stage. */
  private String chosenMech;

  /** The features supported by the server, set after NEGOTIATE stage. */
  private Set<RpcHeader.RpcFeatureFlag> serverFeatures;

  /**
   * The negotiation protocol relies on tunneling the TLS handshake through
   * protobufs. The embedder holds a Netty SslHandler which can perform the
   * handshake. Once the handshake is complete, we will stop using the embedder
   * and add the handler directly to the real ChannelPipeline.
   * Only non-null once TLS is initiated.
   */
  private DecoderEmbedder<ChannelBuffer> sslEmbedder;

  /**
   * Future indicating whether the embedded handshake has completed.
   * Only non-null once TLS is initiated.
   */
  private ChannelFuture sslHandshakeFuture;

  public Negotiator(Subject subject, String remoteHostname) {
    this.subject = subject;
    this.remoteHostname = remoteHostname;
  }

  public void sendHello(Channel channel) {
    sendNegotiateMessage(channel);
  }

  private void sendNegotiateMessage(Channel channel) {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();

    // Advertise our supported features
    for (RpcHeader.RpcFeatureFlag flag : SUPPORTED_RPC_FEATURES) {
      builder.addSupportedFeatures(flag);
    }

    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.NEGOTIATE);
    state = State.AWAIT_NEGOTIATE;
    sendSaslMessage(channel, builder.build());
  }

  private void sendSaslMessage(Channel channel, RpcHeader.NegotiatePB msg) {
    Preconditions.checkNotNull(channel);
    RpcHeader.RequestHeader.Builder builder = RpcHeader.RequestHeader.newBuilder();
    builder.setCallId(SASL_CALL_ID);
    RpcHeader.RequestHeader header = builder.build();
    Channels.write(channel, new RpcOutboundMessage(header, msg));
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent evt)
      throws Exception {
    Object m = evt.getMessage();
    if (!(m instanceof CallResponse)) {
      ctx.sendUpstream(evt);
      return;
    }
    handleResponse(ctx.getChannel(), (CallResponse)m);
  }

  private void handleResponse(Channel chan, CallResponse callResponse)
      throws SaslException, SSLException {
    // TODO(todd): this needs to handle error responses, not just success responses.
    RpcHeader.NegotiatePB response = parseSaslMsgResponse(callResponse);

    // TODO: check that the message type matches the expected one in all
    // of the below implementations.
    switch (state) {
      case AWAIT_NEGOTIATE:
        handleNegotiateResponse(chan, response);
        break;
      case AWAIT_SASL:
        handleSaslMessage(chan, response);
        break;
      case AWAIT_TLS_HANDSHAKE:
        handleTlsMessage(chan, response);
        break;
      default:
        throw new IllegalStateException("received a message in unexpected state: " +
            state.toString());
    }
  }

  private void handleSaslMessage(Channel chan, NegotiatePB response)
      throws SaslException {
    switch (response.getStep()) {
      case SASL_CHALLENGE:
        handleChallengeResponse(chan, response);
        break;
      case SASL_SUCCESS:
        handleSuccessResponse(chan);
        break;
      default:
        throw new IllegalStateException("Wrong negotiation step: " +
            response.getStep());
    }
  }


  private RpcHeader.NegotiatePB parseSaslMsgResponse(CallResponse response) {
    RpcHeader.ResponseHeader responseHeader = response.getHeader();
    int id = responseHeader.getCallId();
    if (id != SASL_CALL_ID) {
      throw new IllegalStateException("Received a call that wasn't for SASL");
    }

    RpcHeader.NegotiatePB.Builder saslBuilder = RpcHeader.NegotiatePB.newBuilder();
    KuduRpc.readProtobuf(response.getPBMessage(), saslBuilder);
    return saslBuilder.build();
  }

  private void handleNegotiateResponse(Channel chan, RpcHeader.NegotiatePB response) throws
      SaslException, SSLException {
    // Store the supported features advertised by the server.
    ImmutableSet.Builder<RpcHeader.RpcFeatureFlag> features = ImmutableSet.builder();
    for (RpcHeader.RpcFeatureFlag feature : response.getSupportedFeaturesList()) {
      if (SUPPORTED_RPC_FEATURES.contains(feature)) {
        features.add(feature);
      }
    }
    serverFeatures = features.build();

    boolean willUseTls = serverFeatures.contains(RpcFeatureFlag.TLS);

    // Gather the set of server-supported mechanisms.
    Set<String> serverMechs = Sets.newHashSet();
    for (RpcHeader.NegotiatePB.SaslMechanism mech : response.getSaslMechanismsList()) {
      serverMechs.add(mech.getMechanism());
    }

    // For each of our own mechanisms, in descending priority, check if
    // the server also supports them. If so, try to initialize saslClient.
    // If we find a common mechanism that also can be successfully initialized,
    // choose that mech.
    Map<String, String> errorsByMech = Maps.newHashMap();
    for (String clientMech : PRIORITIZED_MECHS) {
      if (!serverMechs.contains(clientMech)) {
        errorsByMech.put(clientMech, "not advertised by server");
        continue;
      }
      Map<String, String> props = Maps.newHashMap();
      // If we are using GSSAPI with TLS, enable integrity protection, which we use
      // to securely transmit the channel bindings. Channel bindings aren't
      // yet implemented, but if we don't advertise 'auth-int', then the server
      // won't talk to us.
      if ("GSSAPI".equals(clientMech) && willUseTls) {
        props.put(Sasl.QOP, "auth-int");
      }
      try {
        saslClient = Sasl.createSaslClient(new String[]{ clientMech },
                                           null,
                                           "kudu",
                                           remoteHostname,
                                           props,
                                           SASL_CALLBACK);
        chosenMech = clientMech;
        break;
      } catch (SaslException e) {
        errorsByMech.put(clientMech, e.getMessage());
        saslClient = null;
      }
    }

    if (chosenMech == null) {
      throw new SaslException("unable to negotiate a matching mechanism. Errors: [" +
                              Joiner.on(",").withKeyValueSeparator(": ").join(errorsByMech) +
                              "]");
    }

    // If we negotiated TLS, then we want to start the TLS handshake.
    if (willUseTls) {
      startTlsHandshake(chan);
    } else {
      sendSaslInitiate(chan);
    }
  }

  /**
   * Send the initial TLS "ClientHello" message.
   */
  private void startTlsHandshake(Channel chan) throws SSLException {
    SSLEngine engine = SecurityUtil.createSslEngine();
    // TODO(todd): remove usage of this anonymous cipher suite.
    // It's replaced in the next patch in this patch series by
    // a self-signed cert used for tests.
    engine.setEnabledCipherSuites(ObjectArrays.concat(
        engine.getEnabledCipherSuites(),
        "TLS_DH_anon_WITH_AES_128_CBC_SHA"));
    engine.setUseClientMode(true);
    SslHandler handler = new SslHandler(engine);
    handler.setEnableRenegotiation(false);
    sslEmbedder = new DecoderEmbedder<>(handler);
    sslHandshakeFuture = handler.handshake();
    state = State.AWAIT_TLS_HANDSHAKE;
    boolean sent = sendPendingOutboundTls(chan);
    assert sent;
  }

  /**
   * Handle an inbound message during the TLS handshake. If this message
   * causes the handshake to complete, triggers the beginning of SASL initiation.
   */
  private void handleTlsMessage(Channel chan, NegotiatePB response)
      throws SaslException {
    Preconditions.checkState(response.getStep() == NegotiateStep.TLS_HANDSHAKE);
    Preconditions.checkArgument(!response.getTlsHandshake().isEmpty(),
        "empty TLS message from server");

    // Pass the TLS message into our embedded SslHandler.
    sslEmbedder.offer(ChannelBuffers.copiedBuffer(
        response.getTlsHandshake().asReadOnlyByteBuffer()));
    if (sendPendingOutboundTls(chan)) {
      // Data was sent -- we must continue the handshake process.
      return;
    }
    // The handshake completed.
    // Insert the SSL handler into the pipeline so that all following traffic
    // gets encrypted, and then move on to the SASL portion of negotiation.
    //
    // NOTE: this takes effect immediately (i.e. the following SASL initiation
    // sequence is encrypted).
    chan.getPipeline().addFirst("tls", sslEmbedder.getPipeline().getFirst());
    sendSaslInitiate(chan);
  }

  /**
   * If the embedded SslHandler has data to send outbound, gather
   * it all, send it tunneled in a NegotiatePB message, and return true.
   *
   * Otherwise, indicates that the handshake is complete by returning false.
   */
  private boolean sendPendingOutboundTls(Channel chan) {
    // The SslHandler can generate multiple TLS messages in response
    // (e.g. ClientKeyExchange, ChangeCipherSpec, ClientFinished).
    // We poll the handler until it stops giving us buffers.
    List<ByteString> bufs = Lists.newArrayList();
    while (sslEmbedder.peek() != null) {
      bufs.add(ByteString.copyFrom(sslEmbedder.poll().toByteBuffer()));
    }
    ByteString data = ByteString.copyFrom(bufs);
    if (sslHandshakeFuture.isDone()) {
      // TODO(danburkert): is this a correct assumption? would the
      // client ever be "done" but also produce handshake data?
      // if it did, would we want to encrypt the SSL message or no?
      assert data.size() == 0;
      return false;
    } else {
      assert data.size() > 0;
      sendTunneledTls(chan, data);
      return true;
    }
  }

  /**
   * Send a buffer of data for the TLS handshake, encapsulated in the
   * appropriate TLS_HANDSHAKE negotiation message.
   */
  private void sendTunneledTls(Channel chan, ByteString buf) {
    sendSaslMessage(chan, NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TLS_HANDSHAKE)
        .setTlsHandshake(buf)
        .build());
  }

  private void sendSaslInitiate(Channel chan) throws SaslException {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    if (saslClient.hasInitialResponse()) {
      byte[] initialResponse = evaluateChallenge(new byte[0]);
      builder.setToken(ZeroCopyLiteralByteString.wrap(initialResponse));
    }
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_INITIATE);
    builder.addSaslMechanismsBuilder().setMechanism(chosenMech);
    state = State.AWAIT_SASL;
    sendSaslMessage(chan, builder.build());
  }

  private void handleChallengeResponse(Channel chan, RpcHeader.NegotiatePB response) throws
      SaslException {
    byte[] saslToken = evaluateChallenge(response.getToken().toByteArray());
    if (saslToken == null) {
      throw new IllegalStateException("Not expecting an empty token");
    }
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    builder.setToken(ZeroCopyLiteralByteString.wrap(saslToken));
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_RESPONSE);
    sendSaslMessage(chan, builder.build());
  }

  private void handleSuccessResponse(Channel chan) {
    state = State.FINISHED;
    chan.getPipeline().remove(this);

    Channels.write(chan, makeConnectionContext());
    Channels.fireMessageReceived(chan, new Result(serverFeatures));
  }

  private RpcOutboundMessage makeConnectionContext() {
    RpcHeader.ConnectionContextPB.Builder builder = RpcHeader.ConnectionContextPB.newBuilder();

    // The UserInformationPB is deprecated, but used by servers prior to Kudu 1.1.
    RpcHeader.UserInformationPB.Builder userBuilder = RpcHeader.UserInformationPB.newBuilder();
    userBuilder.setEffectiveUser(Negotiator.USER_AND_PASSWORD);
    userBuilder.setRealUser(Negotiator.USER_AND_PASSWORD);
    builder.setDEPRECATEDUserInfo(userBuilder.build());
    RpcHeader.ConnectionContextPB pb = builder.build();
    RpcHeader.RequestHeader header =
        RpcHeader.RequestHeader.newBuilder().setCallId(CONNECTION_CTX_CALL_ID).build();
    return new RpcOutboundMessage(header, pb);
  }

  private byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
    try {
      return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
          @Override
          public byte[] run() throws Exception {
            return saslClient.evaluateChallenge(challenge);
          }
        });
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, SaslException.class);
      throw Throwables.propagate(e);
    }
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          ((NameCallback) callback).setName(USER_AND_PASSWORD);
        } else if (callback instanceof PasswordCallback) {
          ((PasswordCallback) callback).setPassword(USER_AND_PASSWORD.toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback,
                                                 "Unrecognized SASL client callback");
        }
      }
    }
  }

  /**
   * The results of a successful negotiation. This is sent to upstream handlers in the
   * Netty pipeline after negotiation completes.
   */
  static class Result {
    final Set<RpcFeatureFlag> serverFeatures;

    public Result(Set<RpcFeatureFlag> serverFeatures) {
      this.serverFeatures = serverFeatures;
    }
  }
}
