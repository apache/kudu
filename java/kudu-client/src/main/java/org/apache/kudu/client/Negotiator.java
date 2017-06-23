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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.PrivilegedExceptionAction;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.yetus.audience.InterfaceAudience;
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

import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.AuthenticationTypePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.util.SecurityUtil;

/**
 * Netty Pipeline handler which runs connection negotiation with
 * the server. When negotiation is complete, this removes itself
 * from the pipeline and fires a Negotiator.Result upstream.
 */
@InterfaceAudience.Private
public class Negotiator extends SimpleChannelUpstreamHandler {
  static final Logger LOG = LoggerFactory.getLogger(Negotiator.class);

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

  static final int CONNECTION_CTX_CALL_ID = -3;
  static final int SASL_CALL_ID = -33;

  private static enum State {
    INITIAL,
    AWAIT_NEGOTIATE,
    AWAIT_TLS_HANDSHAKE,
    AWAIT_TOKEN_EXCHANGE,
    AWAIT_SASL,
    FINISHED
  }

  /** The remote hostname we're connecting to, used by TLS and GSSAPI */
  private final String remoteHostname;
  /** The security context holding the client credentials */
  private final SecurityContext securityContext;
  /**
   * The authentication token we'll try to connect with, maybe null.
   * This is fetched from {@link #securityContext} in the constructor to
   * ensure that it doesn't change over the course of a negotiation attempt.
   */
  private final SignedTokenPB authnToken;

  private State state = State.INITIAL;
  private SaslClient saslClient;

  /** The negotiated mechanism, set after NEGOTIATE stage. */
  private String chosenMech;

  /** The negotiated authentication type, set after NEGOTIATE state. */
  private AuthenticationTypePB.TypeCase chosenAuthnType;

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

  /** True if we have negotiated TLS with the server */
  private boolean negotiatedTls;

  /**
   * The nonce sent from the server to the client, or null if negotiation has
   * not yet taken place, or the server does not send a nonce.
   */
  private byte[] nonce;

  /**
   * Future indicating whether the embedded handshake has completed.
   * Only non-null once TLS is initiated.
   */
  private ChannelFuture sslHandshakeFuture;

  private Certificate peerCert;

  @VisibleForTesting
  boolean overrideLoopbackForTests;

  public Negotiator(String remoteHostname, SecurityContext securityContext) {
    this.remoteHostname = remoteHostname;
    this.securityContext = securityContext;
    this.authnToken = securityContext.getAuthenticationToken();
  }

  public void sendHello(Channel channel) {
    sendNegotiateMessage(channel);
  }

  private void sendNegotiateMessage(Channel channel) {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder()
        .setStep(RpcHeader.NegotiatePB.NegotiateStep.NEGOTIATE);

    // Advertise our supported features
    // ----------------------------------
    for (RpcHeader.RpcFeatureFlag flag : SUPPORTED_RPC_FEATURES) {
      builder.addSupportedFeatures(flag);
    }
    if (isLoopbackConnection(channel)) {
      builder.addSupportedFeatures(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY);
    }

    // Advertise our authentication types.
    // ----------------------------------
    // We always advertise SASL.
    builder.addAuthnTypesBuilder().setSasl(
        AuthenticationTypePB.Sasl.getDefaultInstance());

    // We may also have a token. But, we can only use the token
    // if we are able to use authenticated TLS to authenticate the server.
    if (authnToken != null && securityContext.hasTrustedCerts()) {
      builder.addAuthnTypesBuilder().setToken(
          AuthenticationTypePB.Token.getDefaultInstance());
    }

    // We currently don't support client-certificate authentication from the
    // Java client.

    state = State.AWAIT_NEGOTIATE;
    sendSaslMessage(channel, builder.build());
  }

  private void sendSaslMessage(Channel channel, RpcHeader.NegotiatePB msg) {
    Preconditions.checkNotNull(channel);
    RpcHeader.RequestHeader.Builder builder = RpcHeader.RequestHeader.newBuilder();
    builder.setCallId(SASL_CALL_ID);
    Channels.write(channel, new RpcOutboundMessage(builder, msg));
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
      throws IOException {
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
      case AWAIT_TOKEN_EXCHANGE:
        handleTokenExchangeResponse(chan, response);
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
      throws IOException {
    switch (response.getStep()) {
      case SASL_CHALLENGE:
        handleChallengeResponse(chan, response);
        break;
      case SASL_SUCCESS:
        handleSuccessResponse(chan, response);
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
    Preconditions.checkState(response.getStep() == NegotiateStep.NEGOTIATE,
        "Expected NEGOTIATE message, got {}", response.getStep());

    // Store the supported features advertised by the server.
    serverFeatures = getFeatureFlags(response);
    // If the server supports TLS, we will always speak TLS to it.
    negotiatedTls = serverFeatures.contains(RpcFeatureFlag.TLS);

    // Check the negotiated authentication type sent by the server.
    chosenAuthnType = chooseAuthenticationType(response);

    if (chosenAuthnType == AuthenticationTypePB.TypeCase.SASL) {
      chooseAndInitializeSaslMech(response);
    }

    // If we negotiated TLS, then we want to start the TLS handshake; otherwise,
    // we can move directly to the authentication phase.
    if (negotiatedTls) {
      startTlsHandshake(chan);
    } else {
      startAuthentication(chan);
    }
  }

  /**
   * Determine whether the given channel is a loopback connection (i.e. the server
   * and client are on the same host).
   */
  private boolean isLoopbackConnection(Channel channel) {
    if (overrideLoopbackForTests) {
      return true;
    }
    try {
      InetAddress local = ((InetSocketAddress)channel.getLocalAddress()).getAddress();
      InetAddress remote = ((InetSocketAddress)channel.getRemoteAddress()).getAddress();
      return local.equals(remote);
    } catch (ClassCastException cce) {
      // In the off chance that we have some other type of local/remote address,
      // we'll just assume it's not loopback.
      return false;
    }
  }

  private void chooseAndInitializeSaslMech(NegotiatePB response) throws SaslException {
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
      // If the negotiated mechanism is GSSAPI (Kerberos), configure SASL to use
      // integrity protection so that the channel bindings and nonce can be
      // verified.
      if ("GSSAPI".equals(clientMech)) {
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
  }

  private AuthenticationTypePB.TypeCase chooseAuthenticationType(NegotiatePB response) {
    Preconditions.checkArgument(response.getAuthnTypesCount() <= 1,
        "Expected server to reply with at most one authn type");

    if (response.getAuthnTypesCount() == 0) {
      // Default to SASL for compatibility with old servers.
      return AuthenticationTypePB.TypeCase.SASL;
    }

    AuthenticationTypePB.TypeCase type = response.getAuthnTypes(0).getTypeCase();
    switch (type) {
      case SASL:
        break;
      case TOKEN:
        if (authnToken == null) {
          // TODO(todd): should we also check whether we have a CA cert?
          // it seems like this should have the same logic as whether we advertised it
          throw new IllegalArgumentException("server chose token authentication " +
              "but client had no valid token");
        }
        break;
      default:
        throw new IllegalArgumentException("server chose bad authn type " + chosenAuthnType);
    }
    return type;
  }

  private Set<RpcFeatureFlag> getFeatureFlags(NegotiatePB response) {
    ImmutableSet.Builder<RpcHeader.RpcFeatureFlag> features = ImmutableSet.builder();
    for (RpcHeader.RpcFeatureFlag feature : response.getSupportedFeaturesList()) {
      if (feature != RpcFeatureFlag.UNKNOWN) {
        features.add(feature);
      }
    }
    return features.build();
  }

  /**
   * Send the initial TLS "ClientHello" message.
   */
  private void startTlsHandshake(Channel chan) throws SSLException {
    SSLEngine engine;
    switch (chosenAuthnType) {
      case SASL:
        engine = securityContext.createSSLEngineTrustAll();
        break;
      case TOKEN:
        engine = securityContext.createSSLEngine();
        break;
      default:
        throw new AssertionError("unreachable");
    }
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
      throws IOException {
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
    SslHandler handler = (SslHandler)sslEmbedder.getPipeline().getFirst();
    Certificate[] certs = handler.getEngine().getSession().getPeerCertificates();
    if (certs.length == 0) {
      throw new SSLPeerUnverifiedException("no peer cert found");
    }

    // Don't wrap the TLS socket if we are using TLS for authentication only.
    boolean isAuthOnly = serverFeatures.contains(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY) &&
        isLoopbackConnection(chan);
    if (!isAuthOnly) {
      chan.getPipeline().addFirst("tls", handler);
    }
    startAuthentication(chan);
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
      // TODO(todd): should check sslHandshakeFuture.isSuccess()
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

  private void startAuthentication(Channel chan) throws SaslException {
    switch (chosenAuthnType) {
      case SASL:
        sendSaslInitiate(chan);
        break;
      case TOKEN:
        sendTokenExchange(chan);
        break;
      default:
        throw new AssertionError("unreachable");
    }
  }

  private void sendTokenExchange(Channel chan) {
    // We must not send a token unless we have successfully finished
    // authenticating via TLS.
    Preconditions.checkNotNull(authnToken);
    Preconditions.checkNotNull(sslHandshakeFuture);
    Preconditions.checkState(sslHandshakeFuture.isSuccess());

    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TOKEN_EXCHANGE)
        .setAuthnToken(authnToken);
    state = State.AWAIT_TOKEN_EXCHANGE;
    sendSaslMessage(chan, builder.build());
  }

  private void handleTokenExchangeResponse(Channel chan,
                                           NegotiatePB response) throws SaslException {
    Preconditions.checkArgument(response.getStep() == NegotiateStep.TOKEN_EXCHANGE,
        "expected TOKEN_EXCHANGE, got step: {}", response.getStep());

    // The token response doesn't have any actual data in it, so we can just move on.
    finish(chan);
  }

  private void sendSaslInitiate(Channel chan) throws SaslException {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    if (saslClient.hasInitialResponse()) {
      byte[] initialResponse = evaluateChallenge(new byte[0]);
      builder.setToken(UnsafeByteOperations.unsafeWrap(initialResponse));
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
    builder.setToken(UnsafeByteOperations.unsafeWrap(saslToken));
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_RESPONSE);
    sendSaslMessage(chan, builder.build());
  }

  /**
   * Verify the channel bindings included in 'response'. This is used only
   * for GSSAPI-authenticated connections over TLS.
   * @throws RuntimeException on failure to verify
   */
  private void verifyChannelBindings(NegotiatePB response) throws IOException {
    byte[] expected = SecurityUtil.getEndpointChannelBindings(peerCert);
    if (!response.hasChannelBindings()) {
      throw new SSLPeerUnverifiedException("no channel bindings provided by remote peer");
    }
    byte[] provided = response.getChannelBindings().toByteArray();
    // NOTE: the C SASL library's implementation of sasl_encode() actually
    // includes a length prefix. Java's equivalents do not. So, we have to
    // chop off the length prefix here before unwrapping.
    if (provided.length < 4) {
      throw new SSLPeerUnverifiedException("invalid too-short channel bindings");
    }
    byte[] unwrapped = saslClient.unwrap(provided, 4, provided.length - 4);
    if (!Bytes.equals(expected, unwrapped)) {
      throw new SSLPeerUnverifiedException("invalid channel bindings provided by remote peer");
    }
  }

  private void handleSuccessResponse(Channel chan, NegotiatePB response) throws IOException {
    Preconditions.checkState(saslClient.isComplete(),
                             "server sent SASL_SUCCESS step, but SASL negotiation is not complete");
    if (chosenMech.equals("GSSAPI")) {
      if (response.hasNonce()) {
        // Grab the nonce from the server, if it has sent one. We'll send it back
        // later with SASL integrity protection as part of the connection context.
        nonce = response.getNonce().toByteArray();
      }

      if (peerCert != null) {
        // Check the channel bindings provided by the server against the expected channel bindings.
        verifyChannelBindings(response);
      }
    }

    finish(chan);
  }

  /**
   * Marks the negotiation as finished, and sends the connection context to the server.
   * @param chan the connection channel
   */
  private void finish(Channel chan) throws SaslException {
    state = State.FINISHED;
    chan.getPipeline().remove(this);

    Channels.write(chan, makeConnectionContext());
    Channels.fireMessageReceived(chan, new Result(serverFeatures));
  }

  private RpcOutboundMessage makeConnectionContext() throws SaslException {
    RpcHeader.ConnectionContextPB.Builder builder = RpcHeader.ConnectionContextPB.newBuilder();

    // The UserInformationPB is deprecated, but used by servers prior to Kudu 1.1.
    RpcHeader.UserInformationPB.Builder userBuilder = RpcHeader.UserInformationPB.newBuilder();
    String user = System.getProperty("user.name");
    userBuilder.setEffectiveUser(user);
    userBuilder.setRealUser(user);
    builder.setDEPRECATEDUserInfo(userBuilder.build());

    if (nonce != null) {
      // Reply with the SASL-protected nonce. We only set the nonce when using SASL GSSAPI.
      // The Java SASL client does not automatically add the length header,
      // so we have to do it ourselves.
      byte[] encodedNonce = saslClient.wrap(nonce, 0, nonce.length);
      ByteBuffer buf = ByteBuffer.allocate(encodedNonce.length + 4);
      buf.order(ByteOrder.BIG_ENDIAN);
      buf.putInt(encodedNonce.length);
      buf.put(encodedNonce);
      builder.setEncodedNonce(UnsafeByteOperations.unsafeWrap(buf.array()));
    }

    RpcHeader.ConnectionContextPB pb = builder.build();
    RpcHeader.RequestHeader.Builder header =
        RpcHeader.RequestHeader.newBuilder().setCallId(CONNECTION_CTX_CALL_ID);
    return new RpcOutboundMessage(header, pb);
  }

  private byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
    try {
      return Subject.doAs(securityContext.getSubject(),
          new PrivilegedExceptionAction<byte[]>() {
          @Override
          public byte[] run() throws Exception {
            return saslClient.evaluateChallenge(challenge);
          }
        });
    } catch (Exception e) {
      Throwables.throwIfInstanceOf(e, SaslException.class);
      throw new RuntimeException(e);
    }
  }

  private static class SaslClientCallbackHandler implements CallbackHandler {
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          ((NameCallback) callback).setName(System.getProperty("user.name"));
        } else if (callback instanceof PasswordCallback) {
          ((PasswordCallback) callback).setPassword(new char[0]);
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
