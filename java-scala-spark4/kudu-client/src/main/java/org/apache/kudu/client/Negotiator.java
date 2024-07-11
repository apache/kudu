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
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.cert.Certificate;
import java.util.List;
import java.util.Locale;
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
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.apache.yetus.audience.InterfaceAudience;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.AuthenticationTypePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.security.Token.JwtRawPB;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.util.SecurityUtil;

/**
 * Netty Pipeline handler which runs connection negotiation with
 * the server. When negotiation is complete, this removes itself
 * from the pipeline and fires a Negotiator.Success or Negotiator.Failure upstream.
 */
@InterfaceAudience.Private
public class Negotiator extends SimpleChannelInboundHandler<CallResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(Negotiator.class);

  private final SaslClientCallbackHandler saslCallback = new SaslClientCallbackHandler();
  private static final ImmutableSet<RpcHeader.RpcFeatureFlag> SUPPORTED_RPC_FEATURES =
      ImmutableSet.of(
          RpcHeader.RpcFeatureFlag.APPLICATION_FEATURE_FLAGS,
          RpcHeader.RpcFeatureFlag.TLS);

  /**
   * Set of SASL mechanisms supported by the client, in descending priority order.
   * The client will pick the first of these mechanisms that is supported by
   * the server and also succeeds to initialize.
   */
  private enum SaslMechanism {
    GSSAPI,
    PLAIN,
  }

  static final int CONNECTION_CTX_CALL_ID = -3;
  static final int SASL_CALL_ID = -33;

  /**
   * The cipher suites, in order of our preference.
   *
   * This list is based on the kDefaultTls13Ciphers and kDefaultTlsCiphers lists
   * in security_flags.cc: see that file for details on how it was derived.
   *
   * For the mapping between IANA and OpenSSL cipher names, run
   * `openssl ciphers -stdname` on OpenSSL 1.1.1 (and newer) or see
   *   https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
   *   https://wiki.mozilla.org/Security/Cipher_Suites
   *
   * For information on TLSv1.3 (JEP 332) and appropriate ciphers in Java 8
   * updates, see
   *   https://www.oracle.com/java/technologies/javase/8all-relnotes.html
   */
  static final String[] PREFERRED_CIPHER_SUITES = new String[] {
      "TLS_AES_128_GCM_SHA256",                 // TLSv1.3: Java 8 updates (8u261), Java 11
      "TLS_AES_256_GCM_SHA384",                 // TLSv1.3: Java 8 updates (8u261), Java 11
      "TLS_CHACHA20_POLY1305_SHA256",           // TLSv1.3: Java 12
      "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",// TLSv1.2: Java 8
      "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",  // TLSv1.2: Java 8
      "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",// TLSv1.2: Java 8
      "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",  // TLSv1.2: Java 8
      "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",  // TLSv1.2: Java 12
      "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",    // TLSv1.2: Java 12
      // The following AES CBC ciphers are present to be around when no AES GCM
      // ciphers are available (that's so for some FIPS 140-2 environments).
      "TLS_ECDHE_ECDSA_WITH_AES_128_CCM",       // TLSv1.2: custom JSSE providers
      "TLS_ECDHE_ECDSA_WITH_AES_256_CCM",       // TLSv1.2: custom JSSE providers
      "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",// TLSv1.2: Java 7
      "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",  // TLSv1.2: Java 7
      "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",// TLSv1.2: Java 7
      "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",  // TLSv1.2: Java 7
  };

  /**
   * TLS protocols to enable among those supported by SSLEngine.
   * This list is based on the kDefaultTlsMinVersion in security_flags.cc.
   */
  static final String[] PREFERRED_PROTOCOLS = new String[]{
      "TLSv1.3",  // Java 8 updates (8u261), Java 11
      "TLSv1.2",  // Java 8
  };

  private enum State {
    INITIAL,
    AWAIT_NEGOTIATE,
    AWAIT_TLS_HANDSHAKE,
    AWAIT_AUTHN_TOKEN_EXCHANGE,
    AWAIT_JWT_EXCHANGE,
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
   * An authentication token is used as secondary credentials.
   */
  private final SignedTokenPB authnToken;
  /**
   * A JSON Web Token (JWT) to authenticate this client/actor to the server
   * that we'll try to connect with. Similar to {@link #authnToken}, this token
   * may be null, and it's fetched from {@link #securityContext} as well.
   * Cannot change over the course of an RPC connection negotiation attempt.
   *
   * @note unlike {@link #authnToken}, {@link #jsonWebToken} is used as primary credentials
   */
  private final JwtRawPB jsonWebToken;

  private enum AuthnTokenNotUsedReason {
    NONE_AVAILABLE("no token is available"),
    NO_TRUSTED_CERTS("no TLS certificates are trusted by the client"),
    FORBIDDEN_BY_POLICY("this connection will be used to acquire a new token and " +
                        "therefore requires primary credentials"),
    NOT_CHOSEN_BY_SERVER("the server chose not to accept token authentication");

    AuthnTokenNotUsedReason(String msg) {
      this.msg = msg;
    }

    final String msg;
  }

  private AuthnTokenNotUsedReason authnTokenNotUsedReason = null;

  private State state = State.INITIAL;
  private SaslClient saslClient;

  /** The negotiated mechanism, set after NEGOTIATE stage. */
  private SaslMechanism chosenMech;

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
  private EmbeddedChannel sslEmbedder;

  /**
   * The nonce sent from the server to the client, or null if negotiation has
   * not yet taken place, or the server does not send a nonce.
   */
  private byte[] nonce;

  /**
   * Future indicating whether the embedded handshake has completed.
   * Only non-null once TLS is initiated.
   */
  private Future<Channel> sslHandshakeFuture;

  private Certificate peerCert;

  private final String saslProtocolName;

  private final boolean requireAuthentication;

  private final boolean requireEncryption;

  private final boolean encryptLoopback;

  @InterfaceAudience.LimitedPrivate("Test")
  boolean overrideLoopbackForTests;

  public Negotiator(String remoteHostname,
                    SecurityContext securityContext,
                    boolean ignoreAuthnToken,
                    String saslProtocolName,
                    boolean requireAuthentication,
                    boolean requireEncryption,
                    boolean encryptLoopback) {
    this.remoteHostname = remoteHostname;
    this.securityContext = securityContext;
    this.saslProtocolName = saslProtocolName;
    this.requireAuthentication = requireAuthentication;
    this.requireEncryption = requireEncryption;
    this.encryptLoopback = encryptLoopback;

    SignedTokenPB token = securityContext.getAuthenticationToken();
    if (token != null) {
      if (ignoreAuthnToken) {
        this.authnToken = null;
        this.authnTokenNotUsedReason = AuthnTokenNotUsedReason.FORBIDDEN_BY_POLICY;
      } else if (!securityContext.hasTrustedCerts()) {
        this.authnToken = null;
        this.authnTokenNotUsedReason = AuthnTokenNotUsedReason.NO_TRUSTED_CERTS;
      } else {
        this.authnToken = token;
      }
    } else {
      this.authnToken = null;
      this.authnTokenNotUsedReason = AuthnTokenNotUsedReason.NONE_AVAILABLE;
    }

    JwtRawPB jwt = securityContext.getJsonWebToken();
    if (jwt != null && securityContext.hasTrustedCerts()) {
      this.jsonWebToken = jwt;
    } else {
      this.jsonWebToken = null;
    }
  }

  public void sendHello(ChannelHandlerContext ctx) {
    sendNegotiateMessage(ctx);
  }

  private void sendNegotiateMessage(ChannelHandlerContext ctx) {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder()
        .setStep(RpcHeader.NegotiatePB.NegotiateStep.NEGOTIATE);

    // Advertise our supported features
    // ----------------------------------
    for (RpcHeader.RpcFeatureFlag flag : SUPPORTED_RPC_FEATURES) {
      builder.addSupportedFeatures(flag);
    }
    if (isLoopbackConnection(ctx.channel()) && !encryptLoopback) {
      builder.addSupportedFeatures(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY);
    }

    // Advertise our authentication types.
    // ----------------------------------
    // We always advertise SASL.
    builder.addAuthnTypesBuilder().setSasl(
        AuthenticationTypePB.Sasl.getDefaultInstance());

    // We may also have a token. But, we can only use the token
    // if we are able to use authenticated TLS to authenticate the server.
    if (authnToken != null) {
      builder.addAuthnTypesBuilder().setToken(
          AuthenticationTypePB.Token.getDefaultInstance());
    }

    // We may also have a JSON Web token, but it can be sent to the server
    // only if we can verify the server's authenticity and the channel between
    // this client and the server is confidential, i.e. it's protected by
    // authenticated TLS.
    if (jsonWebToken != null) {
      builder.addAuthnTypesBuilder().setJwt(
          AuthenticationTypePB.Jwt.getDefaultInstance());
    }

    // We currently don't support client-certificate authentication from the
    // Java client.

    state = State.AWAIT_NEGOTIATE;
    sendSaslMessage(ctx, builder.build());
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void sendSaslMessage(ChannelHandlerContext ctx, RpcHeader.NegotiatePB msg) {
    RpcHeader.RequestHeader.Builder builder = RpcHeader.RequestHeader.newBuilder();
    builder.setCallId(SASL_CALL_ID);
    ctx.writeAndFlush(new RpcOutboundMessage(builder, msg), ctx.voidPromise());
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, CallResponse msg) throws IOException {
    final RpcHeader.ResponseHeader header = msg.getHeader();
    if (header.getIsError()) {
      final RpcHeader.ErrorStatusPB.Builder errBuilder = RpcHeader.ErrorStatusPB.newBuilder();
      KuduRpc.readProtobuf(msg.getPBMessage(), errBuilder);
      final RpcHeader.ErrorStatusPB error = errBuilder.build();
      LOG.debug("peer {} sent connection negotiation error: {}",
          ctx.channel().remoteAddress(), error.getMessage());

      // The upstream code should handle the negotiation failure.
      state = State.FINISHED;
      ctx.pipeline().remove(this);
      ctx.fireChannelRead(new Failure(error));
      return;
    }

    RpcHeader.NegotiatePB response = parseSaslMsgResponse(msg);
    // TODO: check that the message type matches the expected one in all
    // of the below implementations.
    switch (state) {
      case AWAIT_NEGOTIATE:
        handleNegotiateResponse(ctx, response);
        break;
      case AWAIT_SASL:
        handleSaslMessage(ctx, response);
        break;
      case AWAIT_AUTHN_TOKEN_EXCHANGE:
        handleAuthnTokenExchangeResponse(ctx, response);
        break;
      case AWAIT_JWT_EXCHANGE:
        handleJwtExchangeResponse(ctx, response);
        break;
      case AWAIT_TLS_HANDSHAKE:
        handleTlsMessage(ctx, response);
        break;
      default:
        throw new IllegalStateException("received a message in unexpected state: " +
            state.toString());
    }
  }

  private void handleSaslMessage(ChannelHandlerContext ctx, NegotiatePB response)
          throws IOException {
    switch (response.getStep()) {
      case SASL_CHALLENGE:
        handleChallengeResponse(ctx, response);
        break;
      case SASL_SUCCESS:
        handleSuccessResponse(ctx, response);
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

  private void handleNegotiateResponse(ChannelHandlerContext ctx,
                                       RpcHeader.NegotiatePB response) throws IOException {
    Preconditions.checkState(response.getStep() == NegotiateStep.NEGOTIATE,
        "Expected NEGOTIATE message, got {}", response.getStep());

    // Store the supported features advertised by the server.
    serverFeatures = getFeatureFlags(response);
    // If the server supports TLS, we will always speak TLS to it.
    final boolean negotiatedTls = serverFeatures.contains(RpcFeatureFlag.TLS);
    if (!negotiatedTls && requireEncryption) {
      throw new NonRecoverableException(Status.NotAuthorized(
          "server does not support required TLS encryption"));
    }

    // Check the negotiated authentication type sent by the server.
    chosenAuthnType = chooseAuthenticationType(response);

    if (chosenAuthnType == AuthenticationTypePB.TypeCase.SASL) {
      chooseAndInitializeSaslMech(response);
    }

    // If we negotiated TLS, then we want to start the TLS handshake; otherwise,
    // we can move directly to the authentication phase.
    if (negotiatedTls) {
      startTlsHandshake(ctx);
    } else {
      startAuthentication(ctx);
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
      InetAddress local = ((InetSocketAddress)channel.localAddress()).getAddress();
      InetAddress remote = ((InetSocketAddress)channel.remoteAddress()).getAddress();
      return local.equals(remote);
    } catch (ClassCastException cce) {
      // In the off chance that we have some other type of local/remote address,
      // we'll just assume it's not loopback.
      return false;
    }
  }

  private void chooseAndInitializeSaslMech(NegotiatePB response) throws KuduException {
    securityContext.refreshSubject();
    // Gather the set of server-supported mechanisms.
    Map<String, String> errorsByMech = Maps.newHashMap();
    Set<SaslMechanism> serverMechs = Sets.newHashSet();
    for (RpcHeader.NegotiatePB.SaslMechanism mech : response.getSaslMechanismsList()) {
      switch (mech.getMechanism().toUpperCase(Locale.ENGLISH)) {
        case "GSSAPI":
          serverMechs.add(SaslMechanism.GSSAPI);
          break;
        case "PLAIN":
          serverMechs.add(SaslMechanism.PLAIN);
          break;
        default:
          errorsByMech.put(mech.getMechanism(), "unrecognized mechanism");
          break;
      }
    }

    // For each of our own mechanisms, in descending priority, check if
    // the server also supports them. If so, try to initialize saslClient.
    // If we find a common mechanism that also can be successfully initialized,
    // choose that mech.
    for (SaslMechanism clientMech : SaslMechanism.values()) {

      if (clientMech.equals(SaslMechanism.GSSAPI)) {
        Subject s = securityContext.getSubject();
        if (s == null ||
            s.getPrivateCredentials(KerberosTicket.class).isEmpty()) {
          errorsByMech.put(clientMech.name(), "client does not have Kerberos credentials (tgt)");
          continue;
        }
        if (SecurityUtil.isTgtExpired(s)) {
          errorsByMech.put(clientMech.name(), "client Kerberos credentials (TGT) have expired");
          continue;
        }
      }

      if (!serverMechs.contains(clientMech)) {
        errorsByMech.put(clientMech.name(), "not advertised by server");
        continue;
      }
      Map<String, String> props = Maps.newHashMap();
      // If the negotiated mechanism is GSSAPI (Kerberos), configure SASL to use
      // integrity protection so that the channel bindings and nonce can be
      // verified.
      if (clientMech == SaslMechanism.GSSAPI) {
        props.put(Sasl.QOP, "auth-int");
      }

      try {
        saslClient = Sasl.createSaslClient(new String[]{ clientMech.name() },
                                           null,
                                           saslProtocolName,
                                           remoteHostname,
                                           props,
                                           saslCallback);
        chosenMech = clientMech;
        break;
      } catch (SaslException e) {
        errorsByMech.put(clientMech.name(), e.getMessage());
      }
    }

    if (chosenMech != null) {
      LOG.debug("SASL mechanism {} chosen for peer {}", chosenMech.name(), remoteHostname);
      if (chosenMech.equals(SaslMechanism.PLAIN) && requireAuthentication) {
        String message = "client requires authentication, " +
            "but server does not have Kerberos enabled";
        throw new NonRecoverableException(Status.NotAuthorized(message));
      }
      return;
    }

    // TODO(KUDU-1948): when the Java client has an option to require security, detect the case
    // where the server is configured without Kerberos and the client requires it.
    String message;
    if (serverMechs.size() == 1 && serverMechs.contains(SaslMechanism.GSSAPI)) {
      // Give a better error diagnostic for common case of an unauthenticated client connecting
      // to a secure server.
      message = "server requires authentication, but " +
          errorsByMech.get(SaslMechanism.GSSAPI.name());
    } else {
      message = "client/server supported SASL mechanism mismatch: [" +
                Joiner.on(", ").withKeyValueSeparator(": ").join(errorsByMech) + "]";
    }

    if (authnTokenNotUsedReason != null) {
      message += ". Authentication tokens were not used because " +
          authnTokenNotUsedReason.msg;
    }

    // If client has valid secondary authn credentials (such as authn token),
    // but it does not have primary authn credentials (such as Kerberos creds),
    // throw a recoverable exception. So that the request can be retried as long
    // as the original call hasn't timed out, for cases documented in KUDU-2267,
    // e.g. masters are in the process of the very first leader election after
    // started up and does not have CA signed cert.
    if (authnToken != null) {
      throw new RecoverableException(Status.NotAuthorized(message));
    } else {
      throw new NonRecoverableException(Status.NotAuthorized(message));
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
        if (authnToken != null) {
          authnTokenNotUsedReason = AuthnTokenNotUsedReason.NOT_CHOSEN_BY_SERVER;
        }
        break;
      case TOKEN:
        if (authnToken == null) {
          // TODO(todd): should we also check whether we have a CA cert?
          // it seems like this should have the same logic as whether we advertised it
          throw new IllegalArgumentException("server chose token authentication " +
              "but client had no valid token");
        }
        break;
      case JWT:
        if (jsonWebToken == null) {
          throw new IllegalArgumentException("server chose JWT authentication " +
              "but client had no valid JWT");
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
  private void startTlsHandshake(ChannelHandlerContext ctx) throws SSLException {
    SSLEngine engine;
    switch (chosenAuthnType) {
      case SASL:
        engine = securityContext.createSSLEngineTrustAll();
        break;
      case TOKEN:
      case JWT:
        engine = securityContext.createSSLEngine();
        break;
      default:
        throw new AssertionError("unreachable");
    }
    engine.setUseClientMode(true);

    // Set the preferred cipher suites.
    {
      Set<String> supported = Sets.newHashSet(engine.getSupportedCipherSuites());
      List<String> toEnable = Lists.newArrayList();
      for (String c: PREFERRED_CIPHER_SUITES) {
        if (supported.contains(c)) {
          toEnable.add(c);
        }
      }
      if (toEnable.isEmpty()) {
        // This should never be the case given the cipher suites we picked are
        // supported by the standard JDK, but just in case, better to have a clear
        // exception.
        throw new RuntimeException("found no preferred cipher suite among supported: " +
            Joiner.on(',').join(supported));
      }
      engine.setEnabledCipherSuites(toEnable.toArray(new String[0]));
    }

    // Enable preferred TLS protocols, if supported. This is to match the set
    // of TLS protocols supported by Kudu servers: no other protocols need to
    // be enabled. In addition, this is to enable TLSv1.3 in Java 8. The latest
    // builds of OpenJDK 8 and Oracle JDK 8 support TLSv1.3, but TLSv1.3 is not
    // enabled by default for SSLEngine.
    // For example, see Oracle JDK 8u261 update release notes at
    // https://www.oracle.com/java/technologies/javase/8u261-relnotes.html
    // TLSv1.3 is enabled by default in Java 11, at least with OpenJDK.
    {
      Set<String> supported = Sets.newHashSet(engine.getSupportedProtocols());
      List<String> toEnable = Lists.newArrayList();
      for (String p : PREFERRED_PROTOCOLS) {
        if (supported.contains(p)) {
          toEnable.add(p);
        }
      }
      if (toEnable.isEmpty()) {
        // This should never be the case given that at least one preferred TLS
        // protocol (TLSv1) is supported by the standard JDK. It's better to
        // have a clear exception, just in case.
        throw new RuntimeException("found no preferred TLS protocol among supported: " +
            Joiner.on(',').join(supported));
      }
      engine.setEnabledProtocols(toEnable.toArray(new String[0]));
    }

    // TODO(aserbin): maybe, check that at least one cipher is enabled per each
    //                enabled protocol?

    SharableSslHandler handler = new SharableSslHandler(engine);

    sslEmbedder = new EmbeddedChannel(handler);
    sslHandshakeFuture = handler.handshakeFuture();
    state = State.AWAIT_TLS_HANDSHAKE;
    boolean sent = sendPendingOutboundTls(ctx);
    assert sent;
  }

  /**
   * Handle an inbound message during the TLS handshake. If this message
   * causes the handshake to complete, triggers the beginning of SASL initiation.
   */
  private void handleTlsMessage(ChannelHandlerContext ctx, NegotiatePB response)
          throws IOException {
    Preconditions.checkState(response.getStep() == NegotiateStep.TLS_HANDSHAKE);
    Preconditions.checkArgument(!response.getTlsHandshake().isEmpty(),
        "empty TLS message from server");

    // Pass the TLS message into our embedded SslHandler.
    sslEmbedder.writeInbound(Unpooled.copiedBuffer(
        response.getTlsHandshake().asReadOnlyByteBuffer()));
    sslEmbedder.flush();
    if (sendPendingOutboundTls(ctx)) {
      // Data was sent -- we must continue the handshake process.
      return;
    }

    // The handshake completed.
    // Insert the SSL handler into the pipeline so that all following traffic
    // gets encrypted, and then move on to the SASL portion of negotiation.
    //
    // NOTE: this takes effect immediately (i.e. the following SASL initiation
    // sequence is encrypted).
    SharableSslHandler handler = (SharableSslHandler) sslEmbedder.pipeline().first();
    handler.resetAdded();
    Certificate[] certs = handler.engine().getSession().getPeerCertificates();
    if (certs.length == 0) {
      throw new SSLPeerUnverifiedException("no peer cert found");
    }

    // The first element of the array is the peer's own certificate.
    peerCert = certs[0];

    // Don't wrap the TLS socket if we are using TLS for authentication only.
    boolean isAuthOnly = serverFeatures.contains(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY) &&
        isLoopbackConnection(ctx.channel()) && !encryptLoopback;
    if (!isAuthOnly) {
      ctx.pipeline().addFirst("tls", handler);
    }
    startAuthentication(ctx);
  }

  /**
   * If the embedded SslHandler has data to send outbound, gather
   * it all, send it tunneled in a NegotiatePB message, and return true.
   *
   * Otherwise, indicates that the handshake is complete by returning false.
   */
  private boolean sendPendingOutboundTls(ChannelHandlerContext ctx) {
    // The SslHandler can generate multiple TLS messages in response
    // (e.g. ClientKeyExchange, ChangeCipherSpec, ClientFinished).
    // We poll the handler until it stops giving us buffers.
    List<ByteString> bufs = Lists.newArrayList();
    while (!sslEmbedder.outboundMessages().isEmpty()) {
      ByteBuf msg = sslEmbedder.readOutbound();
      bufs.add(ByteString.copyFrom(msg.nioBuffer()));
      // Release the reference counted ByteBuf to avoid leaks now that we are done with it.
      // https://netty.io/wiki/reference-counted-objects.html
      msg.release();
    }
    ByteString data = ByteString.copyFrom(bufs);
    if (sslHandshakeFuture.isDone()) {
      // TODO(todd): should check sslHandshakeFuture.isSuccess()
      if (!data.isEmpty()) {
        // This is a case of TLSv1.3 protocol.
        sendTunneledTls(ctx, data);
      }
      return false;
    } else {
      assert data.size() > 0;
      sendTunneledTls(ctx, data);
      return true;
    }
  }

  /**
   * Send a buffer of data for the TLS handshake, encapsulated in the
   * appropriate TLS_HANDSHAKE negotiation message.
   */
  private void sendTunneledTls(ChannelHandlerContext ctx, ByteString buf) {
    sendSaslMessage(ctx, NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TLS_HANDSHAKE)
        .setTlsHandshake(buf)
        .build());
  }

  private void startAuthentication(ChannelHandlerContext ctx)
          throws SaslException, NonRecoverableException {
    switch (chosenAuthnType) {
      case SASL:
        sendSaslInitiate(ctx);
        break;
      case TOKEN:
        sendTokenExchange(ctx);
        break;
      case JWT:
        sendJwtExchange(ctx);
        break;
      default:
        throw new AssertionError("unreachable");
    }
  }

  private void sendTokenExchange(ChannelHandlerContext ctx) {
    // We must not send authn token unless we have successfully finished
    // authenticating via TLS.
    Preconditions.checkNotNull(authnToken);
    Preconditions.checkNotNull(sslHandshakeFuture);
    Preconditions.checkState(sslHandshakeFuture.isSuccess());

    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TOKEN_EXCHANGE)
        .setAuthnToken(authnToken);
    state = State.AWAIT_AUTHN_TOKEN_EXCHANGE;
    sendSaslMessage(ctx, builder.build());
  }

  private void sendJwtExchange(ChannelHandlerContext ctx) {
    // We must not send JWT unless we have successfully finished
    // authenticating via TLS.
    Preconditions.checkNotNull(jsonWebToken);
    Preconditions.checkNotNull(sslHandshakeFuture);
    Preconditions.checkState(sslHandshakeFuture.isSuccess());

    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder()
        .setStep(NegotiateStep.JWT_EXCHANGE)
        .setJwtRaw(jsonWebToken);
    state = State.AWAIT_JWT_EXCHANGE;
    sendSaslMessage(ctx, builder.build());
  }

  private void handleAuthnTokenExchangeResponse(ChannelHandlerContext ctx, NegotiatePB response)
      throws SaslException {
    Preconditions.checkArgument(response.getStep() == NegotiateStep.TOKEN_EXCHANGE,
        "expected TOKEN_EXCHANGE, got step: {}", response.getStep());

    // The authn token response doesn't have any actual data in it, so we can just move on.
    finish(ctx);
  }

  private void handleJwtExchangeResponse(ChannelHandlerContext ctx, NegotiatePB response)
      throws SaslException {
    Preconditions.checkArgument(response.getStep() == NegotiateStep.JWT_EXCHANGE,
        "expected JWT_EXCHANGE, got step: {}", response.getStep());

    // The JWT response doesn't have any actual data in it, so we can just move on.
    finish(ctx);
  }

  private void sendSaslInitiate(ChannelHandlerContext ctx)
          throws SaslException, NonRecoverableException {
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    if (saslClient.hasInitialResponse()) {
      byte[] initialResponse = evaluateChallenge(new byte[0]);
      builder.setToken(UnsafeByteOperations.unsafeWrap(initialResponse));
    }
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_INITIATE);
    builder.addSaslMechanismsBuilder().setMechanism(chosenMech.name());
    state = State.AWAIT_SASL;
    sendSaslMessage(ctx, builder.build());
  }

  private void handleChallengeResponse(ChannelHandlerContext ctx, RpcHeader.NegotiatePB response)
      throws SaslException, NonRecoverableException {
    byte[] saslToken = evaluateChallenge(response.getToken().toByteArray());
    if (saslToken == null) {
      throw new IllegalStateException("Not expecting an empty token");
    }
    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    builder.setToken(UnsafeByteOperations.unsafeWrap(saslToken));
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_RESPONSE);
    sendSaslMessage(ctx, builder.build());
  }

  /**
   * Verify the channel bindings included in 'response'. This is used only
   * for GSSAPI-authenticated connections over TLS.
   * @throws SSLPeerUnverifiedException on failure to verify
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

  private void handleSuccessResponse(ChannelHandlerContext ctx, NegotiatePB response)
          throws IOException {
    Preconditions.checkState(saslClient.isComplete(),
            "server sent SASL_SUCCESS step, but SASL negotiation is not complete");
    if (chosenMech == SaslMechanism.GSSAPI) {
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

    finish(ctx);
  }

  /**
   * Marks the negotiation as finished, and sends the connection context to the server.
   * @param ctx the connection context
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  private void finish(ChannelHandlerContext ctx) throws SaslException {
    state = State.FINISHED;
    ctx.pipeline().remove(this);

    ctx.writeAndFlush(makeConnectionContext(), ctx.voidPromise());
    LOG.debug("Authenticated connection {} using {}/{}",
            ctx.channel(), chosenAuthnType, chosenMech);
    ctx.fireChannelRead(new Success(serverFeatures));
  }

  private RpcOutboundMessage makeConnectionContext() throws SaslException {
    RpcHeader.ConnectionContextPB.Builder builder = RpcHeader.ConnectionContextPB.newBuilder();

    // The UserInformationPB is deprecated, but used by servers prior to Kudu 1.1.
    RpcHeader.UserInformationPB.Builder userBuilder = RpcHeader.UserInformationPB.newBuilder();
    String user = securityContext.getRealUser();
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

  private byte[] evaluateChallenge(final byte[] challenge)
      throws SaslException, NonRecoverableException {
    try {
      return Subject.doAs(securityContext.getSubject(),
          new PrivilegedExceptionAction<byte[]>() {
            @Override
            public byte[] run() throws SaslException {
              return saslClient.evaluateChallenge(challenge);
            }
          });
    } catch (PrivilegedActionException e) {
      // This cast is safe because the action above only throws checked SaslException.
      SaslException saslException = (SaslException) e.getCause();

      // TODO(KUDU-2121): We should never get to this point if the client does not have
      // Kerberos credentials, but it seems that on certain platforms it can happen.
      // So, we try and determine whether the evaluateChallenge failed due to missing
      // credentials, and return a nicer error message if so.
      Throwable cause = saslException.getCause();
      if (cause instanceof GSSException &&
          ((GSSException) cause).getMajor() == GSSException.NO_CRED) {
        throw new NonRecoverableException(
            Status.ConfigurationError(
                "Server requires Kerberos, but this client is not authenticated " +
                "(missing or expired TGT)"),
            saslException);
      }
      throw saslException;
    }
  }

  private class SaslClientCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          ((NameCallback) callback).setName(securityContext.getRealUser());
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
  static class Success {
    final Set<RpcFeatureFlag> serverFeatures;

    public Success(Set<RpcFeatureFlag> serverFeatures) {
      this.serverFeatures = serverFeatures;
    }
  }

  /**
   * The results of a failed negotiation. This is sent to upstream handlers in the Netty pipeline
   * when a negotiation fails.
   */
  static class Failure {
    /** The RPC error received from the server. */
    final RpcHeader.ErrorStatusPB status;

    public Failure(RpcHeader.ErrorStatusPB status) {
      this.status = status;
    }
  }

  /**
   * A hack to allow sharing the SslHandler even though it's not annotated as "Sharable".
   * We aren't technically sharing it, but when we move it from the EmbeddedChannel to
   * the actual channel above the sharing validation runs and throws an exception.
   *
   * https://netty.io/wiki/new-and-noteworthy-in-4.0.html#well-defined-thread-model
   * https://netty.io/4.0/api/io/netty/channel/ChannelHandler.Sharable.html
   *
   * TODO (ghenke): Remove the need for this reflection.
   */
  static class SharableSslHandler extends SslHandler {

    public SharableSslHandler(SSLEngine engine) {
      super(engine);
    }

    void resetAdded() {
      Field addedField = AccessController.doPrivileged((PrivilegedAction<Field>) () -> {
        try {
          Class<?> c = ChannelHandlerAdapter.class;
          Field added = c.getDeclaredField("added");
          added.setAccessible(true);
          return added;
        } catch (NoSuchFieldException e) {
          throw new RuntimeException(e);
        }
      });
      try {
        addedField.setBoolean(this, false);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
