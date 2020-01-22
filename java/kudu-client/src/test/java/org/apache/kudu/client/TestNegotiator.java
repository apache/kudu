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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.Negotiator.Success;
import org.apache.kudu.rpc.RpcHeader;
import org.apache.kudu.rpc.RpcHeader.AuthenticationTypePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.SaslMechanism;
import org.apache.kudu.rpc.RpcHeader.ResponseHeader;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.util.SecurityUtil;

public class TestNegotiator {
  static final Logger LOG = LoggerFactory.getLogger(TestNegotiator.class);

  private EmbeddedChannel embedder;
  private SecurityContext secContext;
  private SSLEngine serverEngine;

  private static final char[] KEYSTORE_PASSWORD = "password".toCharArray();

  /**
   * The cert stored in the keystore, in base64ed DER format.
   * The real certs we'll get from the server will not be in Base64,
   * but the CertificateFactory also supports binary DER.
   */
  private static final String CA_CERT_DER =
      "-----BEGIN CERTIFICATE-----\n" +
      "MIIDXTCCAkWgAwIBAgIJAOOmFHYkBz4rMA0GCSqGSIb3DQEBCwUAMEUxCzAJBgNVBAYTAkFVMRMw" +
      "EQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwHhcN" +
      "MTYxMTAyMjI0OTQ5WhcNMTcwMjEwMjI0OTQ5WjBFMQswCQYDVQQGEwJBVTETMBEGA1UECAwKU29t" +
      "ZS1TdGF0ZTEhMB8GA1UECgwYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMIIBIjANBgkqhkiG9w0B" +
      "AQEFAAOCAQ8AMIIBCgKCAQEAppo9GwiDisQVYAF9NXl8ykqo0MIi5rfNwiE9kUWbZ2ejzxs+1Cf7" +
      "WCn4mzbkJx5ZscRjhnNb6dJxtZJeid/qgiNVBcNzh35H8J+ao0tEbHjCs7rKOX0etsFUp4GQwYkd" +
      "fpvVBsU8ciXvkxhvt1XjSU3/YJJRAvCyGVxUQlKiVKGCD4OnFNBwMdNw7qI8ryiRv++7I9udfSuM" +
      "713yMeBtkkV7hWUfxrTgQOLsV/CS+TsSoOJ7JJqHozeZ+VYom85UqSfpIFJVzM6S7BTb6SX/vwYI" +
      "oS70gubT3HbHgDRcMvpCye1npHL9fL7B87XZn7wnnUem0eeCqWyUjJ82Uj9mQQIDAQABo1AwTjAd" +
      "BgNVHQ4EFgQUOY7rpWGoZMrmyRZ9RohPWVwyPBowHwYDVR0jBBgwFoAUOY7rpWGoZMrmyRZ9RohP" +
      "WVwyPBowDAYDVR0TBAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEATKh3io8ruqbhmopY3xQWA2pE" +
      "hs4ZSu3H+AfULMruVsXKEZjWp27nTsFaxLZYUlzeZr0EcWwZ79qkcA8Dyj+mVHhrCAPpcjsDACh1" +
      "ZdUQAgASkVS4VQvkukct3DFa3y0lz5VwQIxjoQR5y6dCvxxXT9NpRo/Z7pd4MRhEbz3NT6PScQ9f" +
      "2MTrR0NOikLdB98JlpKQbEKxzbMhWDw4J3mrmK6zdemjdCcRDsBVPswKnyAjkibXaZkpNRzjvDNA" +
      "gO88MKlArCYoyRZqIfkcSXAwwTdGQ+5GQLsY9zS49Rrhk9R7eOmDhaHybdRBDqW1JiCSmzURZAxl" +
      "nrjox4GmC3JJaA==\n" +
      "-----END CERTIFICATE-----";

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void setUp() {
    serverEngine = createServerEngine();
    serverEngine.setUseClientMode(false);
    secContext = new SecurityContext();
  }

  private void startNegotiation(boolean fakeLoopback) {
    Negotiator negotiator = new Negotiator("127.0.0.1", secContext, false);
    negotiator.overrideLoopbackForTests = fakeLoopback;
    embedder = new EmbeddedChannel(negotiator);
    negotiator.sendHello(embedder.pipeline().firstContext());
  }

  static CallResponse fakeResponse(ResponseHeader header, Message body) {
    ByteBuf buf = Unpooled.buffer();
    KuduRpc.toByteBuf(buf, header, body);
    buf = buf.slice(4, buf.readableBytes() - 4);
    return new CallResponse(buf);
  }

  KeyStore loadTestKeystore() throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    try (InputStream stream =
                 TestNegotiator.class.getResourceAsStream("/test-key-and-cert.jks")) {
      ks.load(stream, KEYSTORE_PASSWORD);
    }
    return ks;
  }

  SSLEngine createServerEngine() {
    try {
      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(loadTestKeystore(), KEYSTORE_PASSWORD);
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(kmf.getKeyManagers(), null, null);
      return ctx.createSSLEngine();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks that the client sends a connection context and then yields
   * a Negotiation.Success to the pipeline.
   * @return the result
   */
  private Success assertComplete(boolean isTls) throws Exception {
    RpcOutboundMessage msg = isTls ?
            unwrapOutboundMessage(embedder.readOutbound(),
                    RpcHeader.ConnectionContextPB.newBuilder()) :
            embedder.readOutbound();
    RpcHeader.ConnectionContextPB connCtx = (RpcHeader.ConnectionContextPB) msg.getBody();
    assertEquals(Negotiator.CONNECTION_CTX_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(System.getProperty("user.name"), connCtx.getDEPRECATEDUserInfo().getRealUser());

    // Expect the client to also emit a negotiation Success.
    Success success = embedder.readInbound();
    assertNotNull(success);
    return success;
  }

  @Test
  public void testChannelBinding() throws Exception {
    KeyStore ks = loadTestKeystore();
    Certificate cert = ks.getCertificate("1");
    byte[] bindings = SecurityUtil.getEndpointChannelBindings(cert);
    assertEquals(32, bindings.length);
  }

  /**
   * Simple test case for a PLAIN negotiation.
   */
  @Test
  public void testNegotiation() throws Exception {
    startNegotiation(false);

    // Expect client->server: NEGOTIATE.
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());

    // Respond with NEGOTIATE.
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));
    embedder.flushInbound();

    // Expect client->server: SASL_INITIATE (PLAIN)
    msg = embedder.readOutbound();
    body = (NegotiatePB) msg.getBody();

    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(NegotiateStep.SASL_INITIATE, body.getStep());
    assertEquals(1, body.getSaslMechanismsCount());
    assertEquals("PLAIN", body.getSaslMechanisms(0).getMechanism());
    assertTrue(body.hasToken());

    // Respond with SASL_SUCCESS:
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .setStep(NegotiateStep.SASL_SUCCESS)
          .build()));
    embedder.flushInbound();

    // Expect client->server: ConnectionContext
    assertComplete(/*isTls*/ false);
  }

  private static void runTasks(SSLEngineResult result,
      SSLEngine engine) {
    if (result.getHandshakeStatus() != HandshakeStatus.NEED_TASK) {
      return;
    }
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }
  }

  private static CallResponse runServerStep(SSLEngine engine,
                                            ByteString clientTlsMessage) throws SSLException {
    LOG.debug("Handling TLS message from client: {}", Bytes.hex(clientTlsMessage.toByteArray()));
    ByteBuffer dst = ByteBuffer.allocate(engine.getSession().getPacketBufferSize());
    ByteBuffer src =  ByteBuffer.wrap(clientTlsMessage.toByteArray());
    do {
      SSLEngineResult result = engine.unwrap(src, dst);
      runTasks(result, engine);
    } while (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_UNWRAP);

    if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
      // The server has more to send.
      // Produce the ServerHello and send it back to the client.
      List<ByteString> bufs = Lists.newArrayList();
      while (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
        dst.clear();
        runTasks(engine.wrap(ByteBuffer.allocate(0), dst), engine);
        dst.flip();
        bufs.add(ByteString.copyFrom(dst));
      }
      return fakeResponse(
          ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
          NegotiatePB.newBuilder()
            .setTlsHandshake(ByteString.copyFrom(bufs))
            .setStep(NegotiateStep.TLS_HANDSHAKE)
            .build());
    } else if (engine.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
      // Handshake complete.
      return null;
    } else {
      throw new AssertionError("unexpected state: " + engine.getHandshakeStatus());
    }
  }

  /**
   * Completes the 3-step TLS handshake, assuming that the client is
   * about to generate the first of the messages.
   */
  private void runTlsHandshake(boolean isAuthOnly) throws SSLException {
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.TLS_HANDSHAKE, body.getStep());

    // Consume the ClientHello in our fake server, which should generate ServerHello.
    embedder.writeInbound(runServerStep(serverEngine, body.getTlsHandshake()));
    embedder.flushInbound();

    // Expect client to generate ClientKeyExchange, ChangeCipherSpec, Finished.
    msg = embedder.readOutbound();
    body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.TLS_HANDSHAKE, body.getStep());

    // Now that the handshake is complete, we need to encode RpcOutboundMessages
    // to ByteBuf to be accepted by the the SslHandler.
    // This encoder is added to the pipeline by the Connection in normal Negotiator usage.
    if (!isAuthOnly) {
      embedder.pipeline().addFirst("encode-outbound", new RpcOutboundMessage.Encoder());
    }

    // Server consumes the above. Should send the TLS "Finished" message.
    embedder.writeInbound(runServerStep(serverEngine, body.getTlsHandshake()));
    embedder.flushInbound();
  }

  @Test
  public void testTlsNegotiation() throws Exception {
    startNegotiation(false);

    // Expect client->server: NEGOTIATE, TLS included.
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());
    assertTrue(body.getSupportedFeaturesList().contains(RpcFeatureFlag.TLS));

    // Fake a server response with TLS enabled.
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(NegotiatePB.SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));
    embedder.flushInbound();

    // Expect client->server: TLS_HANDSHAKE.
    runTlsHandshake(/*isAuthOnly*/ false);

    // The pipeline should now have an SSL handler as the first handler.
    assertTrue(embedder.pipeline().first() instanceof SslHandler);

    // The Negotiator should have sent the SASL_INITIATE at this point.
    msg = unwrapOutboundMessage(embedder.readOutbound(), RpcHeader.NegotiatePB.newBuilder());
    body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.SASL_INITIATE, body.getStep());
  }

  @Test
  public void testTlsNegotiationAuthOnly() throws Exception {
    startNegotiation(true);

    // Expect client->server: NEGOTIATE, TLS and TLS_AUTHENTICATION_ONLY included.
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());
    assertTrue(body.getSupportedFeaturesList().contains(RpcFeatureFlag.TLS));
    assertTrue(body.getSupportedFeaturesList().contains(
        RpcFeatureFlag.TLS_AUTHENTICATION_ONLY));

    // Fake a server response with TLS and TLS_AUTHENTICATION_ONLY enabled.
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(NegotiatePB.SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .addSupportedFeatures(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY)
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));
    embedder.flushInbound();

    // Expect client->server: TLS_HANDSHAKE.
    runTlsHandshake(/*isAuthOnly*/ true);

    // The pipeline should *not* have an SSL handler as the first handler,
    // since we used TLS for authentication only.
    assertFalse(embedder.pipeline().first() instanceof SslHandler);

    // The Negotiator should have sent the SASL_INITIATE at this point.
    msg = embedder.readOutbound();
    body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.SASL_INITIATE, body.getStep());
  }

  /**
   * Test that, if we don't have any trusted certs, we don't expose
   * token authentication as an option.
   */
  @Test
  public void testNoTokenAuthWhenNoTrustedCerts() throws Exception {
    secContext.setAuthenticationToken(SignedTokenPB.getDefaultInstance());
    startNegotiation(false);

    // Expect client->server: NEGOTIATE, TLS included, Token not included.
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals("supported_features: APPLICATION_FEATURE_FLAGS " +
        "supported_features: TLS " +
        "step: NEGOTIATE " +
        "authn_types { sasl { } }", TextFormat.shortDebugString(body));
  }

  /**
   * Test that, if we have a trusted CA cert, we expose token authentication
   * as an option during negotiation, and run it to completion.
   */
  @Test
  public void testTokenAuthWithTrustedCerts() throws Exception {
    secContext.trustCertificates(ImmutableList.of(ByteString.copyFromUtf8(CA_CERT_DER)));
    secContext.setAuthenticationToken(SignedTokenPB.getDefaultInstance());
    startNegotiation(false);

    // Expect client->server: NEGOTIATE, TLS included, Token included.
    RpcOutboundMessage msg = embedder.readOutbound();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals("supported_features: APPLICATION_FEATURE_FLAGS " +
        "supported_features: TLS " +
        "step: NEGOTIATE " +
        "authn_types { sasl { } } " +
        "authn_types { token { } }", TextFormat.shortDebugString(body));

    // Fake a server response with TLS enabled and TOKEN chosen.
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .addAuthnTypes(AuthenticationTypePB.newBuilder().setToken(
              AuthenticationTypePB.Token.getDefaultInstance()))
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));
    embedder.flushInbound();

    // Expect to now run the TLS handshake
    runTlsHandshake(/*isAuthOnly*/ false);

    // Expect the client to send the token.
    msg = unwrapOutboundMessage(embedder.readOutbound(), RpcHeader.NegotiatePB.newBuilder());
    body = (NegotiatePB) msg.getBody();
    assertEquals("step: TOKEN_EXCHANGE authn_token { }",
        TextFormat.shortDebugString(body));

    // Fake a response indicating success.
    embedder.writeInbound(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TOKEN_EXCHANGE)
          .build()));
    embedder.flushInbound();

    // TODO (ghenke): For some reason the SslHandler adds an extra empty message here.
    // This should be harmless, but it would be good to understand or fix why.
    ByteBuf empty = embedder.readOutbound();
    assertEquals(0, empty.readableBytes());

    // Should be complete now.
    assertComplete(/*isTls*/ true);
  }

  private RpcOutboundMessage unwrapOutboundMessage(ByteBuf wrappedBuf,
                                                   Message.Builder requestBuilder)
          throws Exception {
    // Create an SSL handle to handle unwrapping the ssl message.
    SslHandler handler = new SslHandler(serverEngine);
    EmbeddedChannel serverSSLChannel = new EmbeddedChannel(handler);

    // Pass the ssl message through the channel with the ssl handler.
    serverSSLChannel.writeInbound(wrappedBuf);
    serverSSLChannel.flushInbound();
    ByteBuf unwrappedbuf = serverSSLChannel.readInbound();

    // Read the message size and bytes.
    final int size = unwrappedbuf.readInt();
    final byte [] bytes = new byte[size];
    unwrappedbuf.getBytes(unwrappedbuf.readerIndex(), bytes);

    // Parse the message header.
    final CodedInputStream in = CodedInputStream.newInstance(bytes);
    RpcHeader.RequestHeader.Builder header = RpcHeader.RequestHeader.newBuilder();
    in.readMessage(header, ExtensionRegistry.getEmptyRegistry());

    // Parse the request message.
    in.readMessage(requestBuilder, ExtensionRegistry.getEmptyRegistry());

    return new RpcOutboundMessage(header, requestBuilder.build());
  }
}
