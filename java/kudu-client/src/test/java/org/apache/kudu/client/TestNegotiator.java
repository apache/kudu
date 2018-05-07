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
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.ssl.SslHandler;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.Negotiator.Success;
import org.apache.kudu.rpc.RpcHeader.AuthenticationTypePB;
import org.apache.kudu.rpc.RpcHeader.ConnectionContextPB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.SaslMechanism;
import org.apache.kudu.rpc.RpcHeader.ResponseHeader;
import org.apache.kudu.rpc.RpcHeader.RpcFeatureFlag;
import org.apache.kudu.security.Token.SignedTokenPB;
import org.apache.kudu.util.SecurityUtil;

public class TestNegotiator {
  static final Logger LOG = LoggerFactory.getLogger(TestNegotiator.class);

  private DecoderEmbedder<Object> embedder;
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

  @Before
  public void setup() {
    serverEngine = createServerEngine();
    serverEngine.setUseClientMode(false);
    secContext = new SecurityContext();
  }

  private void startNegotiation(boolean fakeLoopback) {
    Negotiator negotiator = new Negotiator("127.0.0.1", secContext, false);
    negotiator.overrideLoopbackForTests = fakeLoopback;
    embedder = new DecoderEmbedder<Object>(negotiator);
    negotiator.sendHello(embedder.getPipeline().getChannel());
  }

  static CallResponse fakeResponse(ResponseHeader header, Message body) {
    ChannelBuffer buf = KuduRpc.toChannelBuffer(header, body);
    buf = buf.slice(4, buf.readableBytes() - 4);
    return new CallResponse(buf);
  }

  KeyStore loadTestKeystore() throws Exception {
    KeyStore ks = KeyStore.getInstance("JKS");
    ks.load(TestNegotiator.class.getResourceAsStream("/test-key-and-cert.jks"),
        KEYSTORE_PASSWORD);
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
  private Success assertComplete() {
    RpcOutboundMessage msg = (RpcOutboundMessage)embedder.poll();
    ConnectionContextPB connCtx = (ConnectionContextPB)msg.getBody();
    assertEquals(Negotiator.CONNECTION_CTX_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(System.getProperty("user.name"), connCtx.getDEPRECATEDUserInfo().getRealUser());

    // Expect the client to also emit a negotiation Success.
    Success success = (Success)embedder.poll();
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
  public void testNegotiation() {
    startNegotiation(false);

    // Expect client->server: NEGOTIATE.
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());

    // Respond with NEGOTIATE.
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));

    // Expect client->server: SASL_INITIATE (PLAIN)
    msg = (RpcOutboundMessage)embedder.poll();
    body = (NegotiatePB) msg.getBody();

    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeaderBuilder().getCallId());
    assertEquals(NegotiateStep.SASL_INITIATE, body.getStep());
    assertEquals(1, body.getSaslMechanismsCount());
    assertEquals("PLAIN", body.getSaslMechanisms(0).getMechanism());
    assertTrue(body.hasToken());

    // Respond with SASL_SUCCESS:
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .setStep(NegotiateStep.SASL_SUCCESS)
          .build()));

    // Expect client->server: ConnectionContext
    assertComplete();
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
  private void runTlsHandshake() throws SSLException {
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.TLS_HANDSHAKE, body.getStep());

    // Consume the ClientHello in our fake server, which should generate ServerHello.
    embedder.offer(runServerStep(serverEngine, body.getTlsHandshake()));

    // Expect client to generate ClientKeyExchange, ChangeCipherSpec, Finished.
    msg = (RpcOutboundMessage) embedder.poll();
    body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.TLS_HANDSHAKE, body.getStep());

    // Server consumes the above. Should send the TLS "Finished" message.
    embedder.offer(runServerStep(serverEngine, body.getTlsHandshake()));
  }

  @Test
  public void testTlsNegotiation() throws Exception {
    startNegotiation(false);

    // Expect client->server: NEGOTIATE, TLS included.
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());
    assertTrue(body.getSupportedFeaturesList().contains(RpcFeatureFlag.TLS));

    // Fake a server response with TLS enabled.
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(NegotiatePB.SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));

    // Expect client->server: TLS_HANDSHAKE.
    runTlsHandshake();

    // The pipeline should now have an SSL handler as the first handler.
    assertTrue(embedder.getPipeline().getFirst() instanceof SslHandler);

    // The Negotiator should have sent the SASL_INITIATE at this point.
    // NOTE: in a non-mock environment, this message would now be encrypted
    // by the newly-added TLS handler. But, with the DecoderEmbedder that we're
    // using, we don't actually end up processing outbound events. Upgrading
    // to Netty 4 and using EmbeddedChannel instead would make this more realistic.
    msg = (RpcOutboundMessage) embedder.poll();
    body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.SASL_INITIATE, body.getStep());
  }

  @Test
  public void testTlsNegotiationAuthOnly() throws Exception {
    startNegotiation(true);

    // Expect client->server: NEGOTIATE, TLS and TLS_AUTHENTICATION_ONLY included.
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(NegotiateStep.NEGOTIATE, body.getStep());
    assertTrue(body.getSupportedFeaturesList().contains(RpcFeatureFlag.TLS));
    assertTrue(body.getSupportedFeaturesList().contains(
        RpcFeatureFlag.TLS_AUTHENTICATION_ONLY));

    // Fake a server response with TLS and TLS_AUTHENTICATION_ONLY enabled.
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSaslMechanisms(NegotiatePB.SaslMechanism.newBuilder().setMechanism("PLAIN"))
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .addSupportedFeatures(RpcFeatureFlag.TLS_AUTHENTICATION_ONLY)
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));

    // Expect client->server: TLS_HANDSHAKE.
    runTlsHandshake();

    // The pipeline should *not* have an SSL handler as the first handler,
    // since we used TLS for authentication only.
    assertFalse(embedder.getPipeline().getFirst() instanceof SslHandler);

    // The Negotiator should have sent the SASL_INITIATE at this point.
    msg = (RpcOutboundMessage) embedder.poll();
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
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
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
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals("supported_features: APPLICATION_FEATURE_FLAGS " +
        "supported_features: TLS " +
        "step: NEGOTIATE " +
        "authn_types { sasl { } } " +
        "authn_types { token { } }", TextFormat.shortDebugString(body));

    // Fake a server response with TLS enabled and TOKEN chosen.
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
          .addSupportedFeatures(RpcFeatureFlag.TLS)
          .addAuthnTypes(AuthenticationTypePB.newBuilder().setToken(
              AuthenticationTypePB.Token.getDefaultInstance()))
          .setStep(NegotiateStep.NEGOTIATE)
          .build()));

    // Expect to now run the TLS handshake
    runTlsHandshake();

    // Expect the client to send the token.
    msg = (RpcOutboundMessage) embedder.poll();
    body = (NegotiatePB) msg.getBody();
    assertEquals("step: TOKEN_EXCHANGE authn_token { }",
        TextFormat.shortDebugString(body));

    // Fake a response indicating success.
    embedder.offer(fakeResponse(
        ResponseHeader.newBuilder().setCallId(Negotiator.SASL_CALL_ID).build(),
        NegotiatePB.newBuilder()
        .setStep(NegotiateStep.TOKEN_EXCHANGE)
          .build()));

    // Should be complete now.
    assertComplete();
  }
}
