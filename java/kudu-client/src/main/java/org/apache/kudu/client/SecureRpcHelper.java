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
import java.util.Map;
import java.util.Set;

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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ZeroCopyLiteralByteString;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.rpc.RpcHeader;

@InterfaceAudience.Private
public class SecureRpcHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TabletClient.class);

  private static final Map<String, String> SASL_PROPS = ImmutableMap.of();
  private static final SaslClientCallbackHandler SASL_CALLBACK = new SaslClientCallbackHandler();

  /**
   * List of SASL mechanisms supported by the client, in descending priority order.
   * The client will pick the first of these mechanisms that is supported by
   * the server and also succeeds to initialize.
   */
  private static final String[] PRIORITIZED_MECHS = new String[] { "GSSAPI", "PLAIN" };

  static final String USER_AND_PASSWORD = "java_client";

  private final TabletClient client;
  private SaslClient saslClient;
  private static final int SASL_CALL_ID = -33;
  private static final Set<RpcHeader.RpcFeatureFlag> SUPPORTED_RPC_FEATURES =
      ImmutableSet.of(RpcHeader.RpcFeatureFlag.APPLICATION_FEATURE_FLAGS);
  private volatile boolean negoUnderway = true;
  private Set<RpcHeader.RpcFeatureFlag> serverFeatures;

  public SecureRpcHelper(final TabletClient client) {
    this.client = client;
  }

  public Set<RpcHeader.RpcFeatureFlag> getServerFeatures() {
    Preconditions.checkState(!negoUnderway);
    Preconditions.checkNotNull(serverFeatures);
    return serverFeatures;
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
    sendSaslMessage(channel, builder.build());
  }

  private void sendSaslMessage(Channel channel, RpcHeader.NegotiatePB msg) {
    Preconditions.checkNotNull(channel);
    RpcHeader.RequestHeader.Builder builder = RpcHeader.RequestHeader.newBuilder();
    builder.setCallId(SASL_CALL_ID);
    RpcHeader.RequestHeader header = builder.build();

    ChannelBuffer buffer = KuduRpc.toChannelBuffer(header, msg);
    Channels.write(channel, buffer);
  }

  public ChannelBuffer handleResponse(ChannelBuffer buf, Channel chan) throws SaslException {
    if (negoUnderway) {
      RpcHeader.NegotiatePB response = parseSaslMsgResponse(buf);
      switch (response.getStep()) {
        case NEGOTIATE:
          handleNegotiateResponse(chan, response);
          break;
        case SASL_CHALLENGE:
          handleChallengeResponse(chan, response);
          break;
        case SASL_SUCCESS:
          handleSuccessResponse(chan);
          break;
        default:
          LOG.error(String.format("Wrong negotiation step: %s", response.getStep()));
      }
      return null;
    }
    return buf;
  }


  private RpcHeader.NegotiatePB parseSaslMsgResponse(ChannelBuffer buf) {
    CallResponse response = new CallResponse(buf);
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
      SaslException {
    Preconditions.checkNotNull(chan);
    // Store the supported features advertised by the server.
    ImmutableSet.Builder<RpcHeader.RpcFeatureFlag> features = ImmutableSet.builder();
    for (RpcHeader.RpcFeatureFlag feature : response.getSupportedFeaturesList()) {
      if (SUPPORTED_RPC_FEATURES.contains(feature)) {
        features.add(feature);
      }
    }
    serverFeatures = features.build();

    // Gather the set of server-supported mechanisms.
    Set<String> serverMechs = Sets.newHashSet();
    for (RpcHeader.NegotiatePB.SaslMechanism mech : response.getSaslMechanismsList()) {
      serverMechs.add(mech.getMechanism());
    }

    // For each of our own mechanisms, in descending priority, check if
    // the server also supports them. If so, try to initialize saslClient.
    // If we find a common mechanism that also can be successfully initialized,
    // choose that mech.
    byte[] initialResponse = null;
    String chosenMech = null;
    Map<String, String> errorsByMech = Maps.newHashMap();
    for (String clientMech : PRIORITIZED_MECHS) {
      if (!serverMechs.contains(clientMech)) {
        errorsByMech.put(clientMech, "not advertised by server");
        continue;
      }
      try {
        saslClient = Sasl.createSaslClient(new String[]{ clientMech },
                                           null,
                                           "kudu",
                                           client.getServerInfo().getHostname(),
                                           SASL_PROPS,
                                           SASL_CALLBACK);
        if (saslClient.hasInitialResponse()) {
          initialResponse = evaluateChallenge(new byte[0]);
        }
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

    RpcHeader.NegotiatePB.Builder builder = RpcHeader.NegotiatePB.newBuilder();
    if (initialResponse != null) {
      builder.setToken(ZeroCopyLiteralByteString.wrap(initialResponse));
    }
    builder.setStep(RpcHeader.NegotiatePB.NegotiateStep.SASL_INITIATE);
    builder.addSaslMechanismsBuilder().setMechanism(chosenMech);
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
    LOG.debug("nego finished");
    negoUnderway = false;
    client.sendContext(chan);
  }

  private byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
    final Subject subject = client.getSubject();
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
}
