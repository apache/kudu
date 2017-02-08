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

import static org.junit.Assert.*;

import java.security.AccessControlContext;
import java.security.AccessController;

import javax.security.auth.Subject;

import org.apache.kudu.client.Negotiator.Result;
import org.apache.kudu.rpc.RpcHeader.ConnectionContextPB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.NegotiateStep;
import org.apache.kudu.rpc.RpcHeader.NegotiatePB.SaslMechanism;
import org.apache.kudu.rpc.RpcHeader.ResponseHeader;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Message;

public class TestNegotiator {
  private Negotiator negotiator;
  private DecoderEmbedder<Object> embedder;

  @Before
  public void setup() {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    negotiator = new Negotiator(subject, "127.0.0.1");
    embedder = new DecoderEmbedder<Object>(negotiator);
  }

  static CallResponse fakeResponse(ResponseHeader header, Message body) {
    ChannelBuffer buf = KuduRpc.toChannelBuffer(header, body);
    buf = buf.slice(4, buf.readableBytes() - 4);
    return new CallResponse(buf);
  }

  /**
   * Simple test case for a PLAIN negotiation.
   */
  @Test
  public void testNegotiation() {
    negotiator.sendHello(embedder.getPipeline().getChannel());

    // Expect client->server: NEGOTIATE.
    RpcOutboundMessage msg = (RpcOutboundMessage) embedder.poll();
    NegotiatePB body = (NegotiatePB) msg.getBody();
    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeader().getCallId());
    assertEquals(NegotiateStep.NEGOTIATE, ((NegotiatePB)msg.getBody()).getStep());

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

    assertEquals(Negotiator.SASL_CALL_ID, msg.getHeader().getCallId());
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
    msg = (RpcOutboundMessage)embedder.poll();
    ConnectionContextPB connCtx = (ConnectionContextPB)msg.getBody();
    assertEquals(Negotiator.CONNECTION_CTX_CALL_ID, msg.getHeader().getCallId());
    assertEquals("java_client", connCtx.getDEPRECATEDUserInfo().getRealUser());

    // Expect the client to also emit a negotiation Result.
    Result result = (Result)embedder.poll();
    assertNotNull(result);
  }

}
