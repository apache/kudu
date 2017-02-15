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

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.rpc.RpcHeader.RequestHeader;

/**
 * An RPC header and associated body protobuf which can be sent outbound
 * through the Netty pipeline. The 'Encoder' inner class is responsible
 * for serializing these instances into wire-format-compatible buffers.
 */
class RpcOutboundMessage {
  private static final Logger LOG = LoggerFactory.getLogger(RpcOutboundMessage.class);

  private final RequestHeader.Builder headerBuilder;
  private final Message body;

  RpcOutboundMessage(RequestHeader.Builder header, Message body) {
    this.headerBuilder = header;
    this.body = body;
  }

  public RequestHeader.Builder getHeaderBuilder() {
    return headerBuilder;
  }

  public Message getBody() {
    return body;
  }

  @Override
  public String toString() {
    // TODO(todd): should this redact? it's only used at TRACE level, so hopefully OK.
    return "RpcOutboundMessage[header={" + TextFormat.shortDebugString(headerBuilder) +
        "}, body={" + TextFormat.shortDebugString(body) + "}]";
  }

  /**
   * Netty encoder implementation to serialize outbound messages.
   */
  static class Encoder extends OneToOneEncoder {
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel chan,
        Object obj) throws Exception {
      if (!(obj instanceof RpcOutboundMessage)) {
        return obj;
      }
      RpcOutboundMessage msg = (RpcOutboundMessage)obj;
      if (LOG.isTraceEnabled()) {
        LOG.trace("{}: sending RPC {}", chan, msg);
      }
      // TODO(todd): move this impl into this class and remove external
      // callers.
      return KuduRpc.toChannelBuffer(msg.getHeaderBuilder().build(),
          msg.getBody());
    }
  }
}
