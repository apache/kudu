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

package org.apache.kudu.subprocess;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.WireProtocol.AppStatusPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessRequestPB;
import org.apache.kudu.subprocess.Subprocess.SubprocessResponsePB;

/**
 * The {@link MessageParser} class,
 *    1. retrieves one message from the inbound queue at a time,
 *    2. processes the message and generates a response,
 *    3. and then puts the response to the outbound queue.
 */
@InterfaceAudience.Private
class MessageParser implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MessageParser.class);
  private final BlockingQueue<InboundRequest> inboundQueue;
  private final BlockingQueue<OutboundResponse> outboundQueue;
  private final ProtocolHandler protocolHandler;

  MessageParser(BlockingQueue<InboundRequest> inboundQueue,
                BlockingQueue<OutboundResponse> outboundQueue,
                ProtocolHandler protocolHandler) {
    Preconditions.checkNotNull(inboundQueue);
    Preconditions.checkNotNull(outboundQueue);
    this.inboundQueue = inboundQueue;
    this.outboundQueue = outboundQueue;
    this.protocolHandler = protocolHandler;
  }

  @Override
  public void run() {
    while (true) {
      InboundRequest req = QueueUtil.take(inboundQueue);
      SubprocessMetrics metrics = req.metrics();
      metrics.recordInboundQueueTimeMs();

      // Record the execution time.
      metrics.startTimer();
      SubprocessResponsePB.Builder responseBuilder = parseAndExecuteRequest(req.bytes());
      metrics.recordExecutionTimeMs();

      // Begin recording the time it takes to make it through the outbound
      // queue. The writer thread will record the elapsed time right before
      // writing the response to the pipe.
      metrics.startTimer();
      QueueUtil.put(outboundQueue, new OutboundResponse(responseBuilder, metrics));
    }
  }

  /**
   * Returns a response builder with the given error status.
   *
   * @param errorCode the given error status
   * @param resp the message builder
   * @return a message with the given error status
   */
  static SubprocessResponsePB.Builder builderWithError(AppStatusPB.ErrorCode errorCode,
                                                       SubprocessResponsePB.Builder resp) {
    Preconditions.checkNotNull(resp);
    AppStatusPB.Builder errorBuilder = AppStatusPB.newBuilder();
    errorBuilder.setCode(errorCode);
    resp.setError(errorBuilder);
    return resp;
  }

  /**
   * Parses the given protobuf request and executes it, returning a builder for
   * the response. If a InvalidProtocolBufferException is thrown, which
   * indicates the message is invalid, the builder will contain an error
   * message.
   *
   * @param data the protobuf message
   * @return a SubprocessResponsePB
   */
  private SubprocessResponsePB.Builder parseAndExecuteRequest(byte[] data) {
    SubprocessResponsePB.Builder responseBuilder = SubprocessResponsePB.newBuilder();
    try {
      // Parses the data as a message of SubprocessRequestPB type.
      SubprocessRequestPB request = SubprocessRequestPB.parser().parseFrom(data);
      responseBuilder = protocolHandler.unpackAndExecuteRequest(request);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn(String.format("%s: %s", "Unable to parse the protobuf message",
                             new String(data, StandardCharsets.UTF_8)), e);
      responseBuilder = builderWithError(AppStatusPB.ErrorCode.ILLEGAL_STATE, responseBuilder);
    }
    return responseBuilder;
  }
}
