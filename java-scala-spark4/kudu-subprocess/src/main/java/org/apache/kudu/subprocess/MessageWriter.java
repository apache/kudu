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

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The {@link MessageWriter} class,
 *    1. retrieves one message from the outbound queue at a time,
 *    2. and then writes the response to the underlying output stream.
 */
@InterfaceAudience.Private
class MessageWriter implements Runnable {
  private final BlockingQueue<OutboundResponse> outboundQueue;
  private final MessageIO messageIO;
  private final long blockWriteMs;

  MessageWriter(BlockingQueue<OutboundResponse> outboundQueue,
                MessageIO messageIO,
                long blockWriteMs) {
    Preconditions.checkNotNull(outboundQueue);
    Preconditions.checkNotNull(messageIO);
    this.outboundQueue = outboundQueue;
    this.messageIO = messageIO;
    this.blockWriteMs = blockWriteMs;
  }

  @Override
  public void run() {
    while (true) {
      OutboundResponse resp = QueueUtil.take(outboundQueue);
      resp.metrics().recordOutboundQueueTimeMs();

      // Write the response to the underlying output stream. IOException is fatal,
      // and should be propagated up the call stack.
      try {
        // Block the write for the given milliseconds if needed (for tests only).
        // -1 means the write will not be blocked.
        if (blockWriteMs != -1) {
          Thread.sleep(blockWriteMs);
        }
        messageIO.writeMessage(resp.buildRespPB(outboundQueue));
      } catch (IOException | InterruptedException e) {
        throw new KuduSubprocessException("Unable to write the protobuf message", e);
      }
    }
  }
}
