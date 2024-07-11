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

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MessageReader} class,
 *   1. processes a message that reads from the underlying input stream.
 *   2. and then puts it to the inbound message queue.
 *
 * Since {@link MessageIO#readBytes()} is not atomic, the implementation
 * of MessageReader is not thread-safe, and thus MessageReader should not
 * be called concurrently unless handled by the caller.
 */
@InterfaceAudience.Private
class MessageReader implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);
  private final BlockingQueue<InboundRequest> inboundQueue;
  private final MessageIO messageIO;
  private final boolean injectInterrupt;

  MessageReader(BlockingQueue<InboundRequest> inboundQueue,
                MessageIO messageIO,
                boolean injectInterrupt) {
    Preconditions.checkNotNull(inboundQueue);
    this.inboundQueue = inboundQueue;
    this.messageIO = messageIO;
    this.injectInterrupt = injectInterrupt;
  }

  @Override
  public void run() {
    // Inject InterruptedException if needed (for tests only).
    if (injectInterrupt) {
      Thread.currentThread().interrupt();
    }
    while (true) {
      // Read the message from the standard input. If fail to read the
      // message properly, IOException is thrown. IOException is fatal,
      // and should be propagated up the call stack. Retry on IOException
      // is not necessary as the error can happen in the middle of message
      // reading.
      byte[] data;
      try {
        data = messageIO.readBytes();
      } catch (KuduSubprocessException e) {
        LOG.error("{}: continuing", e.getMessage());
        continue;
      } catch (IOException e) {
        throw new KuduSubprocessException("Unable to read the protobuf message", e);
      }

      // Log a warning for empty message which is not expected.
      if (data.length == 0) {
        LOG.warn("Empty message received.");
        continue;
      }
      SubprocessMetrics metrics = new SubprocessMetrics(inboundQueue);
      // Begin recording the time it takes to make it through the inbound
      // queue. A parser thread will record the elapsed time right after
      // it pulls the request from the queue.
      metrics.startTimer();
      QueueUtil.put(inboundQueue, new InboundRequest(data, metrics));
    }
  }
}
