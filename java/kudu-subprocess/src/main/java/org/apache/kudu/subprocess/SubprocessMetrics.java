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

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates the metrics associated with the subprocess. It is expected that
 * this is passed around alongside each request/response as it makes its way
 * through the different stages of the SubprocessExecutor, and for callers to
 * call startTimer() and the various record methods as appropriate.
 */
public class SubprocessMetrics {
  private final Subprocess.SubprocessMetricsPB.Builder builder;
  private final Stopwatch stopwatch;
  private final BlockingQueue<InboundRequest> inboundQueue;

  /**
   * Construct a SubprocessMetrics object.
   *
   * @param inboundQueue used to determine the length of the inbound queue
   */
  public SubprocessMetrics(BlockingQueue<InboundRequest> inboundQueue) {
    this.inboundQueue = inboundQueue;
    builder = Subprocess.SubprocessMetricsPB.newBuilder();
    stopwatch = Stopwatch.createUnstarted();
  }

  public void startTimer() {
    stopwatch.start();
  }

  /**
   * Stops the stopwatch and records the amount of time elapsed into the
   * metrics builder, with the assumption that it was started upon placing an
   * element on the inbound queue.
   */
  public void recordInboundQueueTimeMs() {
    Preconditions.checkArgument(!builder.hasInboundQueueTimeMs());
    Preconditions.checkArgument(stopwatch.isRunning());
    builder.setInboundQueueTimeMs(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    // We'll continue to use the timer as it makes its way through the
    // execution lifecycle, so reset it here.
    stopwatch.reset();
  }

  /**
   * Stops the stopwatch and records the amount of time elapsed into the
   * metrics builder, with the assumption that it was started upon beginning
   * to execute.
   */
  public void recordExecutionTimeMs() {
    Preconditions.checkArgument(!builder.hasExecutionTimeMs());
    Preconditions.checkArgument(stopwatch.isRunning());
    builder.setExecutionTimeMs(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    // We'll continue to use the timer as it makes its way through the
    // execution lifecycle, so reset it here.
    stopwatch.reset();
  }

  /**
   * Stops the stopwatch and records the amount of time elapsed into the
   * metrics builder, with the assumption that it was started upon placing an
   * element on the outbound queue.
   */
  public void recordOutboundQueueTimeMs() {
    Preconditions.checkArgument(!builder.hasOutboundQueueTimeMs());
    Preconditions.checkArgument(stopwatch.isRunning());
    builder.setOutboundQueueTimeMs(stopwatch.elapsed(TimeUnit.MILLISECONDS));
    stopwatch.stop();
  }

  /**
   * Builds the metrics protobuf message with the recorded timings and the
   * current lengths of the message queues.
   *
   * @param outboundQueue used to determine the length of the outbound queue
   * @return the constructed SubprocessMetricsPB
   */
  public Subprocess.SubprocessMetricsPB buildMetricsPB(
      BlockingQueue<OutboundResponse> outboundQueue) {
    Preconditions.checkArgument(builder.hasInboundQueueTimeMs());
    Preconditions.checkArgument(builder.hasExecutionTimeMs());
    Preconditions.checkArgument(builder.hasOutboundQueueTimeMs());
    builder.setInboundQueueLength(inboundQueue.size());
    builder.setOutboundQueueLength(outboundQueue.size());
    return builder.build();
  }
}
