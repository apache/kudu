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

import java.util.concurrent.BlockingQueue;

/**
 * Encapsulates a response on the outbound queue. It is expected that the
 * <code>SubprocessMetrics</code> have begun timing the time it takes to
 * make it through the queue.
 */
public class OutboundResponse {
  private final Subprocess.SubprocessResponsePB.Builder respBuilder;
  private final SubprocessMetrics metrics;

  public OutboundResponse(Subprocess.SubprocessResponsePB.Builder respBuilder,
                          SubprocessMetrics metrics) {
    this.respBuilder = respBuilder;
    this.metrics = metrics;
  }

  /**
   * Builds the final <code>SubprocessResponsePB</code> to send over the pipe.
   * This constructs the <code>SubprocessMetricsPB</code> as well, and expects
   * that all queue timings have already been recorded.
   * @param outboundQueue
   * @return the response
   */
  public Subprocess.SubprocessResponsePB buildRespPB(
      BlockingQueue<OutboundResponse> outboundQueue) {
    respBuilder.setMetrics(metrics.buildMetricsPB(outboundQueue));
    return respBuilder.build();
  }

  /**
   * @return the metrics associated with this response
   */
  public SubprocessMetrics metrics() {
    return metrics;
  }
}
