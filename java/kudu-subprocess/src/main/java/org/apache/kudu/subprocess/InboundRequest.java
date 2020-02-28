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

/**
 * Encapsulates a request on the inbound queue. It is expected that the
 * <code>SubprocessMetrics</code> have begun timing the time it takes to make
 * it through the queue before creating this and putting it onto the queue.
 */
public class InboundRequest {
  private final byte[] bytes;
  private SubprocessMetrics metrics;

  // TODO(awong): it might be nice if both the request and response spoke the
  //  same language (e.g. both byte arrays or both protobuf messages).
  public InboundRequest(byte[] bytes, SubprocessMetrics metrics) {
    this.bytes = bytes;
    this.metrics = metrics;
  }

  /**
   * @return the bytes associated with this request
   */
  public byte[] bytes() {
    return bytes;
  }

  /**
   * @return the metrics associated with this request
   */
  public SubprocessMetrics metrics() {
    return metrics;
  }
}
