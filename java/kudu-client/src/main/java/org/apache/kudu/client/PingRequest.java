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
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Ping request only used for tests to test connections.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class PingRequest extends KuduRpc<PingResponse> {

  private final String serviceName;

  static PingRequest makeMasterPingRequest() {
    return new PingRequest(MASTER_SERVICE_NAME);
  }

  static PingRequest makeTabletServerPingRequest() {
    return new PingRequest(TABLET_SERVER_SERVICE_NAME);
  }

  private PingRequest(String serviceName) {
    super(null);
    this.serviceName = serviceName;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final Master.PingRequestPB.Builder builder =
        Master.PingRequestPB.newBuilder();
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() {
    return serviceName;
  }

  @Override
  String method() {
    return "Ping";
  }

  @Override
  Pair<PingResponse, Object> deserialize(CallResponse callResponse, String tsUUID) throws KuduException {
    final Master.PingResponsePB.Builder respBuilder =
        Master.PingResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    PingResponse response = new PingResponse(deadlineTracker.getElapsedMillis(),
        tsUUID);
    return new Pair<>(response, null);
  }
}
