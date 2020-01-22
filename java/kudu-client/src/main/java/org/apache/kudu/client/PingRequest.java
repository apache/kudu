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
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

/**
 * Ping request only used for tests to test connections.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class PingRequest extends KuduRpc<PingResponse> {

  private final String serviceName;

  static PingRequest makeMasterPingRequest() {
    return new PingRequest(MASTER_SERVICE_NAME, null, 0);
  }

  static PingRequest makeTabletServerPingRequest() {
    return new PingRequest(TABLET_SERVER_SERVICE_NAME, null, 0);
  }

  private PingRequest(String serviceName, Timer timer, long timeoutMillis) {
    super(null, timer, timeoutMillis);
    this.serviceName = serviceName;
  }

  @Override
  Message createRequestPB() {
    return Master.PingRequestPB.getDefaultInstance();
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
  Pair<PingResponse, Object> deserialize(CallResponse callResponse, String tsUUID)
      throws KuduException {
    final Master.PingResponsePB.Builder respBuilder =
        Master.PingResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    PingResponse response = new PingResponse(timeoutTracker.getElapsedMillis(), tsUUID);
    return new Pair<>(response, null);
  }
}
