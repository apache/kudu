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

import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.util.Pair;

@InterfaceAudience.Private
class ListTabletsRequest extends KuduRpc<ListTabletsResponse> {

  ListTabletsRequest(Timer timer, long timeoutMillis) {
    super(null, timer, timeoutMillis);
  }

  @Override
  Message createRequestPB() {
    return Tserver.ListTabletsRequestPB.getDefaultInstance();
  }

  @Override
  String serviceName() {
    return TABLET_SERVER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "ListTablets";
  }

  @Override
  Pair<ListTabletsResponse, Object> deserialize(CallResponse callResponse,
                                               String tsUUID) throws KuduException {
    final Tserver.ListTabletsResponsePB.Builder respBuilder =
        Tserver.ListTabletsResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    int serversCount = respBuilder.getStatusAndSchemaCount();
    List<String> tablets = new ArrayList<>(serversCount);
    for (Tserver.ListTabletsResponsePB.StatusAndSchemaPB info
        : respBuilder.getStatusAndSchemaList()) {
      tablets.add(info.getTabletStatus().getTabletId());
    }
    ListTabletsResponse response = new ListTabletsResponse(timeoutTracker.getElapsedMillis(),
                                                           tsUUID,
                                                           tablets);
    return new Pair<>(response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
