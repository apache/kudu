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

import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

@InterfaceAudience.Private
class GetTableStatisticsRequest extends KuduRpc<GetTableStatisticsResponse> {

  static final String GET_TABLE_STATISTICS = "GetTableStatistics";

  private final String name;

  GetTableStatisticsRequest(KuduTable table,
                            String name,
                            Timer timer,
                            long timeoutMillis) {
    super(table, timer, timeoutMillis);
    this.name = name;
  }

  @Override
  Message createRequestPB() {
    final Master.GetTableStatisticsRequestPB.Builder builder =
        Master.GetTableStatisticsRequestPB.newBuilder();
    Master.TableIdentifierPB tableID =
        Master.TableIdentifierPB.newBuilder().setTableName(name).build();
    builder.setTable(tableID);
    return builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return GET_TABLE_STATISTICS;
  }

  @Override
  Pair<GetTableStatisticsResponse, Object> deserialize(CallResponse callResponse,
                                                       String tsUUID) throws KuduException {
    final Master.GetTableStatisticsResponsePB.Builder respBuilder =
        Master.GetTableStatisticsResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    GetTableStatisticsResponse response = new GetTableStatisticsResponse(
        timeoutTracker.getElapsedMillis(),
        tsUUID,
        respBuilder.getOnDiskSize(),
        respBuilder.getLiveRowCount());
    return new Pair<GetTableStatisticsResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
