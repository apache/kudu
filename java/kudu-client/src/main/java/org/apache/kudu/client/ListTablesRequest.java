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

import org.apache.kudu.client.ListTablesResponse.TableInfo;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

@InterfaceAudience.Private
class ListTablesRequest extends KuduRpc<ListTablesResponse> {

  private final String nameFilter;

  ListTablesRequest(KuduTable masterTable,
                    String nameFilter,
                    Timer timer,
                    long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
    this.nameFilter = nameFilter;
  }

  @Override
  Message createRequestPB() {
    final Master.ListTablesRequestPB.Builder builder =
        Master.ListTablesRequestPB.newBuilder();
    if (nameFilter != null) {
      builder.setNameFilter(nameFilter);
    }
    return builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "ListTables";
  }

  @Override
  Pair<ListTablesResponse, Object> deserialize(CallResponse callResponse,
                                               String tsUUID) throws KuduException {
    final Master.ListTablesResponsePB.Builder respBuilder =
        Master.ListTablesResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    int tablesCount = respBuilder.getTablesCount();
    List<TableInfo> tableInfos = new ArrayList<>(tablesCount);
    for (Master.ListTablesResponsePB.TableInfo infoPb : respBuilder.getTablesList()) {
      tableInfos.add(new TableInfo(infoPb.getId().toStringUtf8(), infoPb.getName()));
    }
    ListTablesResponse response = new ListTablesResponse(timeoutTracker.getElapsedMillis(),
                                                         tsUUID, tableInfos);
    return new Pair<ListTablesResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
