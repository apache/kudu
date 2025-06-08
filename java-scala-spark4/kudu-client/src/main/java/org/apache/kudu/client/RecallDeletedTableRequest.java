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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

/**
 * RPC to recall tables
 */
@InterfaceAudience.Private
class RecallDeletedTableRequest extends KuduRpc<RecallDeletedTableResponse> {

  static final String RECALL_DELETED_TABLE = "RecallDeletedTable";

  private final String newTableName;

  private final String id;

  RecallDeletedTableRequest(KuduTable table,
                            String id,
                            String newTableName,
                            Timer timer,
                            long timeoutMillis) {
    super(table, timer, timeoutMillis);
    this.id = id;
    this.newTableName = newTableName;
  }

  @Override
  Message createRequestPB() {
    final Master.RecallDeletedTableRequestPB.Builder builder =
                          Master.RecallDeletedTableRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder()
        .setTableId(ByteString.copyFromUtf8(id)));
    if (!newTableName.isEmpty()) {
      builder.setNewTableName(newTableName);
    }
    return builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return RECALL_DELETED_TABLE;
  }

  @Override
  Pair<RecallDeletedTableResponse, Object> deserialize(CallResponse callResponse,
                                                       String tsUUID) throws KuduException {
    final Master.RecallDeletedTableResponsePB.Builder builder =
        Master.RecallDeletedTableResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    RecallDeletedTableResponse response =
        new RecallDeletedTableResponse(timeoutTracker.getElapsedMillis(), tsUUID);
    return new Pair<RecallDeletedTableResponse, Object>(
        response, builder.hasError() ? builder.getError() : null);
  }
}
