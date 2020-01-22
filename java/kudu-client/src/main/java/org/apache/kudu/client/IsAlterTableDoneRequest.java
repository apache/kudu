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

import static org.apache.kudu.master.Master.IsAlterTableDoneRequestPB;
import static org.apache.kudu.master.Master.IsAlterTableDoneResponsePB;
import static org.apache.kudu.master.Master.TableIdentifierPB;

import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.util.Pair;

/**
 * RPC used to check if an alter is running for the specified table
 */
@InterfaceAudience.Private
class IsAlterTableDoneRequest extends KuduRpc<IsAlterTableDoneResponse> {
  private final TableIdentifierPB.Builder tableId;

  IsAlterTableDoneRequest(KuduTable masterTable,
                          TableIdentifierPB.Builder tableId,
                          Timer timer,
                          long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
    this.tableId = tableId;
  }

  @Override
  Message createRequestPB() {
    final IsAlterTableDoneRequestPB.Builder builder =
        IsAlterTableDoneRequestPB.newBuilder();
    builder.setTable(tableId);
    return builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "IsAlterTableDone";
  }

  @Override
  Pair<IsAlterTableDoneResponse, Object> deserialize(final CallResponse callResponse,
                                                       String tsUUID) throws KuduException {
    final IsAlterTableDoneResponsePB.Builder respBuilder = IsAlterTableDoneResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    IsAlterTableDoneResponse resp = new IsAlterTableDoneResponse(timeoutTracker.getElapsedMillis(),
                                                                 tsUUID,
                                                                 respBuilder.getDone());
    return new Pair<IsAlterTableDoneResponse, Object>(
        resp, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
