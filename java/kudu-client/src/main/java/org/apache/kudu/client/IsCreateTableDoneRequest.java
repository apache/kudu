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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.kudu.master.Master.IsCreateTableDoneRequestPB;
import org.apache.kudu.master.Master.IsCreateTableDoneResponsePB;
import org.apache.kudu.master.Master.TableIdentifierPB;
import org.apache.kudu.util.Pair;

/**
 * Package-private RPC that can only go to a master.
 */
@InterfaceAudience.Private
class IsCreateTableDoneRequest extends KuduRpc<IsCreateTableDoneResponse> {
  private final TableIdentifierPB.Builder table;

  IsCreateTableDoneRequest(KuduTable masterTable, TableIdentifierPB.Builder table) {
    super(masterTable);
    this.table = table;
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "IsCreateTableDone";
  }

  @Override
  Pair<IsCreateTableDoneResponse, Object> deserialize(
      final CallResponse callResponse, String tsUUID) throws KuduException {
    IsCreateTableDoneResponsePB.Builder builder =
        IsCreateTableDoneResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    IsCreateTableDoneResponse resp =
        new IsCreateTableDoneResponse(deadlineTracker.getElapsedMillis(),
        tsUUID, builder.getDone());
    return new Pair<IsCreateTableDoneResponse, Object>(
        resp, builder.hasError() ? builder.getError() : null);
  }

  @Override
  Message createRequestPB() {
    final IsCreateTableDoneRequestPB.Builder builder =
        IsCreateTableDoneRequestPB.newBuilder();
    builder.setTable(table);
    return builder.build();
  }
}
