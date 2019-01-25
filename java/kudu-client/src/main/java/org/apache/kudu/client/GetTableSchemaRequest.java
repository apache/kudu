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

import static org.apache.kudu.master.Master.GetTableSchemaRequestPB;
import static org.apache.kudu.master.Master.GetTableSchemaResponsePB;
import static org.apache.kudu.master.Master.TableIdentifierPB;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.apache.yetus.audience.InterfaceAudience;
import org.jboss.netty.util.Timer;

import org.apache.kudu.Schema;
import org.apache.kudu.master.Master.TableIdentifierPB.Builder;
import org.apache.kudu.util.Pair;

/**
 * RPC to fetch a table's schema
 */
@InterfaceAudience.Private
public class GetTableSchemaRequest extends KuduRpc<GetTableSchemaResponse> {
  private final String id;
  private final String name;


  GetTableSchemaRequest(KuduTable masterTable,
                        String id,
                        String name,
                        Timer timer,
                        long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
    Preconditions.checkArgument(id != null ^ name != null,
        "Only one of table ID or the table name should be provided");
    this.id = id;
    this.name = name;
  }

  @Override
  Message createRequestPB() {
    final GetTableSchemaRequestPB.Builder builder =
        GetTableSchemaRequestPB.newBuilder();
    Builder identifierBuilder = TableIdentifierPB.newBuilder();
    if (id != null) {
      identifierBuilder.setTableId(ByteString.copyFromUtf8(id));
    } else {
      Preconditions.checkNotNull(name);
      identifierBuilder.setTableName(name);
    }
    builder.setTable(identifierBuilder.build());
    return builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "GetTableSchema";
  }

  @Override
  Pair<GetTableSchemaResponse, Object> deserialize(CallResponse callResponse,
                                                   String tsUUID) throws KuduException {
    final GetTableSchemaResponsePB.Builder respBuilder = GetTableSchemaResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    Schema schema = ProtobufHelper.pbToSchema(respBuilder.getSchema());
    GetTableSchemaResponse response = new GetTableSchemaResponse(
        deadlineTracker.getElapsedMillis(),
        tsUUID,
        schema,
        respBuilder.getTableId().toStringUtf8(),
        respBuilder.getNumReplicas(),
        ProtobufHelper.pbToPartitionSchema(respBuilder.getPartitionSchema(), schema));
    return new Pair<GetTableSchemaResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
