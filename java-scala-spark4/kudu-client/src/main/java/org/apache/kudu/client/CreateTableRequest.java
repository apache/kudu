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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.Schema;
import org.apache.kudu.client.ProtobufHelper.SchemaPBConversionFlags;
import org.apache.kudu.master.Master;
import org.apache.kudu.util.Pair;

/**
 * RPC to create new tables
 */
@InterfaceAudience.Private
class CreateTableRequest extends KuduRpc<CreateTableResponse> {

  static final String CREATE_TABLE = "CreateTable";

  private final Schema schema;
  private final String name;
  private final Master.CreateTableRequestPB.Builder builder;
  private final List<Integer> featureFlags;

  CreateTableRequest(KuduTable masterTable,
                     String name,
                     Schema schema,
                     CreateTableOptions cto,
                     Timer timer,
                     long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
    this.schema = schema;
    this.name = name;
    this.builder = cto.getBuilder();
    featureFlags = cto.getRequiredFeatureFlags(schema);
  }

  @Override
  Message createRequestPB() {
    this.builder.setName(this.name);
    this.builder.setSchema(
        ProtobufHelper.schemaToPb(this.schema,
                                  EnumSet.of(SchemaPBConversionFlags.SCHEMA_PB_WITHOUT_ID)));
    return this.builder.build();
  }

  @Override
  String serviceName() {
    return MASTER_SERVICE_NAME;
  }

  @Override
  String method() {
    return CREATE_TABLE;
  }

  @Override
  Pair<CreateTableResponse, Object> deserialize(final CallResponse callResponse,
                                                String tsUUID) throws KuduException {
    final Master.CreateTableResponsePB.Builder builder = Master.CreateTableResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    CreateTableResponse response =
        new CreateTableResponse(
            timeoutTracker.getElapsedMillis(),
            tsUUID,
            builder.getTableId().toStringUtf8());
    return new Pair<CreateTableResponse, Object>(
        response, builder.hasError() ? builder.getError() : null);
  }

  @Override
  Collection<Integer> getRequiredFeatures() {
    return featureFlags;
  }
}
