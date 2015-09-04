// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.master.Master;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to create new tables
 */
@InterfaceAudience.Private
class CreateTableRequest extends KuduRpc<CreateTableResponse> {

  static final String CREATE_TABLE = "CreateTable";

  private final Schema schema;
  private final String name;
  private final Master.CreateTableRequestPB.Builder builder;

  CreateTableRequest(KuduTable masterTable, String name, Schema schema,
                     CreateTableBuilder builder) {
    super(masterTable);
    this.schema = schema;
    this.name = name;
    this.builder = builder.getBuilder();
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    this.builder.setName(this.name);
    this.builder.setSchema(ProtobufHelper.schemaToPb(this.schema));
    return toChannelBuffer(header, this.builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return CREATE_TABLE;
  }

  @Override
  Pair<CreateTableResponse, Object> deserialize(final CallResponse callResponse,
                                                String tsUUID) throws Exception {
    final Master.CreateTableResponsePB.Builder builder = Master.CreateTableResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);    CreateTableResponse response =
        new CreateTableResponse(deadlineTracker.getElapsedMillis(), tsUUID);
    return new Pair<CreateTableResponse, Object>(
        response, builder.hasError() ? builder.getError() : null);
  }
}
