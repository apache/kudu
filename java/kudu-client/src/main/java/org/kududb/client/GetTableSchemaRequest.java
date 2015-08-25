// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import static org.kududb.master.Master.*;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to fetch a table's schema
 */
@InterfaceAudience.Private
public class GetTableSchemaRequest extends KuduRpc<GetTableSchemaResponse> {
  static final String GET_TABLE_SCHEMA = "GetTableSchema";
  private final String name;


  GetTableSchemaRequest(KuduTable masterTable, String name) {
    super(masterTable);
    this.name = name;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final GetTableSchemaRequestPB.Builder builder = GetTableSchemaRequestPB.newBuilder();
    TableIdentifierPB tableID =
        TableIdentifierPB.newBuilder().setTableName(name).build();
    builder.setTable(tableID);
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return GET_TABLE_SCHEMA;
  }

  @Override
  Pair<GetTableSchemaResponse, Object> deserialize(CallResponse callResponse,
                                                   String tsUUID) throws Exception {
    final GetTableSchemaResponsePB.Builder respBuilder = GetTableSchemaResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    Schema schema = ProtobufHelper.pbToSchema(respBuilder.getSchema());
    GetTableSchemaResponse response = new GetTableSchemaResponse(
        deadlineTracker.getElapsedMillis(),
        tsUUID,
        schema,
        ProtobufHelper.pbToPartitionSchema(respBuilder.getPartitionSchema(), schema));
    return new Pair<GetTableSchemaResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
