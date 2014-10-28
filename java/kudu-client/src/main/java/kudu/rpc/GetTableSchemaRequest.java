// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;

import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to fetch a table's schema
 */
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
  String method() {
    return GET_TABLE_SCHEMA;
  }

  @Override
  Pair<GetTableSchemaResponse, Object> deserialize(ChannelBuffer buf) throws Exception {
    final GetTableSchemaResponsePB.Builder respBuilder = GetTableSchemaResponsePB.newBuilder();
    readProtobuf(buf, respBuilder);
    GetTableSchemaResponse response = new GetTableSchemaResponse(
        deadlineTracker.getElapsedMillis(), ProtobufHelper.pbToSchema(respBuilder.getSchema()));
    return new Pair<GetTableSchemaResponse, Object>(response, respBuilder.getError());
  }
}
