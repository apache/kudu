// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;

import kudu.Schema;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to fetch a table's schema
 */
public class GetTableSchemaRequest extends KuduRpc<Schema> {
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
  Pair<Schema, Object> deserialize(ChannelBuffer buf) throws Exception {
    final GetTableSchemaResponsePB.Builder respBuilder = GetTableSchemaResponsePB.newBuilder();
    readProtobuf(buf, respBuilder);
    GetTableSchemaResponsePB resp = respBuilder.build();
    return new Pair<Schema, Object>(ProtobufHelper.pbToSchema(resp.getSchema()), resp.getError());
  }
}
