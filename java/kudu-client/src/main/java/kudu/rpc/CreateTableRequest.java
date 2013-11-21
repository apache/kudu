// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.Schema;
import kudu.master.Master;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to create new tables
 */
class CreateTableRequest extends KuduRpc {

  static final String CREATE_TABLE = "CreateTable";

  private final Schema schema;
  private final String name;

  CreateTableRequest(KuduTable table, String name, Schema schema) {
    super(table);
    this.schema = schema;
    this.name = name;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final Master.CreateTableRequestPB.Builder builder = Master.CreateTableRequestPB.newBuilder();
    builder.setName(this.name);
    builder.setSchema(ProtobufHelper.schemaToPb(this.schema));
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String method() {
    return CREATE_TABLE;
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    final Master.CreateTableResponsePB.Builder builder = Master.CreateTableResponsePB.newBuilder();
    readProtobuf(buf, builder);
    Master.CreateTableResponsePB resp = builder.build();
    return resp.hasError() ? new MasterErrorException(resp.getError()) : resp;
  }
}
