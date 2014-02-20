// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import org.jboss.netty.buffer.ChannelBuffer;
import static kudu.master.Master.*;

/**
 * RPC used to alter a table. When it returns it doesn't mean that the table is altered,
 * a success just means that the master accepted it.
 */
class AlterTableRequest extends KuduRpc {

  static final String ALTER_TABLE = "AlterTable";
  private final String name;
  private final AlterTableRequestPB.Builder builder;

  AlterTableRequest(KuduTable masterTable, String name, AlterTableBuilder atb) {
    super(masterTable);
    this.name = name;
    this.builder = atb.pb;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    TableIdentifierPB tableID =
        TableIdentifierPB.newBuilder().setTableName(name).build();
    this.builder.setTable(tableID);
    return toChannelBuffer(header, this.builder.build());
  }

  @Override
  String method() {
    return ALTER_TABLE;
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    final AlterTableResponsePB.Builder respBuilder = AlterTableResponsePB.newBuilder();
    readProtobuf(buf, respBuilder);
    AlterTableResponsePB resp = respBuilder.build();
    return resp.hasError() ? new MasterErrorException(resp.getError()) : resp;
  }
}
