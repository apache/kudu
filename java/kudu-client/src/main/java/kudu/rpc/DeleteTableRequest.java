// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.master.Master;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to delete tables
 */
class DeleteTableRequest extends KuduRpc<Master.DeleteTableResponsePB> {

  static final String DELETE_TABLE = "DeleteTable";

  private final String name;

  DeleteTableRequest(KuduTable table, String name) {
    super(table);
    this.name = name;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final Master.DeleteTableRequestPB.Builder builder = Master.DeleteTableRequestPB.newBuilder();
    Master.TableIdentifierPB tableID =
       Master.TableIdentifierPB.newBuilder().setTableName(name).build();
    builder.setTable(tableID);
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String method() {
    return DELETE_TABLE;
  }

  @Override
  Pair<Master.DeleteTableResponsePB, Object> deserialize(ChannelBuffer buf) throws Exception {
    final Master.DeleteTableResponsePB.Builder builder = Master.DeleteTableResponsePB.newBuilder();
    readProtobuf(buf, builder);
    Master.DeleteTableResponsePB resp = builder.build();
    return new Pair<Master.DeleteTableResponsePB, Object>(resp, resp.getError());
  }
}
