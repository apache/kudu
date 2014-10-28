// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.master.Master;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to a master.
 */
class IsCreateTableDoneRequest extends KuduRpc<Master.IsCreateTableDoneResponsePB> {

  private final String tableName;

  IsCreateTableDoneRequest(KuduTable table, String tableName) {
    super(table);
    this.tableName = tableName;
  }

  @Override
  String method() {
    return "IsCreateTableDone";
  }

  @Override
  kudu.util.Pair<Master.IsCreateTableDoneResponsePB, Object> deserialize(final ChannelBuffer buf) throws Exception {
    Master.IsCreateTableDoneResponsePB.Builder builder = Master.IsCreateTableDoneResponsePB
        .newBuilder();
    readProtobuf(buf, builder);
    Master.IsCreateTableDoneResponsePB resp = builder.build();
    return new Pair<Master.IsCreateTableDoneResponsePB, Object>(resp, resp.getError());
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Master.IsCreateTableDoneRequestPB.Builder builder = Master
        .IsCreateTableDoneRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
    return toChannelBuffer(header, builder.build());
  }
}
