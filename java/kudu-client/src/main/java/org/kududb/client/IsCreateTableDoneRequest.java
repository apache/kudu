// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.master.Master;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to a master.
 */
@InterfaceAudience.Private
class IsCreateTableDoneRequest extends KuduRpc<Master.IsCreateTableDoneResponsePB> {

  private final String tableName;

  IsCreateTableDoneRequest(KuduTable table, String tableName) {
    super(table);
    this.tableName = tableName;
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return "IsCreateTableDone";
  }

  @Override
  Pair<Master.IsCreateTableDoneResponsePB, Object> deserialize(
      final CallResponse callResponse, String tsUUID) throws Exception {
    Master.IsCreateTableDoneResponsePB.Builder builder = Master.IsCreateTableDoneResponsePB
        .newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    Master.IsCreateTableDoneResponsePB resp = builder.build();
    return new Pair<Master.IsCreateTableDoneResponsePB, Object>(
        resp, builder.hasError() ? builder.getError() : null);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Master.IsCreateTableDoneRequestPB.Builder builder = Master
        .IsCreateTableDoneRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
    return toChannelBuffer(header, builder.build());
  }
}
