// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.master.Master;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC to delete tables
 */
@InterfaceAudience.Private
class DeleteTableRequest extends KuduRpc<DeleteTableResponse> {

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
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return DELETE_TABLE;
  }

  @Override
  Pair<DeleteTableResponse, Object> deserialize(CallResponse callResponse,
                                                String tsUUID) throws Exception {
    final Master.DeleteTableResponsePB.Builder builder = Master.DeleteTableResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    DeleteTableResponse response =
        new DeleteTableResponse(deadlineTracker.getElapsedMillis(), tsUUID);
    return new Pair<DeleteTableResponse, Object>(
        response, builder.hasError() ? builder.getError() : null);
  }
}
