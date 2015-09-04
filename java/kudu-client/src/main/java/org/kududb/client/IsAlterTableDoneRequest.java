// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import static org.kududb.master.Master.*;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC used to check if an alter is running for the specified table
 */
@InterfaceAudience.Private
class IsAlterTableDoneRequest extends KuduRpc<IsAlterTableDoneResponse> {

  static final String IS_ALTER_TABLE_DONE = "IsAlterTableDone";
  private final String name;


  IsAlterTableDoneRequest(KuduTable masterTable, String name) {
    super(masterTable);
    this.name = name;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final IsAlterTableDoneRequestPB.Builder builder = IsAlterTableDoneRequestPB.newBuilder();
    TableIdentifierPB tableID =
        TableIdentifierPB.newBuilder().setTableName(name).build();
    builder.setTable(tableID);
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return IS_ALTER_TABLE_DONE;
  }

  @Override
  Pair<IsAlterTableDoneResponse, Object> deserialize(final CallResponse callResponse,
                                                       String tsUUID) throws Exception {
    final IsAlterTableDoneResponsePB.Builder respBuilder = IsAlterTableDoneResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    IsAlterTableDoneResponse resp = new IsAlterTableDoneResponse(deadlineTracker.getElapsedMillis(),
        tsUUID, respBuilder.getDone());
    return new Pair<IsAlterTableDoneResponse, Object>(
        resp, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
