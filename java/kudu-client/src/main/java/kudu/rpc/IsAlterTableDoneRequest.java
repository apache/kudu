// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;

import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * RPC used to check if an alter is running for the specified table
 */
class IsAlterTableDoneRequest extends KuduRpc<IsAlterTableDoneResponsePB> {

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
  String method() {
    return IS_ALTER_TABLE_DONE;
  }

  @Override
  Pair<IsAlterTableDoneResponsePB, Object> deserialize(final CallResponse callResponse,
                                                       String tsUUID) throws Exception {
    final IsAlterTableDoneResponsePB.Builder respBuilder = IsAlterTableDoneResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    IsAlterTableDoneResponsePB resp = respBuilder.build();
    return new Pair<IsAlterTableDoneResponsePB, Object>(resp, resp.getError());
  }
}
