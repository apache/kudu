// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.master.Master;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

class ListTablesRequest extends KuduRpc<ListTablesResponse> {

  private final String nameFilter;

  ListTablesRequest(KuduTable masterTable, String nameFilter) {
    super(masterTable);
    this.nameFilter = nameFilter;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final Master.ListTablesRequestPB.Builder builder =
        Master.ListTablesRequestPB.newBuilder();
    if (nameFilter != null) {
      builder.setNameFilter(nameFilter);
    }
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String method() {
    return "ListTables";
  }

  @Override
  Pair<ListTablesResponse, Object> deserialize(ChannelBuffer buf, String tsUUID) throws Exception {
    final Master.ListTablesResponsePB.Builder respBuilder =
        Master.ListTablesResponsePB.newBuilder();
    readProtobuf(buf, respBuilder);
    int serversCount = respBuilder.getTablesCount();
    List<String> tables = new ArrayList<String>(serversCount);
    for (Master.ListTablesResponsePB.TableInfo info : respBuilder.getTablesList()) {
      tables.add(info.getName());
    }
    ListTablesResponse response = new ListTablesResponse(deadlineTracker.getElapsedMillis(),
                                                         tsUUID, tables);
    return new Pair<ListTablesResponse, Object>(response, respBuilder.getError());
  }
}
