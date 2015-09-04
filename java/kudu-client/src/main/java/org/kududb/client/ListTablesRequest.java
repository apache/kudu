// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.master.Master;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
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
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return "ListTables";
  }

  @Override
  Pair<ListTablesResponse, Object> deserialize(CallResponse callResponse,
                                               String tsUUID) throws Exception {
    final Master.ListTablesResponsePB.Builder respBuilder =
        Master.ListTablesResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    int serversCount = respBuilder.getTablesCount();
    List<String> tables = new ArrayList<String>(serversCount);
    for (Master.ListTablesResponsePB.TableInfo info : respBuilder.getTablesList()) {
      tables.add(info.getName());
    }
    ListTablesResponse response = new ListTablesResponse(deadlineTracker.getElapsedMillis(),
                                                         tsUUID, tables);
    return new Pair<ListTablesResponse, Object>(
        response, respBuilder.hasError() ? respBuilder.getError() : null);
  }
}
