// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;

import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

public class ListTabletServersRequest extends KuduRpc<ListTabletServersResponse> {

  public ListTabletServersRequest(KuduTable masterTable) {
    super(masterTable);
  }
  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final ListTabletServersRequestPB.Builder builder =
        ListTabletServersRequestPB.newBuilder();
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return "ListTabletServers";
  }

  @Override
  Pair<ListTabletServersResponse, Object> deserialize(CallResponse callResponse,
                                                      String tsUUID) throws Exception {
    final ListTabletServersResponsePB.Builder respBuilder =
        ListTabletServersResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    int serversCount = respBuilder.getServersCount();
    List<String> servers = new ArrayList<String>(serversCount);
    for (ListTabletServersResponsePB.Entry entry : respBuilder.getServersList()) {
      servers.add(entry.getRegistration().getRpcAddresses(0).getHost());
    }
    ListTabletServersResponse response = new ListTabletServersResponse(deadlineTracker
        .getElapsedMillis(), tsUUID, serversCount, servers);
    return new Pair<ListTabletServersResponse, Object>(response, null);
  }
}
