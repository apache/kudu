// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import static kudu.master.Master.*;
import org.jboss.netty.buffer.ChannelBuffer;

public class ListTabletServersRequest extends KuduRpc {

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
  String method() {
    return "ListTabletServers";
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    final ListTabletServersResponsePB.Builder respBuilder =
        ListTabletServersResponsePB.newBuilder();
    readProtobuf(buf, respBuilder);
    ListTabletServersResponsePB resp = respBuilder.build();
    return resp.getServersCount();
  }
}
