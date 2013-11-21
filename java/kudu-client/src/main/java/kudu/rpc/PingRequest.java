// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.tserver.Tserver;
import org.jboss.netty.buffer.ChannelBuffer;

public class PingRequest extends KuduRpc {

  public static final String PING = "Ping";

  PingRequest(KuduTable table) {
    super(table);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Tserver.PingRequestPB ping = Tserver.PingRequestPB.newBuilder().build();
    return toChannelBuffer(header, ping);
  }

  @Override
  String method() {
    return PING;
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    Tserver.PingResponsePB.Builder builder = Tserver.PingResponsePB.newBuilder();
    readProtobuf(buf, builder);
    return builder.build();
  }
}
