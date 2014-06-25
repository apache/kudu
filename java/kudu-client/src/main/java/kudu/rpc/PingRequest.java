// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import kudu.tserver.Tserver;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

public class PingRequest extends KuduRpc<Tserver.PingResponsePB> {

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
  Pair<Tserver.PingResponsePB, Object> deserialize(ChannelBuffer buf) throws Exception {
    Tserver.PingResponsePB.Builder builder = Tserver.PingResponsePB.newBuilder();
    readProtobuf(buf, builder);
    return new Pair<Tserver.PingResponsePB, Object>(builder.build(), null);
  }
}
