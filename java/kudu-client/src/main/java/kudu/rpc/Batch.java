// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.tserver.Tserver;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Used internally to batch Operations together before sending to the cluster
 */
class Batch extends KuduRpc {

  final List<Operation> ops;

  Batch(KuduTable table) {
    this(table, 1000);
  }

  Batch(KuduTable table, int estimatedBatchSize) {
    super(table);
    this.ops = new ArrayList<Operation>(estimatedBatchSize);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Tserver.WriteRequestPB.Builder builder =
        Operation.createAndFillWriteRequestPB(ops.toArray(new Operation[ops.size()]));
    builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getBytes()));
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String method() {
    return Operation.METHOD;
  }

  @Override
  Object deserialize(ChannelBuffer buf) {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(buf, builder);
    return builder.build();
  }
}
