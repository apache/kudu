// Copyright (c) 2013, Cloudera, inc.
package kudu.rpc;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.tserver.Tserver;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * Used internally to batch Operations together before sending to the cluster
 */
class Batch extends KuduRpc<Tserver.WriteResponsePB> implements KuduRpc.HasKey {

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
    builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(this.externalConsistencyMode.pbVersion());
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String method() {
    return Operation.METHOD;
  }

  @Override
  Pair<Tserver.WriteResponsePB, Object> deserialize(ChannelBuffer buf) throws Exception {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(buf, builder);
    Tserver.WriteResponsePB resp = builder.build();
    return new Pair<Tserver.WriteResponsePB, Object>(resp, resp.getError());
  }

  @Override
  public byte[] key() {
    assert this.ops.size() > 0;
    return this.ops.get(0).key();
  }
}
