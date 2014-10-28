// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.master.Master;
import kudu.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to a master.
 */
class GetTableLocationsRequest extends KuduRpc<Master.GetTableLocationsResponsePB> {

  private final byte[] startKey;
  private final byte[] endKey;
  private final String tableName;

  GetTableLocationsRequest(KuduTable table, byte[] startKey, byte[] endKey, String tableName) {
    super(table);
    if (startKey != null && endKey != null && Bytes.memcmp(startKey, endKey) > 0) {
      throw new IllegalArgumentException("The start key needs to be smaller or equal to the end " +
          "key");
    }
    this.startKey = startKey;
    this.endKey = endKey;
    this.tableName = tableName;
  }

  @Override
  String method() {
    return "GetTableLocations";
  }

  @Override
  Pair<Master.GetTableLocationsResponsePB, Object> deserialize(final ChannelBuffer buf)
      throws
      Exception {
    Master.GetTableLocationsResponsePB.Builder builder = Master.GetTableLocationsResponsePB
        .newBuilder();
    readProtobuf(buf, builder);
    Master.GetTableLocationsResponsePB resp = builder.build();
    return new Pair<Master.GetTableLocationsResponsePB, Object>(resp, resp.getError());
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Master.GetTableLocationsRequestPB.Builder builder = Master
        .GetTableLocationsRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
    if (startKey != null) {
      builder.setStartKey(ZeroCopyLiteralByteString.wrap(startKey));
    }
    if (endKey != null) {
      builder.setEndKey(ZeroCopyLiteralByteString.wrap(endKey));
    }
    return toChannelBuffer(header, builder.build());
  }
}
