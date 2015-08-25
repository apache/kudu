// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.master.Master;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Package-private RPC that can only go to a master.
 */
@InterfaceAudience.Private
class GetTableLocationsRequest extends KuduRpc<Master.GetTableLocationsResponsePB> {

  private final byte[] startPartitionKey;
  private final byte[] endKey;
  private final String tableName;

  GetTableLocationsRequest(KuduTable table, byte[] startPartitionKey,
                           byte[] endPartitionKey, String tableName) {
    super(table);
    if (startPartitionKey != null && endPartitionKey != null
        && Bytes.memcmp(startPartitionKey, endPartitionKey) > 0) {
      throw new IllegalArgumentException(
          "The start partition key must be smaller or equal to the end partition key");
    }
    this.startPartitionKey = startPartitionKey;
    this.endKey = endPartitionKey;
    this.tableName = tableName;
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() {
    return "GetTableLocations";
  }

  @Override
  Pair<Master.GetTableLocationsResponsePB, Object> deserialize(
      final CallResponse callResponse, String tsUUID)
      throws Exception {
    Master.GetTableLocationsResponsePB.Builder builder = Master.GetTableLocationsResponsePB
        .newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    Master.GetTableLocationsResponsePB resp = builder.build();
    return new Pair<Master.GetTableLocationsResponsePB, Object>(
        resp, builder.hasError() ? builder.getError() : null);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Master.GetTableLocationsRequestPB.Builder builder = Master
        .GetTableLocationsRequestPB.newBuilder();
    builder.setTable(Master.TableIdentifierPB.newBuilder().setTableName(tableName));
    if (startPartitionKey != null) {
      builder.setPartitionKeyStart(ZeroCopyLiteralByteString.wrap(startPartitionKey));
    }
    if (endKey != null) {
      builder.setPartitionKeyEnd(ZeroCopyLiteralByteString.wrap(endKey));
    }
    return toChannelBuffer(header, builder.build());
  }
}
