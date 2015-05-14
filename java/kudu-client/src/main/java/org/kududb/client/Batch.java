// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;
import kudu.tserver.Tserver;
import org.kududb.util.Pair;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Used internally to batch Operations together before sending to the cluster
 */
class Batch extends KuduRpc<OperationResponse> implements KuduRpc.HasKey {

  private static final OperationsComparatorBySequenceNumber SEQUENCE_NUMBER_COMPARATOR =
      new OperationsComparatorBySequenceNumber();

  final List<Operation> ops;

  // Operations can be added out of order to 'ops' if the tablet had to be looked up. We can detect
  // this situation in AsyncKuduSession and set this to true.
  boolean needsSorting = false;

  Batch(KuduTable table) {
    this(table, 1000);
  }

  Batch(KuduTable table, int estimatedBatchSize) {
    super(table);
    this.ops = new ArrayList<Operation>(estimatedBatchSize);
  }

  @Override
  ChannelBuffer serialize(Message header) {

    // This should only happen if at least one operation triggered a tablet lookup, which is rare
    // on a long-running client.
    if (needsSorting) {
      Collections.sort(ops, SEQUENCE_NUMBER_COMPARATOR);
    }

    final Tserver.WriteRequestPB.Builder builder =
        Operation.createAndFillWriteRequestPB(ops.toArray(new Operation[ops.size()]));
    builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(this.externalConsistencyMode.pbVersion());
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return TABLET_SERVER_SERVICE_NAME; }

  @Override
  String method() {
    return Operation.METHOD;
  }

  @Override
  Pair<OperationResponse, Object> deserialize(final CallResponse callResponse,
                                              String tsUUID) throws Exception {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);
    if (builder.getPerRowErrorsCount() != 0) {
      throw RowsWithErrorException.fromPerRowErrorPB(builder.getPerRowErrorsList(), ops, tsUUID);
    }
    OperationResponse response = new OperationResponse(deadlineTracker.getElapsedMillis(), tsUUID,
        builder.getTimestamp());
    return new Pair<OperationResponse, Object>(response, builder.getError());
  }

  @Override
  public byte[] key() {
    assert this.ops.size() > 0;
    return this.ops.get(0).key();
  }

  /**
   * Sorts the Operations by their sequence number.
   */
  private static class OperationsComparatorBySequenceNumber implements Comparator<Operation> {
    @Override
    public int compare(Operation o1, Operation o2) {
      return Long.compare(o1.getSequenceNumber(), o2.getSequenceNumber());
    }
  }
}
