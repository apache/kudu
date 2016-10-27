// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.kudu.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyLiteralByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.apache.kudu.WireProtocol;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.client.Statistics.Statistic;
import org.apache.kudu.client.Statistics.TabletStatistics;
import org.apache.kudu.tserver.Tserver;
import org.apache.kudu.tserver.Tserver.TabletServerErrorPB;
import org.apache.kudu.util.Pair;

/**
 * Used internally to group Operations for a single tablet together before sending to the tablet
 * server.
 */
@InterfaceAudience.Private
class Batch extends KuduRpc<BatchResponse> {

  /** Holds batched operations. */
  final List<Operation> operations = new ArrayList<>();

  /** The tablet this batch will be routed to. */
  private final LocatedTablet tablet;

  /**
   * This size will be set when serialize is called. It stands for the size of rows in all
   * operations in this batch.
   */
  private long rowOperationsSizeBytes = 0;

  /** See {@link SessionConfiguration#setIgnoreAllDuplicateRows(boolean)} */
  private final boolean ignoreAllDuplicateRows;


  Batch(KuduTable table, LocatedTablet tablet, boolean ignoreAllDuplicateRows) {
    super(table);
    this.ignoreAllDuplicateRows = ignoreAllDuplicateRows;
    this.tablet = tablet;
  }

  /**
   * Returns the bytes size of this batch's row operations after serialization.
   * @return size in bytes
   * @throws IllegalStateException thrown if this RPC hasn't been serialized eg sent to a TS
   */
  long getRowOperationsSizeBytes() {
    if (this.rowOperationsSizeBytes == 0) {
      throw new IllegalStateException("This row hasn't been serialized yet");
    }
    return this.rowOperationsSizeBytes;
  }

  public void add(Operation operation) {
    assert Bytes.memcmp(operation.partitionKey(),
                        tablet.getPartition().getPartitionKeyStart()) >= 0 &&
           (tablet.getPartition().getPartitionKeyEnd().length == 0 ||
            Bytes.memcmp(operation.partitionKey(),
                         tablet.getPartition().getPartitionKeyEnd()) < 0);

    operations.add(operation);
  }

  @Override
  ChannelBuffer serialize(Message header) {
    final Tserver.WriteRequestPB.Builder builder = Operation.createAndFillWriteRequestPB(operations);
    rowOperationsSizeBytes = builder.getRowOperations().getRows().size() +
                             builder.getRowOperations().getIndirectData().size();
    builder.setTabletId(ZeroCopyLiteralByteString.wrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(externalConsistencyMode.pbVersion());
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() {
    return TABLET_SERVER_SERVICE_NAME;
  }

  @Override
  String method() {
    return Operation.METHOD;
  }

  @Override
  Pair<BatchResponse, Object> deserialize(CallResponse callResponse,
                                          String tsUUID) throws KuduException {
    Tserver.WriteResponsePB.Builder builder = Tserver.WriteResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), builder);

    List<Tserver.WriteResponsePB.PerRowErrorPB> errorsPB = builder.getPerRowErrorsList();
    if (ignoreAllDuplicateRows) {
      boolean allAlreadyPresent = true;
      for (Tserver.WriteResponsePB.PerRowErrorPB errorPB : errorsPB) {
        if (errorPB.getError().getCode() != WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT) {
          allAlreadyPresent = false;
          break;
        }
      }
      if (allAlreadyPresent) {
        errorsPB = Collections.emptyList();
      }
    }

    BatchResponse response = new BatchResponse(deadlineTracker.getElapsedMillis(), tsUUID,
                                               builder.getTimestamp(), errorsPB, operations);

    if (injectedError != null) {
      if (injectedlatencyMs > 0) {
        try {
          Thread.sleep(injectedlatencyMs);
        } catch (InterruptedException e) {
        }
      }
      return new Pair<BatchResponse, Object>(response, injectedError);
    }

    return new Pair<BatchResponse, Object>(response, builder.hasError() ? builder.getError() : null);
  }

  @Override
  public byte[] partitionKey() {
    return tablet.getPartition().getPartitionKeyStart();
  }

  @Override
  boolean isRequestTracked() {
    return true;
  }

  @Override
  void updateStatistics(Statistics statistics, BatchResponse response) {
    String tabletId = this.getTablet().getTabletId();
    String tableName = this.getTable().getName();
    TabletStatistics tabletStatistics = statistics.getTabletStatistics(tableName, tabletId);
    if (response == null) {
      tabletStatistics.incrementStatistic(Statistic.OPS_ERRORS, operations.size());
      tabletStatistics.incrementStatistic(Statistic.RPC_ERRORS, 1);
      return;
    }
    tabletStatistics.incrementStatistic(Statistic.WRITE_RPCS, 1);
    for (OperationResponse opResponse : response.getIndividualResponses()) {
      if (opResponse.hasRowError()) {
        tabletStatistics.incrementStatistic(Statistic.OPS_ERRORS, 1);
      } else {
        tabletStatistics.incrementStatistic(Statistic.WRITE_OPS, 1);
      }
    }
    tabletStatistics.incrementStatistic(Statistic.BYTES_WRITTEN, getRowOperationsSizeBytes());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("operations", operations.size())
                      .add("tablet", tablet)
                      .add("ignoreAllDuplicateRows", ignoreAllDuplicateRows)
                      .toString();
  }

  private static TabletServerErrorPB injectedError;
  private static int injectedlatencyMs;

  /**
   * Inject tablet server side error for Batch rpc related tests.
   * @param error error response from tablet server
   * @param latencyMs blocks response handling thread for some time to simulate
   * write latency
   */
  @VisibleForTesting
  static void injectTabletServerErrorAndLatency(TabletServerErrorPB error, int latencyMs) {
    injectedError = error;
    injectedlatencyMs = latencyMs;
  }
}
