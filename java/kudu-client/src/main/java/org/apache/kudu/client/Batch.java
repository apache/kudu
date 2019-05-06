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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.kudu.WireProtocol.AppStatusPB.ErrorCode;
import org.apache.kudu.security.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.jboss.netty.util.Timer;

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
  /** Holds indexes of operations in the original user's batch. */
  final List<Integer> operationIndexes = new ArrayList<>();

  /** The tablet this batch will be routed to. */
  private final LocatedTablet tablet;

  /** The token with which to authorize this RPC. */
  private Token.SignedTokenPB authzToken;

  /**
   * This size will be set when serialize is called. It stands for the size of rows in all
   * operations in this batch.
   */
  private long rowOperationsSizeBytes = 0;

  private final EnumSet<ErrorCode> ignoredErrors;

  Batch(KuduTable table, LocatedTablet tablet, boolean ignoreAllDuplicateRows,
        boolean ignoreAllNotFoundRows) {
    super(table, null, 0);
    // Build a set of ignored errors.
    Set<ErrorCode> ignoredErrors = new HashSet<>();
    if (ignoreAllDuplicateRows) {
      ignoredErrors.add(ErrorCode.ALREADY_PRESENT);
    }
    if (ignoreAllNotFoundRows) {
      ignoredErrors.add(ErrorCode.NOT_FOUND);
    }
    // EnumSet.copyOf doesn't handle an empty set, so handle that case specially.
    if (ignoredErrors.isEmpty()) {
      this.ignoredErrors = EnumSet.noneOf(ErrorCode.class);
    } else {
      this.ignoredErrors = EnumSet.copyOf(ignoredErrors);
    }
    this.tablet = tablet;
  }

  /**
   * Reset the timeout of this batch.
   *
   * TODO(wdberkeley): The fact we have to do this is a sign an Operation should not subclass
   * KuduRpc.
   *
   * @param timeoutMillis the new timeout of the batch in milliseconds
   */
  void resetTimeoutMillis(Timer timer, long timeoutMillis) {
    timeoutTracker.reset();
    timeoutTracker.setTimeout(timeoutMillis);
    if (timeoutTask != null) {
      timeoutTask.cancel();
    }
    timeoutTask = AsyncKuduClient.newTimeout(timer, new RpcTimeoutTask(), timeoutMillis);
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

  public void add(Operation operation, int index) {
    assert Bytes.memcmp(operation.partitionKey(),
                        tablet.getPartition().getPartitionKeyStart()) >= 0 &&
           (tablet.getPartition().getPartitionKeyEnd().length == 0 ||
            Bytes.memcmp(operation.partitionKey(),
                         tablet.getPartition().getPartitionKeyEnd()) < 0);

    operations.add(operation);
    operationIndexes.add(index);
  }

  @Override
  boolean needsAuthzToken() {
    return true;
  }

  @Override
  void bindAuthzToken(Token.SignedTokenPB token) {
    authzToken = token;
  }

  @Override
  Message createRequestPB() {
    final Tserver.WriteRequestPB.Builder builder =
        Operation.createAndFillWriteRequestPB(operations);
    rowOperationsSizeBytes = (long)builder.getRowOperations().getRows().size() +
                             (long)builder.getRowOperations().getIndirectData().size();
    builder.setTabletId(UnsafeByteOperations.unsafeWrap(getTablet().getTabletIdAsBytes()));
    builder.setExternalConsistencyMode(externalConsistencyMode.pbVersion());
    if (authzToken != null) {
      builder.setAuthzToken(authzToken);
    }
    return builder.build();
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
    // Create a new list of errors that doesn't contain ignored error codes.
    if (!ignoredErrors.isEmpty()) {
      List<Tserver.WriteResponsePB.PerRowErrorPB> filteredErrors = new ArrayList<>();
      for (Tserver.WriteResponsePB.PerRowErrorPB errorPB : errorsPB) {
        if (!ignoredErrors.contains(errorPB.getError().getCode())) {
          filteredErrors.add(errorPB);
        }
      }
      errorsPB = filteredErrors;
    }
    BatchResponse response = new BatchResponse(timeoutTracker.getElapsedMillis(),
                                               tsUUID,
                                               builder.getTimestamp(),
                                               errorsPB,
                                               operations,
                                               operationIndexes);

    if (injectedError != null) {
      if (injectedlatencyMs > 0) {
        try {
          Thread.sleep(injectedlatencyMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return new Pair<BatchResponse, Object>(response, injectedError);
    }

    return new Pair<BatchResponse, Object>(response,
        builder.hasError() ? builder.getError() : null);
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
                      .add("ignoredErrors", Iterables.toString(ignoredErrors))
                      .add("rpc", super.toString())
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
  @InterfaceAudience.LimitedPrivate("Test")
  static void injectTabletServerErrorAndLatency(TabletServerErrorPB error, int latencyMs) {
    injectedError = error;
    injectedlatencyMs = latencyMs;
  }
}
