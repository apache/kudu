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

import static org.apache.kudu.transactions.TxnManager.BeginTransactionResponsePB;

import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import io.netty.util.Timer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.transactions.TxnManager;
import org.apache.kudu.util.Pair;

/**
 * A wrapper class for kudu.transactions.TxnManagerService.BeginTransaction RPC.
 */
@InterfaceAudience.Private
class BeginTransactionRequest extends KuduRpc<BeginTransactionResponse> {
  private static final List<Integer> featureFlags = ImmutableList.of();

  BeginTransactionRequest(KuduTable masterTable, Timer timer, long timeoutMillis) {
    super(masterTable, timer, timeoutMillis);
  }

  @Override
  Message createRequestPB() {
    return TxnManager.BeginTransactionRequestPB.getDefaultInstance();
  }

  @Override
  String serviceName() {
    return TXN_MANAGER_SERVICE_NAME;
  }

  @Override
  String method() {
    return "BeginTransaction";
  }

  @Override
  Pair<BeginTransactionResponse, Object> deserialize(
      final CallResponse callResponse, String serverUUID) throws KuduException {
    final BeginTransactionResponsePB.Builder b = BeginTransactionResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), b);
    if (!b.hasError()) {
      Preconditions.checkState(b.hasTxnId());
      Preconditions.checkState(b.hasKeepaliveMillis());
    }
    BeginTransactionResponse response = new BeginTransactionResponse(
        timeoutTracker.getElapsedMillis(),
        serverUUID,
        b.getTxnId(),
        b.getKeepaliveMillis());
    return new Pair<>(response, b.hasError() ? b.getError() : null);
  }

  @Override
  Collection<Integer> getRequiredFeatures() {
    return featureFlags;
  }
}
