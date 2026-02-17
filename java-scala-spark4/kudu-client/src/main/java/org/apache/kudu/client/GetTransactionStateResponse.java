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

import java.util.OptionalLong;

import com.google.common.base.Preconditions;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.transactions.Transactions;

@InterfaceAudience.Private
public class GetTransactionStateResponse extends KuduRpcResponse {
  private final Transactions.TxnStatePB txnState;
  private final OptionalLong txnCommitTimestamp;

  /**
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param serverUUID UUID of the server that sent the response
   * @param txnState the state of the transaction
   */
  GetTransactionStateResponse(
      long elapsedMillis,
      String serverUUID,
      Transactions.TxnStatePB txnState,
      OptionalLong txnCommitTimestamp) {
    super(elapsedMillis, serverUUID);
    this.txnState = txnState;
    this.txnCommitTimestamp = txnCommitTimestamp;
  }

  public Transactions.TxnStatePB txnState() {
    return txnState;
  }

  boolean hasCommitTimestamp() {
    return txnCommitTimestamp.isPresent();
  }

  long getCommitTimestamp() {
    Preconditions.checkState(hasCommitTimestamp());
    return txnCommitTimestamp.getAsLong();
  }

  public boolean isCommitted() {
    return txnState == Transactions.TxnStatePB.COMMITTED;
  }

  public boolean isAborted() {
    return txnState == Transactions.TxnStatePB.ABORTED;
  }
}
