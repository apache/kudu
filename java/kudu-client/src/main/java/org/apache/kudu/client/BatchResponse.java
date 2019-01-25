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
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.tserver.Tserver;

/**
 * Response type for Batch (which is used internally by AsyncKuduSession).
 * Provides the Hybrid Time write timestamp returned by the Tablet Server.
 */
@InterfaceAudience.Private
public class BatchResponse extends KuduRpcResponse {

  private final long writeTimestamp;
  private final List<RowError> rowErrors;
  private final List<OperationResponse> individualResponses;
  private final List<Integer> responsesIndexes;

  /**
   * Package-private constructor to be used by the RPCs.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param writeTimestamp HT's write timestamp
   * @param errorsPB a list of row errors, can be empty
   * @param operations the list of operations which created this response
   * @param indexes the list of operations' order index
   */
  BatchResponse(long elapsedMillis,
                String tsUUID,
                long writeTimestamp,
                List<Tserver.WriteResponsePB.PerRowErrorPB> errorsPB,
                List<Operation> operations,
                List<Integer> indexes) {
    super(elapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
    individualResponses = new ArrayList<>(operations.size());
    this.responsesIndexes = indexes;
    if (errorsPB.isEmpty()) {
      rowErrors = Collections.emptyList();
    } else {
      rowErrors = new ArrayList<>(errorsPB.size());
    }

    // Populate the list of individual row responses and the list of row errors. Not all the rows
    // maybe have errors, but 'errorsPB' contains them in the same order as the operations that
    // were sent.
    int currentErrorIndex = 0;
    Operation currentOperation;
    for (int i = 0; i < operations.size(); i++) {
      RowError rowError = null;
      currentOperation = operations.get(i);
      if (currentErrorIndex < errorsPB.size() &&
          errorsPB.get(currentErrorIndex).getRowIndex() == i) {
        rowError = RowError.fromRowErrorPb(errorsPB.get(currentErrorIndex),
            currentOperation, tsUUID);
        rowErrors.add(rowError);
        currentErrorIndex++;
      }
      individualResponses.add(
          new OperationResponse(currentOperation.deadlineTracker.getElapsedMillis(),
                                tsUUID,
                                writeTimestamp,
                                currentOperation,
                                rowError));
    }
    assert (rowErrors.size() == errorsPB.size());
    assert (individualResponses.size() == operations.size());
    assert (individualResponses.size() == responsesIndexes.size());
  }

  BatchResponse(List<OperationResponse> individualResponses, List<Integer> indexes) {
    super(0, null);
    writeTimestamp = 0;
    rowErrors = ImmutableList.of();
    this.individualResponses = individualResponses;
    this.responsesIndexes = indexes;
  }

  /**
   * Gives the write timestamp that was returned by the Tablet Server.
   * @return a timestamp in milliseconds, 0 if the external consistency mode set in AsyncKuduSession
   * wasn't CLIENT_PROPAGATED
   */
  public long getWriteTimestamp() {
    return writeTimestamp;
  }

  /**
   * Package-private method to get the individual responses.
   * @return a list of OperationResponses
   */
  List<OperationResponse> getIndividualResponses() {
    return individualResponses;
  }

  /**
   * Package-private method to get the responses' order index.
   * @return a list of indexes
   */
  List<Integer> getResponseIndexes() {
    return responsesIndexes;
  }

}
