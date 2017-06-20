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
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.tserver.Tserver;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OperationResponse extends KuduRpcResponse {

  private final long writeTimestamp;
  private final RowError rowError;
  private final Operation operation;

  /**
   * Package-private constructor to build an OperationResponse with a row error in the pb format.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param writeTimestamp HT's write timestamp
   * @param operation the operation that created this response
   * @param errorPB a row error in pb format, can be null
   */
  OperationResponse(long elapsedMillis, String tsUUID, long writeTimestamp,
                    Operation operation, Tserver.WriteResponsePB.PerRowErrorPB errorPB) {
    super(elapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
    this.rowError = errorPB == null ? null : RowError.fromRowErrorPb(errorPB, operation, tsUUID);
    this.operation = operation;
  }

  /**
   * Package-private constructor to build an OperationResponse with a row error.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param writeTimestamp HT's write timestamp
   * @param operation the operation that created this response
   * @param rowError a parsed row error, can be null
   */
  OperationResponse(long elapsedMillis, String tsUUID, long writeTimestamp,
                    Operation operation, RowError rowError) {
    super(elapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
    this.rowError = rowError;
    this.operation = operation;
  }

  /**
   * Utility method that collects all the row errors from the given list of responses.
   * @param responses a list of operation responses to collect the row errors from
   * @return a combined list of row errors
   */
  public static List<RowError> collectErrors(List<OperationResponse> responses) {
    List<RowError> errors = new ArrayList<>(responses.size());
    for (OperationResponse resp : responses) {
      if (resp.hasRowError()) {
        errors.add(resp.getRowError());
      }
    }
    return errors;
  }

  /**
   * Gives the write timestamp that was returned by the Tablet Server.
   * @return a long representing a HybridTime-encoded timestamp
   */
  @InterfaceAudience.Private
  public long getWriteTimestampRaw() {
    return writeTimestamp;
  }

  /**
   * Returns a row error. If {@link #hasRowError()} returns false, then this method returns null.
   * @return a row error, or null if the operation was successful
   */
  public RowError getRowError() {
    return rowError;
  }

  /**
   * Tells if this operation response contains a row error.
   * @return true if this operation response has errors, else false
   */
  public boolean hasRowError() {
    return rowError != null;
  }

  /**
   * Returns the operation associated with this response.
   * @return an operation, cannot be null
   */
  Operation getOperation() {
    return operation;
  }
}
