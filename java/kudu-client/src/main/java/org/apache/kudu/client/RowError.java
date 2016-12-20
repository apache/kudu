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

import org.apache.kudu.WireProtocol;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.tserver.Tserver;

/**
 * Wrapper class for a single row error.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RowError {
  private final Status status;
  private final Operation operation;
  private final String tsUUID;

  /**
   * Creates a new {@code RowError} with the provided status, operation, and tablet server UUID.
   */
  RowError(Status status, Operation operation, String tsUUID) {
    this.status = status;
    this.operation = operation;
    this.tsUUID = tsUUID;
  }

  /**
   * Creates a new {@code RowError} with the provided status, and operation.
   *
   * This constructor should be used when the operation fails before the tablet
   * lookup is complete.
   */
  RowError(Status status, Operation operation) {
    this(status, operation, null);
  }

  /**
   * Get the status code and message of the row error.
   */
  public Status getErrorStatus() {
    return status;
  }

  /**
   * Get the string-representation of the error code that the tablet server returned.
   * @return A short string representation of the error.
   * @deprecated Please use getErrorStatus() instead. Will be removed in a future version.
   */
  public String getStatus() {
    return status.getCodeName();
  }

  /**
   * Get the error message the tablet server sent.
   * @return The error message.
   * @deprecated Please use getErrorStatus() instead. Will be removed in a future version.
   */
  public String getMessage() {
    return status.getMessage();
  }

  /**
   * Get the Operation that failed.
   * @return The same Operation instance that failed
   */
  public Operation getOperation() {
    return operation;
  }

  /**
   * Get the identifier of the tablet server that sent the error.
   * The UUID may be {@code null} if the failure occurred before sending the row
   * to a tablet server (for instance, if the row falls in a non-covered range partition).
   * @return A string containing a UUID
   */
  public String getTsUUID() {
    return tsUUID;
  }

  @Override
  public String toString() {
    // Intentionally not redacting the row key to make this more useful.
    return "Row error for primary key=" + Bytes.pretty(operation.getRow().encodePrimaryKey()) +
        ", tablet=" + operation.getTablet() +
        ", server=" + tsUUID +
        ", status=" + status.toString();
  }

  /**
   * Converts a PerRowErrorPB into a RowError.
   * @param errorPB a row error in its pb format
   * @param operation the original operation
   * @param tsUUID a string containing the originating TS's UUID
   * @return a row error
   */
  static RowError fromRowErrorPb(Tserver.WriteResponsePB.PerRowErrorPB errorPB,
                                 Operation operation, String tsUUID) {
    WireProtocol.AppStatusPB statusPB = errorPB.getError();
    return new RowError(Status.fromPB(statusPB), operation, tsUUID);
  }
}
