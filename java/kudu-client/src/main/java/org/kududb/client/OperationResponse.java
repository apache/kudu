// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import kudu.WireProtocol;
import kudu.tserver.Tserver;

/**
 * Response type for Insert, Update, Delete, and Batch (which is used internally by AsyncKuduSession).
 * Provides the Hybrid Time write timestamp returned by the Tablet Server.
 */
public class OperationResponse extends KuduRpcResponse {

  private final long writeTimestamp;
  private final List<RowError> rowErrors;

  /**
   * Package-private constructor to be used by the RPCs.
   * @param ellapsedMillis time in milliseconds since RPC creation to now
   * @param writeTimestamp HT's write timestamp
   * @param errorsPB a list of row errors, can be empty
   * @param operations the list of operations which created this response
   */
  OperationResponse(long ellapsedMillis, String tsUUID, long writeTimestamp,
                    List<Tserver.WriteResponsePB.PerRowErrorPB> errorsPB,
                    List<Operation> operations) {
    super(ellapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
    if (errorsPB.isEmpty()) {
      rowErrors = Collections.emptyList();
    } else {
      rowErrors = listFromPerRowErrorPB(errorsPB, operations, tsUUID);
    }
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
   * Returns an immutable list of errors, which can be empty.
   * @return an immutable list of row errors.
   */
  public List<RowError> getRowErrors() {
    return Collections.unmodifiableList(rowErrors);
  }

  /**
   * Tells if this operation response contains any row errors.
   * @return true if this operation response has errors, else false
   */
  public boolean hasRowErrors() {
    return !rowErrors.isEmpty();
  }

  /**
   * Utility method that collects all the row errors from the given list of responses.
   * @param responses a list of operation responses to collect the row errors from
   * @return a combined list of row errors
   */
  public static List<RowError> collectErrors(List<OperationResponse> responses) {
    List<RowError> errors = new ArrayList<>();
    for (OperationResponse resp : responses) {
      if (resp.hasRowErrors()) {
        errors.addAll(resp.getRowErrors());
      }
    }
    return errors;
  }

  /**
   * Converts the given list of row errors in their PB format into a list of augmented row errors
   * using the passed list of operations and TS's UUID.
   * @param errorsPB a list of row errors in their pb format
   * @param operations a list of the original operations
   * @param tsUUID a string containing the originating TS's UUID
   * @return a list of augmented row errors
   */
  private static List<RowError> listFromPerRowErrorPB(List<Tserver.WriteResponsePB.PerRowErrorPB>
                                                          errorsPB, List<Operation> operations,
                                                      String tsUUID) {
    assert (operations.size() >= errorsPB.size());
    List<RowError> errors = new ArrayList<>(errorsPB.size());
    for (Tserver.WriteResponsePB.PerRowErrorPB errorPB : errorsPB) {
      WireProtocol.AppStatusPB statusPB = errorPB.getError();
      RowError error = new RowError(statusPB.getCode().toString(), statusPB.getMessage(),
          operations.get(errorPB.getRowIndex()), tsUUID);
      errors.add(error);
    }
    return errors;
  }

  /**
   * Wrapper class for a single row error.
   */
  public static class RowError {
    private final String status;
    private final String message;
    private final Operation operation;
    private final String tsUUID;

    RowError(String errorStatus, String errorMessage, Operation operation, String tsUUID) {
      this.status = errorStatus;
      this.message = errorMessage;
      this.operation = operation;
      this.tsUUID = tsUUID;
    }

    /**
     * Get the string-representation of the error code that the tablet server returned.
     * @return A short string representation of the error.
     */
    public String getStatus() {
      return status;
    }

    /**
     * Get the error message the tablet server sent.
     * @return The error message.
     */
    public String getMessage() {
      return message;
    }

    /**
     * Get the Operation that failed.
     * @return The same Operation instance that failed.
     */
    public Operation getOperation() {
      return operation;
    }

    /**
     * Get the identifier of the tablet server that sent the error.
     * @return A string containing a UUID.
     */
    public String getTsUUID() {
      return tsUUID;
    }

    @Override
    public String toString() {
      return "Row error for key=" + Bytes.pretty(operation.key()) +
          ", tablet=" + operation.getTablet().getTabletIdAsString() +
          ", server=" + tsUUID +
          ", status=" + status +
          ", message=" + message;
    }
  }
}
