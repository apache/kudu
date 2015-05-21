// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import kudu.tserver.Tserver;

/**
 * Response type for Batch (which is used internally by AsyncKuduSession).
 * Provides the Hybrid Time write timestamp returned by the Tablet Server.
 */
public class BatchResponse extends KuduRpcResponse {

  private final long writeTimestamp;
  private final List<RowError> rowErrors;
  private final List<OperationResponse> individualResponses;

  /**
   * Package-private constructor to be used by the RPCs.
   * @param elapsedMillis time in milliseconds since RPC creation to now
   * @param writeTimestamp HT's write timestamp
   * @param errorsPB a list of row errors, can be empty
   * @param operations the list of operations which created this response
   */
  BatchResponse(long elapsedMillis, String tsUUID, long writeTimestamp,
                List<Tserver.WriteResponsePB.PerRowErrorPB> errorsPB,
                List<Operation> operations) {
    super(elapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
    individualResponses = new ArrayList<>(operations.size());
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
          new OperationResponse(currentOperation.deadlineTracker.getElapsedMillis(), tsUUID,
              writeTimestamp, currentOperation, rowError));
    }
    assert (rowErrors.size() == errorsPB.size());
    assert (individualResponses.size() == operations.size());
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
   * Returns an immutable list of errors, which can be empty.
   * @return an immutable list of row errors
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
  public static List<RowError> collectErrors(List<BatchResponse> responses) {
    List<RowError> errors = new ArrayList<>();
    for (BatchResponse resp : responses) {
      if (resp.hasRowErrors()) {
        errors.addAll(resp.getRowErrors());
      }
    }
    return errors;
  }

}
