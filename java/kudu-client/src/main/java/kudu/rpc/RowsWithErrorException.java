// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import com.stumbleupon.async.DeferredGroupException;
import kudu.WireProtocol;
import kudu.tserver.Tserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This exception contains a list of Operations that failed and why.
 */
public class RowsWithErrorException extends NonRecoverableException {

  public static final Logger LOG = LoggerFactory.getLogger(RowsWithErrorException.class);

  private final List<RowError> errors;

  private RowsWithErrorException(List<RowError> errors) {
    super("Some or all rows returned with errors");
    assert !errors.isEmpty();
    this.errors = errors;
  }

  /**
   * Get the list of all the errors, per row. Guaranteed to not be empty.
   * @return A list of row errors.
   */
  public List<RowError> getErrors() {
    return errors;
  }

  /**
   *
   * @param errorsPB
   * @param operations
   * @return
   */
  static RowsWithErrorException fromPerRowErrorPB(List<Tserver.WriteResponsePB.PerRowErrorPB>
                                                      errorsPB, List<Operation> operations) {
    List<RowError> errors = new ArrayList<RowError>(errorsPB.size());
    for (Tserver.WriteResponsePB.PerRowErrorPB errorPB : errorsPB) {
      WireProtocol.AppStatusPB statusPB = errorPB.getError();
      RowError error = new RowError(statusPB.getCode().toString(), statusPB.getMessage(),
          operations.get(errorPB.getRowIndex()));
      errors.add(error);
    }
    return new RowsWithErrorException(errors);
  }

  /**
   * Helper method to help with the clumsiness of merging all the RowsWithErrorException that
   * might be contained in a DeferredGroupException. The latter can contain all sorts of
   * exceptions, this method will only merge those that are of type RowsWithErrorException.
   * @param dge Exception returned by {@link KuduSession#flush} after joining or in the errback.
   * @return A RowsWithErrorException that is the merged result of all the
   * RowsWithErrorExceptions contained in DeferredGroupException or null if none of the
   * exceptions were RowsWithErrorException.
   */
  public static RowsWithErrorException fromDeferredGroupException(DeferredGroupException dge) {
    List<RowError> errors = new ArrayList<RowError>();
    for (Object result : dge.results()) {
      if(result instanceof RowsWithErrorException) {
        errors.addAll(((RowsWithErrorException) result).getErrors());
      }
    }
    return errors.size() == 0 ? null : new RowsWithErrorException(errors);
  }

  /**
   * This method will inspect the passed exception to check if all the errors are of the
   * AlreadyPresent type, which due to KUDU-568 is rather frequent when a leader re-election
   * takes place.
   * @param logOtherErrors Boolean to indicate if the errors that aren't of type AlreadyPresent
   *                       should be logged in ERROR, otherwise nothing will be logged.
   * @return True if all the errors are AlreadyPresent, otherwise false.
   */
  public boolean areAllErrorsOfAlreadyPresentType(boolean logOtherErrors) {
    boolean allAlreadyPresent = true;
    for (RowsWithErrorException.RowError error : errors) {

      if (WireProtocol.AppStatusPB.ErrorCode.valueOf(error.getStatus()) !=
          WireProtocol.AppStatusPB.ErrorCode.ALREADY_PRESENT) {
        allAlreadyPresent = false;
        if (logOtherErrors) {
          LOG.error("Row error status: " + error.getStatus() + ", message: " + error.getMessage());
        }
      }
    }
    return allAlreadyPresent;
  }

  /**
   * Wrapper class for a single row error.
   */
  public static class RowError {
    private final String status;
    private final String message;
    private final Operation operation;

    RowError(String errorStatus, String errorMessage, Operation operation) {
      this.status = errorStatus;
      this.message = errorMessage;
      this.operation = operation;
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
  }
}
