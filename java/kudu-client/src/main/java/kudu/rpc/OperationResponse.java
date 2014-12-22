// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

/**
 * Response type for Insert, Update, Delete, and Batch (which is used internally by KuduSession).
 * Provides the Hybrid Time write timestamp returned by the Tablet Server.
 */
public class OperationResponse extends KuduRpcResponse {
  private final long writeTimestamp;

  /**
   * Package-private constructor to be used by the RPCs.
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   * @param writeTimestamp HT's write timestamp.
   */
  OperationResponse(long ellapsedMillis, String tsUUID, long writeTimestamp) {
    super(ellapsedMillis, tsUUID);
    this.writeTimestamp = writeTimestamp;
  }

  /**
   * Gives the write timestamp that was returned by the Tablet Server.
   * @return Timestamp in milliseconds, 0 if the external consistency mode set in KuduSession
   * wasn't CLIENT_PROPAGATED.
   */
  public long getWriteTimestamp() {
    return writeTimestamp;
  }
}
