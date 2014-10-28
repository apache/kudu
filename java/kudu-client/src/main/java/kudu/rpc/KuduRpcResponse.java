// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

/**
 * Base class for RPC responses.
 */
abstract class KuduRpcResponse {
  private final long elapsedMillis;

  /**
   * Constructor with information common to all RPCs.
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   */
  KuduRpcResponse(long ellapsedMillis) {
    this.elapsedMillis = ellapsedMillis;
  }

  /**
   * Get the number of milliseconds elapsed since the RPC was created up to the moment when this
   * response was created.
   * @return Elapsed time in milliseconds.
   */
  public long getElapsedMillis() {
    return elapsedMillis;
  }
}
