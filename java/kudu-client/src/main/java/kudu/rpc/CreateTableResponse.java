// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

public class CreateTableResponse extends KuduRpcResponse {

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   */
  CreateTableResponse(long ellapsedMillis) {
    super(ellapsedMillis);
  }
}
