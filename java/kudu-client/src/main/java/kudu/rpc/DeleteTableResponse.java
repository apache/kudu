// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

public class DeleteTableResponse extends KuduRpcResponse {

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   */
  DeleteTableResponse(long ellapsedMillis) {
    super(ellapsedMillis);
  }
}
