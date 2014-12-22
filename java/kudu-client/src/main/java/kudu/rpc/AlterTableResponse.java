// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

public class AlterTableResponse extends KuduRpcResponse {

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   */
  AlterTableResponse(long ellapsedMillis, String tsUUID) {
    super(ellapsedMillis, tsUUID);
  }
}