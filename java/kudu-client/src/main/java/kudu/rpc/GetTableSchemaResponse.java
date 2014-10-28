// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import kudu.Schema;

public class GetTableSchemaResponse extends KuduRpcResponse {

  private final Schema schema;

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   * @param schema Table's schema.
   */
  GetTableSchemaResponse(long ellapsedMillis, Schema schema) {
    super(ellapsedMillis);
    this.schema = schema;
  }

  /**
   * Get the table's schema.
   * @return Table's schema.
   */
  public Schema getSchema() {
    return schema;
  }
}
