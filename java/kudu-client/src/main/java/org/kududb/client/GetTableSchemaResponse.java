// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;

@InterfaceAudience.Private
public class GetTableSchemaResponse extends KuduRpcResponse {

  private final Schema schema;

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now.
   * @param schema Table's schema.
   */
  GetTableSchemaResponse(long ellapsedMillis, String tsUUID, Schema schema) {
    super(ellapsedMillis, tsUUID);
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
