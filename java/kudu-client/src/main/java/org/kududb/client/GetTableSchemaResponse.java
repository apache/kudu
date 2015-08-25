// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;

@InterfaceAudience.Private
public class GetTableSchemaResponse extends KuduRpcResponse {

  private final Schema schema;
  private final PartitionSchema partitionSchema;

  /**
   * @param ellapsedMillis Time in milliseconds since RPC creation to now
   * @param schema the table's schema
   * @param partitionSchema the table's partition schema
   */
  GetTableSchemaResponse(long ellapsedMillis,
                         String tsUUID,
                         Schema schema,
                         PartitionSchema partitionSchema) {
    super(ellapsedMillis, tsUUID);
    this.schema = schema;
    this.partitionSchema = partitionSchema;
  }

  /**
   * Get the table's schema.
   * @return Table's schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the table's partition schema.
   * @return the table's partition schema
   */
  public PartitionSchema getPartitionSchema() {
    return partitionSchema;
  }
}
