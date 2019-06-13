// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.client;

import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.Schema;
import org.apache.kudu.security.Token.SignedTokenPB;

@InterfaceAudience.Private
public class GetTableSchemaResponse extends KuduRpcResponse {

  private final Schema schema;
  private final PartitionSchema partitionSchema;
  private final String tableId;
  private final String tableName;
  private final int numReplicas;
  private final SignedTokenPB authzToken;
  private final Map<String, String> extraConfig;

  /**
   * @param elapsedMillis Time in milliseconds since RPC creation to now
   * @param tsUUID the UUID of the tablet server that sent the response
   * @param schema the table's schema
   * @param tableId the UUID of the table in the response
   * @param tableName the name of the table in the response
   * @param numReplicas the table's replication factor
   * @param partitionSchema the table's partition schema
   * @param authzToken an authorization token for use with this table
   * @param extraConfig the table's extra configuration properties
   */
  GetTableSchemaResponse(long elapsedMillis,
                         String tsUUID,
                         Schema schema,
                         String tableId,
                         String tableName,
                         int numReplicas,
                         PartitionSchema partitionSchema,
                         SignedTokenPB authzToken,
                         Map<String, String> extraConfig) {
    super(elapsedMillis, tsUUID);
    this.schema = schema;
    this.partitionSchema = partitionSchema;
    this.tableId = tableId;
    this.tableName = tableName;
    this.numReplicas = numReplicas;
    this.authzToken = authzToken;
    this.extraConfig = extraConfig;
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

  /**
   * Get the table's unique identifier.
   * @return the table's tableId
   */
  public String getTableId() {
    return tableId;
  }

  /**
   * Get the table's name.
   * @return the table's name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get the table's replication factor.
   * @return the table's replication factor
   */
  public int getNumReplicas() {
    return numReplicas;
  }

  /**
   * Get the authorization token for the table.
   * @return the table's authz token
   */
  public SignedTokenPB getAuthzToken() {
    return authzToken;
  }

  /**
   * Get the table's extra configuration properties.
   * @return the table's extra configuration properties
   */
  public Map<String, String> getExtraConfig() {
    return extraConfig;
  }
}
