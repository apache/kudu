// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.kudu.replication;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;

/**
 * A configuration object for ReplicationJob used for the Kudu Flink based replication.
 * <p>
 * This object encapsulates the necessary parameters for setting up and running
 * a replication job, including source and sink Kudu master addresses, table name,
 * optional table suffix, replication discovery interval, and an option to restore
 * table ownership.
 * <p>
 */
public class ReplicationJobConfig implements Serializable {
  private static final long serialVersionUID = 7667951599874025860L;

  private final String sourceMasterAddresses;
  private final String sinkMasterAddresses;
  private final String tableName;
  private final boolean restoreOwner;
  private final String tableSuffix;
  private final long discoveryIntervalSeconds;
  private final boolean createTable;

  private ReplicationJobConfig(
          String sourceMasterAddresses,
          String sinkMasterAddresses,
          String tableName,
          boolean restoreOwner,
          String tableSuffix,
          long discoveryIntervalSeconds,
          boolean createTable) {
    this.sourceMasterAddresses = checkNotNull(sourceMasterAddresses,
      "sourceMasterAddresses cannot be null");
    this.sinkMasterAddresses = checkNotNull(sinkMasterAddresses,
      "sinkMasterAddresses cannot be null");
    this.tableName = checkNotNull(tableName, "tableName cannot be null");
    this.restoreOwner = restoreOwner;
    this.tableSuffix = tableSuffix != null ? tableSuffix : "";
    this.discoveryIntervalSeconds = discoveryIntervalSeconds;
    this.createTable = createTable;
  }

  public String getSourceMasterAddresses() {
    return sourceMasterAddresses;
  }

  public String getSinkMasterAddresses() {
    return sinkMasterAddresses;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean getRestoreOwner() {
    return restoreOwner;
  }

  public String getTableSuffix() {
    return tableSuffix;
  }

  public long getDiscoveryIntervalSeconds() {
    return discoveryIntervalSeconds;
  }

  public boolean getCreateTable() {
    return createTable;
  }

  public String getSinkTableName() {
    return tableName + tableSuffix;
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ReplicationJobConfig}. */
  public static class Builder {
    private String sourceMasterAddresses;
    private String sinkMasterAddresses;
    private String tableName;
    // By default, we don't restore the owner.
    private boolean restoreOwner = false;
    // By default, there is no tableSuffix for the replicated table.
    private String tableSuffix = "";
    // The default discover interval is 5 minutes.
    private long discoveryIntervalSeconds = 5 * 60;
    private boolean createTable = false;

    public Builder setSourceMasterAddresses(String sourceMasterAddresses) {
      this.sourceMasterAddresses = sourceMasterAddresses;
      return this;
    }

    public Builder setSinkMasterAddresses(String sinkMasterAddresses) {
      this.sinkMasterAddresses = sinkMasterAddresses;
      return this;
    }

    public Builder setTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder setRestoreOwner(boolean restoreOwner) {
      this.restoreOwner = restoreOwner;
      return this;
    }

    public Builder setTableSuffix(String tableSuffix) {
      this.tableSuffix = tableSuffix;
      return this;
    }

    public Builder setDiscoveryIntervalSeconds(long discoveryIntervalSeconds) {
      this.discoveryIntervalSeconds = discoveryIntervalSeconds;
      return this;
    }

    public Builder setCreateTable(boolean createTable) {
      this.createTable = createTable;
      return this;
    }

    public ReplicationJobConfig build() {
      return new ReplicationJobConfig(
              sourceMasterAddresses,
              sinkMasterAddresses,
              tableName,
              restoreOwner,
              tableSuffix,
              discoveryIntervalSeconds,
              createTable);
    }
  }


}
