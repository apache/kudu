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

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Utility class for parsing replication-related configuration parameters from
 * {@link ParameterTool}. It expects parameters to follow a prefix-based naming convention:
 * <ul>
 *   <li><b>job.*</b> – general replication job configuration (e.g., source/sink master addresses,
 *   table name)</li>
 *   <li><b>reader.*</b> – reader-specific options for the source Kudu table</li>
 *   <li><b>writer.*</b> – writer-specific options for the sink Kudu table</li>
 * </ul>
 */
public class ReplicationConfigParser {

  /**
   * Parses replication job-related parameters and constructs a
   * {@link ReplicationJobConfig} instance.
   * <p>
   * The following parameters are expected under the {@code job.} prefix:
   * <ul>
   *   <li>{@code job.sourceMasterAddresses} (required) – comma-separated list of source Kudu master
   *   addresses</li>
   *   <li>{@code job.sinkMasterAddresses} (required) – comma-separated list of sink Kudu master
   *   addresses</li>
   *   <li>{@code job.tableName} (required) – name of the source table to replicate</li>
   *   <li>{@code job.restoreOwner} (optional) – whether to restore table ownership on
   *   the sink (default: false)</li>
   *   <li>{@code job.tableSuffix} (optional) – suffix to append to the sink table name
   *   (default: empty string)</li>
   *   <li>{@code job.discoveryIntervalSeconds} (optional) – interval in seconds at which the source
   *   tries to perform a diff scan to get new or changed data (default: 300)</li>
   * </ul>
   *
   * @param params the Flink {@code ParameterTool} containing command-line parameters
   * @return a fully constructed {@link ReplicationJobConfig} instance
   * @throws RuntimeException if any required parameter is missing
   */
  public static ReplicationJobConfig parseJobConfig(ParameterTool params) {
    ReplicationJobConfig.Builder builder = ReplicationJobConfig.builder()
            .setSourceMasterAddresses(params.getRequired("job.sourceMasterAddresses"))
            .setSinkMasterAddresses(params.getRequired("job.sinkMasterAddresses"))
            .setTableName(params.getRequired("job.tableName"));

    if (params.has("job.restoreOwner")) {
      builder.setRestoreOwner(params.getBoolean("job.restoreOwner"));
    }

    if (params.has("job.tableSuffix")) {
      builder.setTableSuffix(params.get("job.tableSuffix"));
    }

    if (params.has("job.discoveryIntervalSeconds")) {
      builder.setDiscoveryIntervalSeconds(params.getInt("job.discoveryIntervalSeconds"));
    }

    return builder.build();
  }

  //TODO(mgreber): add parseReaderConfig()

  //TODO(mgreber): add parseWriterConfig()

}
