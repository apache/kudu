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
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;

import org.apache.kudu.client.ReplicaSelection;
import org.apache.kudu.client.SessionConfiguration;

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
   *   <li>{@code job.createTable} (optional) – whether to create the sink table if it not exists.
   *   This setting recreates the partition schema that is present on the source side.
   *   (default: false)</li>
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

    if (params.has("job.createTable")) {
      builder.setCreateTable(params.getBoolean("job.createTable"));
    }

    return builder.build();
  }

  /**
   * Parses reader-specific parameters and constructs a {@link KuduReaderConfig} instance.
   * <p>
   * The following parameters are recognized under the {@code reader.} prefix:
   * <ul>
   *   <li>{@code reader.batchSizeBytes} (optional) – maximum number of bytes to fetch in a single
   *   batch when reading from Kudu</li>
   *   <li>{@code reader.splitSizeBytes} (optional) – target size in bytes for each scan split
   *   when parallelising input</li>
   *   <li>{@code reader.scanRequestTimeout} (optional) – timeout in milliseconds for individual
   *   scan RPCs</li>
   *   <li>{@code reader.prefetching} (optional) – whether to enable pre-fetching of data blocks
   *   (default: {@code false})</li>
   *   <li>{@code reader.keepAlivePeriodMs} (optional) – period in milliseconds after which an
   *   inactive scanner sends a keep-alive message</li>
   *   <li>{@code reader.replicaSelection} (optional) – replica selection strategy, must be one of
   *   the constants defined in {@link org.apache.kudu.client.ReplicaSelection}</li>
   * </ul>
   *
   * @param params  the Flink {@link ParameterTool} containing command-line parameters
   * @param masters comma-separated list of Kudu master addresses for the source cluster
   * @return a fully-constructed {@link KuduReaderConfig} instance
   */
  public static KuduReaderConfig parseReaderConfig(ParameterTool params, String masters) {
    KuduReaderConfig.Builder builder = KuduReaderConfig.Builder.setMasters(masters);

    if (params.has("reader.batchSizeBytes")) {
      builder.setBatchSizeBytes(params.getInt("reader.batchSizeBytes"));
    }
    if (params.has("reader.splitSizeBytes")) {
      builder.setSplitSizeBytes(params.getLong("reader.splitSizeBytes"));
    }
    if (params.has("reader.scanRequestTimeout")) {
      builder.setScanRequestTimeout(params.getLong("reader.scanRequestTimeout"));
    }
    if (params.has("reader.prefetching")) {
      builder.setPrefetching(params.getBoolean("reader.prefetching"));
    }
    if (params.has("reader.keepAlivePeriodMs")) {
      builder.setKeepAlivePeriodMs(params.getLong("reader.keepAlivePeriodMs"));
    }
    if (params.has("reader.replicaSelection")) {
      builder.setReplicaSelection(ReplicaSelection.valueOf(params.get("reader.replicaSelection")));
    }

    return builder.build();
  }

  /**
   * Parses writer-specific parameters and constructs a {@link KuduWriterConfig} instance.
   * <p>
   * The following parameters are recognized under the {@code writer.} prefix:
   * <ul>
   *   <li>{@code writer.flushMode} (optional) – flush consistency mode, one of the values of
   *   {@link org.apache.kudu.client.SessionConfiguration.FlushMode}</li>
   *   <li>{@code writer.operationTimeout} (optional) – timeout in milliseconds for write
   *   operations</li>
   *   <li>{@code writer.maxBufferSize} (optional) – maximum size in bytes of the client-side write
   *   buffer</li>
   *   <li>{@code writer.flushInterval} (optional) – interval in milliseconds at which buffered
   *   operations are flushed automatically</li>
   *   <li>{@code writer.ignoreNotFound} (optional) – whether to ignore NOT_FOUND errors during
   *   deletes/updates (default: {@code false})</li>
   *   <li>{@code writer.ignoreDuplicate} (optional) – whether to ignore duplicate row errors
   *   during inserts (default: {@code false})</li>
   * </ul>
   *
   * @param params  the Flink {@link ParameterTool} containing command-line parameters
   * @param masters comma-separated list of Kudu master addresses for the sink cluster
   * @return a fully-constructed {@link KuduWriterConfig} instance
   */
  public static KuduWriterConfig parseWriterConfig(ParameterTool params, String masters) {
    KuduWriterConfig.Builder builder = KuduWriterConfig.Builder.setMasters(masters);

    if (params.has("writer.flushMode")) {
      builder.setConsistency(
              SessionConfiguration.FlushMode.valueOf(params.get("writer.flushMode")));
    }

    if (params.has("writer.operationTimeout")) {
      builder.setOperationTimeout(params.getLong("writer.operationTimeout"));
    }

    if (params.has("writer.maxBufferSize")) {
      builder.setMaxBufferSize(params.getInt("writer.maxBufferSize"));
    }

    if (params.has("writer.flushInterval")) {
      builder.setFlushInterval(params.getInt("writer.flushInterval"));
    }

    if (params.has("writer.ignoreNotFound")) {
      builder.setIgnoreNotFound(params.getBoolean("writer.ignoreNotFound"));
    }

    if (params.has("writer.ignoreDuplicate")) {
      builder.setIgnoreDuplicate(params.getBoolean("writer.ignoreDuplicate"));
    }

    return builder.build();
  }

}
