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

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import org.apache.kudu.replication.wrappedsource.MetricWrappedKuduSource;

/**
 * Provides a configured {@link StreamExecutionEnvironment} for the replication job.
 * This class does not execute the environment; it only prepares it.
 */
public class ReplicationEnvProvider {
  private final ReplicationJobConfig jobConfig;
  private final KuduReaderConfig readerConfig;
  private final KuduWriterConfig writerConfig;
  private final boolean enableFailFast;

  ReplicationEnvProvider(ReplicationJobConfig jobConfig,
                         KuduReaderConfig readerConfig,
                         KuduWriterConfig writerConfig,
                         boolean enableFailFast) {
    this.jobConfig = jobConfig;
    this.readerConfig = readerConfig;
    this.writerConfig = writerConfig;
    this.enableFailFast = enableFailFast;
  }

  ReplicationEnvProvider(ReplicationJobConfig jobConfig,
                         KuduReaderConfig readerConfig,
                         KuduWriterConfig writerConfig) {
    this(jobConfig, readerConfig, writerConfig, false);
  }

  /**
   * Builds and configures the {@link StreamExecutionEnvironment} for the replication job.
   *
   * @return the fully configured {@link StreamExecutionEnvironment}
   * @throws Exception if table initialization or environment setup fails
   */
  public StreamExecutionEnvironment getEnv() throws Exception {
    if (jobConfig.getCreateTable()) {
      try (ReplicationTableInitializer tableInitializer =
              new ReplicationTableInitializer(jobConfig)) {
        tableInitializer.createTableIfNotExists();
      }
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.enableCheckpointing(jobConfig.getCheckpointingIntervalMillis());
    // Use AT_LEAST_ONCE mode since the replication job uses UPSERT semantics which are
    // idempotent. This avoids the overhead of checkpoint barrier alignment while still
    // providing fault tolerance.
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

    // Configure filesystem-based checkpoint storage for production use.
    // Checkpoints are required to be persisted to a filesystem path for fault tolerance.
    Configuration config = new Configuration();
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, jobConfig.getCheckpointsDirectory());
    if (enableFailFast) {
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
    }
    env.configure(config);


    KuduSource<Row> kuduSource = KuduSource.<Row>builder()
            .setReaderConfig(readerConfig)
            .setTableInfo(KuduTableInfo.forTable(jobConfig.getTableName()))
            .setRowResultConverter(new CustomReplicationRowRestultConverter())
            .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
            .setDiscoveryPeriod(Duration.ofSeconds(jobConfig.getDiscoveryIntervalSeconds()))
            .build();

    // TODO(mgreber): remove this line once FLINK-38187 is resolved.
    Source<Row, KuduSourceSplit, KuduSourceEnumeratorState> wrappedSource =
            new MetricWrappedKuduSource<>(kuduSource);

    KuduSink<Row> kuduSink = new KuduSinkBuilder<Row>()
            .setWriterConfig(writerConfig)
            .setTableInfo(KuduTableInfo.forTable(jobConfig.getSinkTableName()))
            .setOperationMapper(new CustomReplicationOperationMapper())
            .build();

    env.fromSource(wrappedSource, WatermarkStrategy.noWatermarks(), "KuduSource")
            .returns(TypeInformation.of(Row.class))
            .sinkTo(kuduSink);

    return env;
  }
}
