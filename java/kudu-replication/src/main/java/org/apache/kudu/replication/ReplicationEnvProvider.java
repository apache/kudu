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
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connector.kudu.sink.KuduSink;
import org.apache.flink.connector.kudu.sink.KuduSinkBuilder;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * Provides a configured {@link StreamExecutionEnvironment} for the replication job.
 * This class does not execute the environment; it only prepares it.
 */
public class ReplicationEnvProvider {
  private final ReplicationJobConfig jobConfig;

  ReplicationEnvProvider(ReplicationJobConfig jobConfig) {
    this.jobConfig = jobConfig;
  }

  /**
   * Builds and configures the {@link StreamExecutionEnvironment} for the replication job.
   *
   * @return the fully configured {@link StreamExecutionEnvironment}
   * @throws Exception if table initialization or environment setup fails
   */
  public StreamExecutionEnvironment getEnv() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // TODO(mgreber): implement and use reader config parsed from the command line.
    KuduSource<Row> kuduSource = KuduSource.<Row>builder()
            .setReaderConfig(KuduReaderConfig.Builder
                  .setMasters(jobConfig.getSourceMasterAddresses()).build())
            .setTableInfo(KuduTableInfo.forTable(jobConfig.getTableName()))
            .setRowResultConverter(new CustomReplicationRowRestultConverter())
            .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
            .setDiscoveryPeriod(Duration.ofSeconds(jobConfig.getDiscoveryIntervalSeconds()))
            .build();

    // TODO(mgreber): implement and use writer config parsed from command line.
    KuduSink<Row> kuduSink = new KuduSinkBuilder<Row>()
            .setWriterConfig(KuduWriterConfig.Builder
                    .setMasters(jobConfig.getSinkMasterAddresses()).build())
            .setTableInfo(KuduTableInfo.forTable(jobConfig.getSinkTableName()))
            .setOperationMapper(new CustomReplicationOperationMapper())
            .build();

    env.fromSource(kuduSource, WatermarkStrategy.noWatermarks(), "KuduSource")
            .returns(TypeInformation.of(Row.class))
            .sinkTo(kuduSink);

    return env;
  }
}
