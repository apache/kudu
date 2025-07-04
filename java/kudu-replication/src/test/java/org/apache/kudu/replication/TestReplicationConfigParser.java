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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.connector.writer.KuduWriterConfig;
import org.junit.Test;

import org.apache.kudu.client.ReplicaSelection;
import org.apache.kudu.client.SessionConfiguration;

public class TestReplicationConfigParser {

  @Test
  public void testJobConfigMissingSourceMasterAddressesThrows() {
    String[] args = {
        "--job.sinkMasterAddresses", "sink1:7051",
        "--job.tableName", "my_table"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.sourceMasterAddresses'");
  }

  @Test
  public void testJobConfigMissingSinkMasterAddressesThrows() {
    String[] args = {
        "--job.sourceMasterAddresses", "source1:7051",
        "--job.tableName", "my_table"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.sinkMasterAddresses'");
  }

  @Test
  public void testJobConfigMissingTableNameThrows() {
    String[] args = {
        "--job.sourceMasterAddresses", "source1:7051",
        "--job.sinkMasterAddresses", "sink1:7051"
    };
    ParameterTool params = ParameterTool.fromArgs(args);

    assertThatThrownBy(() -> ReplicationConfigParser.parseJobConfig(params))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("No data for required key 'job.tableName'");
  }

  @Test
  public void testJobConfigAllRequiredParamsPresent() {
    String[] args = {
        "--job.sourceMasterAddresses", "source1:7051",
        "--job.sinkMasterAddresses", "sink1:7051",
        "--job.tableName", "my_table"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    ReplicationJobConfig config = ReplicationConfigParser.parseJobConfig(params);

    assertEquals("source1:7051", config.getSourceMasterAddresses());
    assertEquals("sink1:7051", config.getSinkMasterAddresses());
    assertEquals("my_table", config.getTableName());
    // The default value for restoreOwner is False.
    assertFalse(config.getRestoreOwner());
    assertEquals("", config.getTableSuffix());
    // The default value for the discovery interval is 300.
    assertEquals(300, config.getDiscoveryIntervalSeconds());
  }

  @Test
  public void testReaderConfigAllParamsPresent() {
    String[] args = {
        "--reader.batchSizeBytes", "2048000",
        "--reader.splitSizeBytes", "1048576",
        "--reader.scanRequestTimeout", "45000",
        "--reader.prefetching", "true",
        "--reader.keepAlivePeriodMs", "20000",
        "--reader.replicaSelection", "CLOSEST_REPLICA"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    String masters = "source1:7051,source2:7051";
    KuduReaderConfig config = ReplicationConfigParser.parseReaderConfig(params, masters);

    assertNotNull("Config should not be null", config);
    assertEquals(2048000, config.getBatchSizeBytes());
    assertEquals(1048576, config.getSplitSizeBytes());
    assertEquals(45000, config.getScanRequestTimeout());
    assertEquals(true, config.isPrefetching());
    assertEquals(20000, config.getKeepAlivePeriodMs());
    assertEquals(ReplicaSelection.CLOSEST_REPLICA, config.getReplicaSelection());
  }

  @Test
  public void testReaderConfigBadReplicaSelectionEnum() {
    String masters = "source1:7051,source2:7051";

    String[] argsNotValid = {
        "--reader.replicaSelection", "NOT_A_VALID_ENUM_VALUE",
    };
    ParameterTool paramsNotValid = ParameterTool.fromArgs(argsNotValid);
    assertThatThrownBy(() -> ReplicationConfigParser.parseReaderConfig(paramsNotValid, masters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No enum constant");

    String[] argsEmptyString = {
        "--reader.replicaSelection", "",
    };
    ParameterTool paramsEmptyString = ParameterTool.fromArgs(argsEmptyString);
    assertThatThrownBy(() -> ReplicationConfigParser.parseReaderConfig(paramsEmptyString, masters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No enum constant");

  }

  @Test
  public void testWriterConfigAllParamsPresent() {
    String[] args = {
        "--writer.flushMode", "AUTO_FLUSH_SYNC",
        "--writer.operationTimeout", "60000",
        "--writer.maxBufferSize", "5242880",
        "--writer.flushInterval", "2000",
        "--writer.ignoreNotFound", "true",
        "--writer.ignoreDuplicate", "true"
    };

    ParameterTool params = ParameterTool.fromArgs(args);
    String masters = "sink1:7051,sink2:7051";
    KuduWriterConfig config = ReplicationConfigParser.parseWriterConfig(params, masters);

    assertNotNull("Config should not be null", config);
    assertEquals(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC, config.getFlushMode());
    assertEquals(60000, config.getOperationTimeout());
    assertEquals(5242880, config.getMaxBufferSize());
    assertEquals(2000, config.getFlushInterval());
    assertEquals(true, config.isIgnoreNotFound());
    assertEquals(true, config.isIgnoreDuplicate());
  }

  @Test
  public void testWriterConfigFlushModeBadEnum() {
    String masters = "source1:7051,source2:7051";

    String[] argsNotValid = {
        "--writer.flushMode", "NOT_A_VALID_ENUM_VALUE",
    };
    ParameterTool paramsNotValid = ParameterTool.fromArgs(argsNotValid);
    assertThatThrownBy(() -> ReplicationConfigParser.parseWriterConfig(paramsNotValid, masters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No enum constant");

    String[] argsEmptyString = {
        "--writer.flushMode", "",
    };
    ParameterTool paramsEmptyString = ParameterTool.fromArgs(argsEmptyString);
    assertThatThrownBy(() -> ReplicationConfigParser.parseWriterConfig(paramsEmptyString, masters))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No enum constant");
  }
}
