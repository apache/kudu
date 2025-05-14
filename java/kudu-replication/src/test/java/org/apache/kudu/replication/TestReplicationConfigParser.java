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

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Test;

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

}
