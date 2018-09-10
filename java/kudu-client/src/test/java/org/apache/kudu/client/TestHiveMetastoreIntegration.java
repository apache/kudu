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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;

public class TestHiveMetastoreIntegration {

  @Rule
  public KuduTestHarness harness = new KuduTestHarness(
      KuduTestHarness.getBaseClusterBuilder().enableHiveMetastoreIntegration());

  private KuduClient client;

  @Before
  public void setUp() {
    client = harness.getClient();
  }

  @Test(timeout = 100000)
  public void testOverrideTableOwner() throws Exception {
    // Create a table with an overridden owner.
    String tableName = "default.testOverrideTableOwner";
    String owner = "alice";
    CreateTableOptions options = ClientTestUtil.getBasicCreateTableOptions();
    options.setOwner(owner);
    client.createTable(tableName, ClientTestUtil.getBasicSchema(), options);

    // Create an HMS client. Kudu doesn't provide an API to look up the table owner,
    // so it's necessary to use the HMS APIs directly.
    HiveMetastoreConfig hmsConfig = client.getHiveMetastoreConfig();
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hmsConfig.getHiveMetastoreUris());
    hiveConf.setBoolVar(
        HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        hmsConfig.getHiveMetastoreSaslEnabled());

    // Check that the owner of the table in the HMS matches.
    IMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);
    assertEquals(owner, hmsClient.getTable("default", "testOverrideTableOwner").getOwner());

    // Altering the table should not result in a change of ownership.
    client.alterTable(
        tableName, new AlterTableOptions().renameTable("default.testOverrideTableOwner_renamed"));
    assertEquals(owner, hmsClient.getTable("default", "testOverrideTableOwner_renamed").getOwner());
  }
}
