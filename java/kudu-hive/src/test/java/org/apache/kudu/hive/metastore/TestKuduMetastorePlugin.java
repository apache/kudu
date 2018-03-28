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

package org.apache.kudu.hive.metastore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.UUID;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.MockPartitionExpressionForMetastore;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestKuduMetastorePlugin {
  private static HiveConf clientConf;
  private HiveMetaStoreClient client;

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {
    HiveConf metastoreConf = new HiveConf();
    // Avoids a dependency on the default partition expression class, which is
    // contained in the hive-exec jar.
    metastoreConf.setClass(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
                           MockPartitionExpressionForMetastore.class,
                           PartitionExpressionProxy.class);
    int msPort = MetaStoreUtils.startMetaStore(metastoreConf);

    clientConf = new HiveConf();
    clientConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);
  }

  @Before
  public void createClient() throws Exception {
    client = new HiveMetaStoreClient(clientConf);
  }

  @After
  public void closeClient() {
    if (client != null) {
      client.close();
    }
  }

  /**
   * @return a valid Kudu table descriptor.
   */
  private static Table newTable(String name) {
    Table table = new Table();
    table.setDbName("default");
    table.setTableName(name);
    table.setTableType(TableType.MANAGED_TABLE.toString());
    table.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
                          KuduMetastorePlugin.KUDU_STORAGE_HANDLER);
    table.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                          UUID.randomUUID().toString());
    table.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
                          "localhost");

    // The HMS will NPE if the storage descriptor and partition keys aren't set...
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToCols(new FieldSchema("a", "bigint", ""));
    sd.setSerdeInfo(new SerDeInfo());
    sd.setLocation(String.format("%s/%s/%s",
                                 clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                                 table.getDbName(), table.getTableName()));
    table.setSd(sd);
    table.setPartitionKeys(Lists.<FieldSchema>newArrayList());

    return table;
  }

  @Test
  public void testCreateTableHandler() throws Exception {

    // A non-Kudu table with a Kudu table ID should be rejected.
    try {
      Table table = newTable("table");
      table.getParameters().remove(hive_metastoreConstants.META_TABLE_STORAGE);
      table.getParameters().remove(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY);
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains(
          "non-Kudu table entry must not contain a table ID property"));
    }

    // A non-Kudu table with a Kudu master address should be rejected.
    try {
      Table table = newTable("table");
      table.getParameters().remove(hive_metastoreConstants.META_TABLE_STORAGE);
      table.getParameters().remove(KuduMetastorePlugin.KUDU_TABLE_ID_KEY);
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains(
          "non-Kudu table entry must not contain a Master addresses property"));
    }

    // A Kudu table without a Kudu table ID.
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_TABLE_ID_KEY);
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Kudu table entry must contain a table ID property"));
    }

    // A Kudu table without a master address
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY);
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains(
          "Kudu table entry must contain a Master addresses property"));
    }

    // Check that creating a valid table is accepted.
    Table table = newTable("table");
    client.createTable(table);
    client.dropTable(table.getDbName(), table.getTableName());
  }

  @Test
  public void testAlterTableHandler() throws Exception {
    // Test altering a Kudu table.
    Table table = newTable("table");
    client.createTable(table);
    try {

      // Try to alter the Kudu table with a different table ID.
      try {
        client.alter_table(table.getDbName(), table.getTableName(), newTable(table.getTableName()));
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains("Kudu table ID does not match the existing HMS entry"));
      }

      // Try to alter the Kudu table with no storage handler.
      try {
        Table alteredTable = table.deepCopy();
        alteredTable.getParameters().remove(hive_metastoreConstants.META_TABLE_STORAGE);
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains(
            "Kudu table entry must contain a Kudu storage handler property"));
      }

      // Check that altering the table succeeds.
      client.alter_table(table.getDbName(), table.getTableName(), table);
    } finally {
      client.dropTable(table.getDbName(), table.getTableName());
    }

    // Test altering a non-Kudu table.
    table.getParameters().clear();
    client.createTable(table);
    try {

      // Try to alter the table and add a Kudu table ID.
      try {
        Table alteredTable = table.deepCopy();
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                                     UUID.randomUUID().toString());
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains(
            "non-Kudu table entry must not contain a table ID property"));
      }

      // Try to alter the table and set a Kudu storage handler.
      try {
        Table alteredTable = table.deepCopy();
        alteredTable.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
                                     KuduMetastorePlugin.KUDU_STORAGE_HANDLER);
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains(
            "non-Kudu table entry must not contain the Kudu storage handler"));
      }

      // Check that altering the table succeeds.
      client.alter_table(table.getDbName(), table.getTableName(), table);
    } finally {
      client.dropTable(table.getDbName(), table.getTableName());
    }
  }

  @Test
  public void testDropTableHandler() throws Exception {
    // Test dropping a Kudu table.
    Table table = newTable("table");
    client.createTable(table);
    try {

      // Test with an invalid table ID.
      try {
        EnvironmentContext envContext = new EnvironmentContext();
        envContext.putToProperties(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                                   UUID.randomUUID().toString());
        client.dropTable(table.getDbName(), table.getTableName(),
                         /* delete data */ true,
                         /* ignore unknown */ false,
                         envContext);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains("Kudu table ID does not match the HMS entry"));
      }
    } finally {
      // Dropping a Kudu table without context should succeed.
      client.dropTable(table.getDbName(), table.getTableName());
    }

    // Test dropping a Kudu table with the correct ID.
    client.createTable(table);
    EnvironmentContext envContext = new EnvironmentContext();
    envContext.putToProperties(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                               table.getParameters().get(KuduMetastorePlugin.KUDU_TABLE_ID_KEY));
    client.dropTable(table.getDbName(), table.getTableName(),
                     /* delete data */ true,
                     /* ignore unknown */ false,
                     envContext);

    // Test dropping a non-Kudu table.
    table.getParameters().clear();
    client.createTable(table);
    try {
      client.dropTable(table.getDbName(), table.getTableName(),
                       /* delete data */ true,
                       /* ignore unknown */ false,
                       envContext);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Kudu table ID does not match the non-Kudu HMS entry"));
    } finally {
      client.dropTable(table.getDbName(), table.getTableName());
    }
  }
}
