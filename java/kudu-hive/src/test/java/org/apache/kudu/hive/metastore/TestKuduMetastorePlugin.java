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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
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
import org.apache.kudu.test.junit.RetryRule;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKuduMetastorePlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduMetastorePlugin.class);

  private HiveConf clientConf;
  private HiveMetaStoreClient client;

  private EnvironmentContext masterContext() {
    return new EnvironmentContext(ImmutableMap.of(KuduMetastorePlugin.KUDU_MASTER_EVENT, "true"));
  }

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Before
  public void setUp() throws Exception {
    HiveConf metastoreConf = new HiveConf();
    // Avoids a dependency on the default partition expression class, which is
    // contained in the hive-exec jar.
    metastoreConf.setClass(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS.varname,
                           MockPartitionExpressionForMetastore.class,
                           PartitionExpressionProxy.class);

    // Add the KuduMetastorePlugin.
    metastoreConf.setClass(HiveConf.ConfVars.METASTORE_TRANSACTIONAL_EVENT_LISTENERS.varname,
                           KuduMetastorePlugin.class,
                           MetaStoreEventListener.class);

    // Auto create necessary schema on a startup if one doesn't exist.
    metastoreConf.setBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_ALL, true);
    metastoreConf.setBoolVar(HiveConf.ConfVars.METASTORE_SCHEMA_VERIFICATION, false);

    // Configure a temporary test state directory.
    Path hiveTestDir = Files.createTempDirectory("hive");
    hiveTestDir.toFile().deleteOnExit(); // Ensure we cleanup state.
    LOG.info("Using temporary test state directory:" + hiveTestDir);

    // Set the warehouse directory.
    Path warehouseDir = hiveTestDir.resolve("warehouse");
    metastoreConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouseDir.toString());
    // For some reason the maven tests fallback to the default warehouse directory
    // and fail without this system property. However, the Gradle tests don't need it.
    System.setProperty(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir.toString());

    // Set the metastore connection url.
    Path metadb = hiveTestDir.resolve("metadb");
    metastoreConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY,
        "jdbc:derby:memory:" + metadb.toString() + ";create=true");
    // Set the derby log file.
    Path derbyLogFile = hiveTestDir.resolve("derby.log");
    derbyLogFile.toFile().createNewFile();
    System.setProperty("derby.stream.error.file", derbyLogFile.toString());

    int msPort = MetaStoreUtils.startMetaStore(metastoreConf);

    clientConf = new HiveConf();
    clientConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);

    client = new HiveMetaStoreClient(clientConf);
  }

  @After
  public void tearDown() {
    if (client != null) {
      client.close();
    }
  }

  /**
   * @return a Kudu table descriptor given the storage handler type.
   */
  private Table newKuduTable(String name, String storageHandler) {
    Table table = new Table();
    table.setDbName("default");
    table.setTableName(name);
    table.setTableType(TableType.MANAGED_TABLE.toString());
    table.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
                          storageHandler);
    if (!storageHandler.equals(KuduMetastorePlugin.LEGACY_KUDU_STORAGE_HANDLER)) {
      table.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                            UUID.randomUUID().toString());
      table.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
                           "localhost");
    }

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

  /**
   * @return a legacy Kudu table descriptor.
   */
  private Table newLegacyTable(String name) {
    return newKuduTable(name, KuduMetastorePlugin.LEGACY_KUDU_STORAGE_HANDLER);
  }

  /**
   * @return a valid Kudu table descriptor.
   */
  private Table newTable(String name) {
    return newKuduTable(name, KuduMetastorePlugin.KUDU_STORAGE_HANDLER);
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

    // A Kudu table without a Kudu table ID.
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_TABLE_ID_KEY);
      client.createTable(table, masterContext());
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Kudu table entry must contain a table ID property"));
    }

    // A Kudu table without master context.
    try {
      Table table = newTable("table");
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Kudu tables may not be created through Hive"));
    }

    // A Kudu table without a master address.
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY);
      client.createTable(table, masterContext());
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage().contains(
          "Kudu table entry must contain a Master addresses property"));
    }

    // Check that creating a valid table is accepted.
    Table table = newTable("table");
    client.createTable(table, masterContext());
    client.dropTable(table.getDbName(), table.getTableName());
  }

  @Test
  public void testAlterTableHandler() throws Exception {
    // Test altering a Kudu (or a legacy) table.
    Table table = newTable("table");
    client.createTable(table, masterContext());
    Table legacyTable = newLegacyTable("legacy_table");
    client.createTable(legacyTable, masterContext());
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

      // Check that adding a column fails.
      table.getSd().addToCols(new FieldSchema("b", "int", ""));
      try {
        client.alter_table(table.getDbName(), table.getTableName(), table);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage().contains(
            "Kudu table columns may not be altered through Hive"));
      }

      // Check that adding a column succeeds with the master event property set.
      client.alter_table_with_environmentContext(
          table.getDbName(), table.getTableName(), table,
          new EnvironmentContext(ImmutableMap.of(KuduMetastorePlugin.KUDU_MASTER_EVENT, "true")));

      // Check that altering table with Kudu storage handler to legacy format
      // succeeds.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.getParameters().clear();
        alteredTable.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
            KuduMetastorePlugin.LEGACY_KUDU_STORAGE_HANDLER);
        alteredTable.putToParameters(KuduMetastorePlugin.LEGACY_KUDU_TABLE_NAME,
            "legacy_table");
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
            "localhost");
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }
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

      // Check that altering the legacy table to use the Kudu storage handler
      // succeeds.
      {
        Table alteredTable = legacyTable.deepCopy();
        alteredTable.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
                                     KuduMetastorePlugin.KUDU_STORAGE_HANDLER);
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                                     UUID.randomUUID().toString());
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
                                    "localhost");
        client.alter_table(legacyTable.getDbName(), legacyTable.getTableName(),
                           alteredTable);
      }
    } finally {
      client.dropTable(table.getDbName(), table.getTableName());
    }
  }

  @Test
  public void testLegacyTableHandler() throws Exception {
    // Test creating a legacy Kudu table without context succeeds.
    Table table = newLegacyTable("legacy_table");
    client.createTable(table);

    // Check that altering legacy table's schema succeeds.
    {
      Table alteredTable = table.deepCopy();
      alteredTable.getSd().addToCols(new FieldSchema("c", "int", ""));
      client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
    }

    // Check that renaming legacy table's schema succeeds.
    final String newTable = "new_table";
    {
      Table alteredTable = table.deepCopy();
      alteredTable.setTableName(newTable);
      client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
    }
    // Test dropping a legacy Kudu table without context succeeds.
    client.dropTable(table.getDbName(), newTable);
  }

  @Test
  public void testDropTableHandler() throws Exception {
    // Test dropping a Kudu table.
    Table table = newTable("table");
    client.createTable(table, masterContext());
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
    client.createTable(table, masterContext());
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
