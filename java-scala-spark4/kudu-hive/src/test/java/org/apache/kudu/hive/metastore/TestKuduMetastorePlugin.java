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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultPartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.test.cluster.MiniKuduCluster;

public class TestKuduMetastorePlugin {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduMetastorePlugin.class);

  private HiveConf clientConf;
  private HiveMetaStoreClient client;
  private MiniKuduCluster miniCluster;

  private EnvironmentContext masterContext() {
    return new EnvironmentContext(
        ImmutableMap.of(KuduMetastorePlugin.KUDU_MASTER_EVENT_KEY, "true"));
  }

  public void startCluster(boolean syncEnabled) throws Exception {
    Configuration hmsConf = MetastoreConf.newMetastoreConf();

    // Avoids a dependency on the default partition expression class, which is
    // contained in the hive-exec jar.
    hmsConf.setClass(MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS.getVarname(),
            DefaultPartitionExpressionProxy.class,
            PartitionExpressionProxy.class);

    // Add the KuduMetastorePlugin.
    hmsConf.setClass(MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS.getVarname(),
            KuduMetastorePlugin.class,
            MetaStoreEventListener.class);

    // Auto create necessary schema on a startup if one doesn't exist.
    MetastoreConf.setBoolVar(hmsConf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, true);
    MetastoreConf.setBoolVar(hmsConf, MetastoreConf.ConfVars.SCHEMA_VERIFICATION, false);

    // Configure a temporary test state directory.
    Path hiveTestDir = Files.createTempDirectory("hive");
    hiveTestDir.toFile().deleteOnExit(); // Ensure we cleanup state.
    LOG.info("Using temporary test state directory:" + hiveTestDir);

    // Set the warehouse directory.
    Path warehouseDir = hiveTestDir.resolve("warehouse");
    MetastoreConf.setVar(hmsConf, MetastoreConf.ConfVars.WAREHOUSE, warehouseDir.toString());
    // For some reason the maven tests fallback to the default warehouse directory
    // and fail without this system property. However, the Gradle tests don't need it.
    System.setProperty(MetastoreConf.ConfVars.WAREHOUSE.getVarname(), warehouseDir.toString());

    Path warehouseExternalDir = hiveTestDir.resolve("external-warehouse");
    // NOTE: We use the string value for backwards compatibility.
    MetastoreConf.setVar(hmsConf, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL,
            warehouseExternalDir.toString());
    System.setProperty(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname(),
            warehouseExternalDir.toString());

    // Set the metastore connection url.
    Path metadb = hiveTestDir.resolve("metadb");
    MetastoreConf.setVar(hmsConf, MetastoreConf.ConfVars.CONNECT_URL_KEY,
            "jdbc:derby:memory:" + metadb.toString() + ";create=true");
    // Set the derby log file.
    Path derbyLogFile = hiveTestDir.resolve("derby.log");
    assertTrue(derbyLogFile.toFile().createNewFile());
    System.setProperty("derby.stream.error.file", derbyLogFile.toString());

    int msPort = MetaStoreUtils.startMetaStore(hmsConf);

    clientConf = new HiveConf();
    clientConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + msPort);

    client = new HiveMetaStoreClient(clientConf);

    MiniKuduCluster.MiniKuduClusterBuilder mcb = new MiniKuduCluster.MiniKuduClusterBuilder();
    if (syncEnabled) {
      mcb.addMasterServerFlag("--hive_metastore_uris=thrift://localhost:" + msPort);
    }
    miniCluster = mcb.numMasterServers(3)
        .numTabletServers(0)
        .build();
  }

  @After
  public void tearDown() {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      if (miniCluster != null) {
        miniCluster.shutdown();
      }
    }
  }

  /**
   * @return a Kudu table descriptor given the storage handler type.
   */
  private Table newKuduTable(String name, String storageHandler) {
    Table table = new Table();
    table.setDbName("default");
    table.setTableName(name);
    table.setTableType(TableType.EXTERNAL_TABLE.toString());
    table.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
    table.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "TRUE");
    table.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
                          storageHandler);
    if (!storageHandler.equals(KuduMetastorePlugin.LEGACY_KUDU_STORAGE_HANDLER)) {
      table.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                            UUID.randomUUID().toString());
      table.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
                           miniCluster.getMasterAddressesAsString());
    }

    // The HMS will NPE if the storage descriptor and partition keys aren't set...
    StorageDescriptor sd = new StorageDescriptor();
    sd.addToCols(new FieldSchema("a", "bigint", ""));
    sd.setSerdeInfo(new SerDeInfo());
    // Unset the location to ensure the default location defined by hive will be used.
    sd.unsetLocation();
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
    startCluster(/* syncEnabled */ true);
    // A non-Kudu table with a Kudu table ID should be rejected.
    try {
      Table table = newTable("table");
      table.getParameters().remove(hive_metastoreConstants.META_TABLE_STORAGE);
      table.getParameters().remove(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY);
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains(
              "non-Kudu table entry must not contain a table ID property"));
    }

    // A Kudu table without a Kudu table ID.
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_TABLE_ID_KEY);
      client.createTable(table, masterContext());
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage(),
                 e.getMessage().contains("Kudu table entry must contain a table ID property"));
    }

    // A Kudu table without master context.
    try {
      Table table = newTable("table");
      client.createTable(table);
      fail();
    } catch (TException e) {
      assertTrue(e.getMessage(),
                 e.getMessage().contains("Kudu tables may not be created through Hive"));
    }

    // A Kudu table without a master address.
    try {
      Table table = newTable("table");
      table.getParameters().remove(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY);
      client.createTable(table, masterContext());
      fail();
    } catch (TException e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains(
              "Kudu table entry must contain a Master addresses property"));
    }

    // Check that creating a valid table is accepted.
    {
      Table table = newTable("table");
      client.createTable(table, masterContext());
      client.dropTable(table.getDbName(), table.getTableName());
    }

    // Check that creating an unsynchronized table is accepted.
    {
      Table table = newTable("table");
      table.setTableType(TableType.EXTERNAL_TABLE.toString());
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
      client.createTable(table);
      client.dropTable(table.getDbName(), table.getTableName());
    }
  }

  @Test
  public void testAlterTableHandler() throws Exception {
    startCluster(/* syncEnabled */ true);
    // Test altering a Kudu (or a legacy) table.
    Table initTable = newTable("table");
    client.createTable(initTable, masterContext());
    // Get the table from the HMS in case any translation occurred.
    Table table = client.getTable(initTable.getDbName(), initTable.getTableName());
    Table legacyTable = newLegacyTable("legacy_table");
    client.createTable(legacyTable, masterContext());
    // Get the table from the HMS in case any translation occurred.
    legacyTable = client.getTable(legacyTable.getDbName(), legacyTable.getTableName());
    try {
      // Check that altering the table succeeds.
      client.alter_table(table.getDbName(), table.getTableName(), table);

      // Try to alter the Kudu table with a different table ID.
      Table newTable = table.deepCopy();
      newTable.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
          UUID.randomUUID().toString());
      try {
        client.alter_table(table.getDbName(), table.getTableName(), newTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table ID does not match the existing HMS entry"));
      }

      // Check that altering the Kudu table with a different table ID while
      // setting kudu.check_id to false succeeds.
      EnvironmentContext noCheckIdCtx = new EnvironmentContext(
          ImmutableMap.of(KuduMetastorePlugin.KUDU_CHECK_ID_KEY, "false"));
      client.alter_table_with_environmentContext(table.getDbName(), table.getTableName(),
          newTable, noCheckIdCtx);
      // Alter back for more testing below.
      client.alter_table_with_environmentContext(table.getDbName(), table.getTableName(), table,
          noCheckIdCtx);

      // Try to alter the Kudu table with no storage handler.
      try {
        Table alteredTable = table.deepCopy();
        alteredTable.getParameters().remove(hive_metastoreConstants.META_TABLE_STORAGE);
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(
            e.getMessage(),
            e.getMessage().contains(
                "Kudu table entry must contain a Kudu storage handler property"));
      }

      // Alter the Kudu table to a different type by setting the external property fails.
      try {
        Table alteredTable = table.deepCopy();
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "FALSE");
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table type may not be altered"));
      }

      // Alter the Kudu table to the same type by setting the table property works.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }

      // Alter the Kudu table to a managed type with the master context succeeds.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.setTableType(TableType.MANAGED_TABLE.toString());
        // Also change the location to avoid MetastoreDefaultTransformer validation
        // that exists in some Hive versions.
        alteredTable.getSd().setLocation(String.format("%s/%s/%s",
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            table.getDbName(), table.getTableName()));
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "FALSE");
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
        client.alter_table_with_environmentContext(table.getDbName(), table.getTableName(),
            alteredTable, masterContext());
      }

      // Alter the Kudu table to a different type by setting the purge property fails.
      try {
        Table alteredTable = table.deepCopy();
        // Also change the location to avoid MetastoreDefaultTransformer validation
        // that exists in some Hive versions.
        alteredTable.getSd().setLocation(String.format("%s/%s/%s",
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            table.getDbName(), table.getTableName()));
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table type may not be altered"));
      }

      // Alter the Kudu table to an external type with the master context succeeds.
      {
        Table alteredTable = table.deepCopy();
        // Also change the location to avoid MetastoreDefaultTransformer validation
        // that exists in some Hive versions.
        alteredTable.getSd().setLocation(String.format("%s/%s/%s",
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            table.getDbName(), table.getTableName()));
        alteredTable.setTableType(TableType.EXTERNAL_TABLE.toString());
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "TRUE");
        client.alter_table_with_environmentContext(table.getDbName(), table.getTableName(),
            alteredTable, masterContext());
      }

      // Altering the table type in a what that maintains sync works.
      // In this case an external purge table is the same as a managed table.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.setTableType(TableType.MANAGED_TABLE.toString());
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "FALSE");
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
        // Also change the location to avoid MetastoreDefaultTransformer validation
        // that exists in some Hive versions.
        alteredTable.getSd().setLocation(String.format("%s/%s/%s",
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            table.getDbName(), table.getTableName()));
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }

      // Altering back the table type in a what that maintains sync works.
      // In this case a managed table is the same as an external purge table.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.setTableType(TableType.EXTERNAL_TABLE.toString());
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "TRUE");
        // Also change the location to avoid MetastoreDefaultTransformer validation
        // that exists in some Hive versions.
        alteredTable.getSd().setLocation(String.format("%s/%s/%s",
            clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
            table.getDbName(), table.getTableName()));
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }

      // Check that adding a column fails.
      table.getSd().addToCols(new FieldSchema("b", "int", ""));
      // Also change the location to avoid MetastoreDefaultTransformer validation
      // that exists in some Hive versions.
      table.getSd().setLocation(String.format("%s/%s/%s",
          clientConf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
          table.getDbName(), table.getTableName()));
      try {
        client.alter_table(table.getDbName(), table.getTableName(), table);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table columns may not be altered through Hive"));
      }

      // Check that adding a column succeeds with the master event property set.
      client.alter_table_with_environmentContext(
          table.getDbName(), table.getTableName(), table, new EnvironmentContext(
              ImmutableMap.of(KuduMetastorePlugin.KUDU_MASTER_EVENT_KEY, "true")));

      // Check that altering a table property unrelated to Kudu succeeds.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.putToParameters("some.random.property", "foo");
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }

      // Check that altering table with Kudu storage handler to legacy format
      // succeeds.
      {
        Table alteredTable = table.deepCopy();
        alteredTable.getParameters().clear();
        alteredTable.setTableType(TableType.EXTERNAL_TABLE.toString());
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
        alteredTable.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "TRUE");
        alteredTable.putToParameters(hive_metastoreConstants.META_TABLE_STORAGE,
            KuduMetastorePlugin.LEGACY_KUDU_STORAGE_HANDLER);
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_TABLE_NAME_KEY,
            "legacy_table");
        alteredTable.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY,
            miniCluster.getMasterAddressesAsString());
        client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
      }
    } finally {
      client.dropTable(table.getDbName(), table.getTableName());
    }

    // Test altering a non-Kudu table.
    {
      table = initTable.deepCopy();
      table.getParameters().clear();
      client.createTable(table);
      table = client.getTable(table.getDbName(), table.getTableName());
      try {

        // Try to alter the table and add a Kudu table ID.
        try {
          Table alteredTable = table.deepCopy();
          alteredTable.putToParameters(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                                       UUID.randomUUID().toString());
          client.alter_table(table.getDbName(), table.getTableName(), alteredTable);
          fail();
        } catch (TException e) {
          assertTrue(e.getMessage(),
                     e.getMessage().contains(
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
          assertTrue(
              e.getMessage(),
              e.getMessage().contains(
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
              miniCluster.getMasterAddressesAsString());
          client.alter_table(legacyTable.getDbName(), legacyTable.getTableName(),
                             alteredTable);
        }
      } finally {
        client.dropTable(table.getDbName(), table.getTableName());
      }
    }

    // Test altering an unsynchronized table is accepted.
    {
      table = initTable.deepCopy();
      table.setTableType(TableType.EXTERNAL_TABLE.name());
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
      client.createTable(table);
      table = client.getTable(table.getDbName(), table.getTableName());
      try {
        client.alter_table(table.getDbName(), table.getTableName(), table);
      } finally {
        client.dropTable(table.getDbName(), table.getTableName());
      }
    }
  }

  @Test
  public void testLegacyTableHandler() throws Exception {
    startCluster(/* syncEnabled */ true);
    // Test creating a legacy Kudu table without context succeeds.
    Table table = newLegacyTable("legacy_table");
    client.createTable(table);
    // Get the table from the HMS in case any translation occurred.
    table = client.getTable(table.getDbName(), table.getTableName());

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
    startCluster(/* syncEnabled */ true);
    // Test dropping a Kudu table.
    Table table = newTable("table");
    client.createTable(table, masterContext());
    try {

      // Test with an invalid table ID.
      try {
        EnvironmentContext envContext = new EnvironmentContext();
        envContext.putToProperties(KuduMetastorePlugin.KUDU_TABLE_ID_KEY,
                                   UUID.randomUUID().toString());
        client.dropTable(table.getCatName(), table.getDbName(), table.getTableName(),
                         /* delete data */ true,
                         /* ignore unknown */ false,
                         envContext);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table ID does not match the HMS entry"));
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
    client.dropTable(table.getCatName(), table.getDbName(), table.getTableName(),
                     /* delete data */ true,
                     /* ignore unknown */ false,
                     envContext);

    // Test dropping a non-Kudu table with a Kudu table ID.
    {
      table.getParameters().clear();
      client.createTable(table);
      try {
        client.dropTable(table.getCatName(), table.getDbName(), table.getTableName(),
            /* delete data */ true,
            /* ignore unknown */ false,
            envContext);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table ID does not match the non-Kudu HMS entry"));
      } finally {
        client.dropTable(table.getDbName(), table.getTableName());
      }
    }

    // Test dropping a non-Kudu table.
    {
      table.getParameters().clear();
      client.createTable(table);
      try {
        client.dropTable(table.getCatName(), table.getDbName(), table.getTableName(),
            /* delete data */ true,
            /* ignore unknown */ false,
            envContext);
        fail();
      } catch (TException e) {
        assertTrue(e.getMessage(),
                   e.getMessage().contains("Kudu table ID does not match the non-Kudu HMS entry"));
      } finally {
        client.dropTable(table.getDbName(), table.getTableName());
      }
    }

    // Test dropping an unsynchronized table is accepted.
    {
      table.getParameters().clear();
      table.setTableType(TableType.EXTERNAL_TABLE.name());
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "TRUE");
      table.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
      client.createTable(table);
      client.dropTable(table.getDbName(), table.getTableName());
    }
  }

  @Test
  public void testSyncDisabled() throws Exception {
    startCluster(/* syncEnabled */ false);

    // A Kudu table should should be allowed to be created via Hive.
    Table table = newTable("table");
    client.createTable(table);
    // Get the table from the HMS in case any translation occurred.
    table = client.getTable(table.getDbName(), table.getTableName());

    // A Kudu table should should be allowed to be altered via Hive.
    // Add a column to the original table.
    Table newTable = table.deepCopy();
    newTable.getSd().addToCols(new FieldSchema("b", "int", ""));
    client.alter_table(table.getDbName(), table.getTableName(), newTable);

    // A Kudu table should should be allowed to be dropped via Hive.
    client.dropTable(table.getDbName(), table.getTableName());
  }

  @Test
  public void testKuduMetadataUnchanged() throws Exception {
    startCluster(/* syncEnabled */ true);

    Table before = newTable("table");

    // Changing from external purge to managed is true (and vice versa).
    {
      Table after = before.deepCopy();
      after.setTableType(TableType.MANAGED_TABLE.name());
      after.putToParameters(KuduMetastorePlugin.EXTERNAL_TABLE_KEY, "FALSE");
      after.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
      assertTrue(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
      assertTrue(KuduMetastorePlugin.kuduMetadataUnchanged(after, before));
    }

    // Changing from external purge to just external is false (and vice versa).
    {
      Table after = before.deepCopy();
      after.putToParameters(KuduMetastorePlugin.EXTERNAL_PURGE_KEY, "FALSE");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(after, before));
    }

    // Changing an unrelated property is true.
    {
      Table after = before.deepCopy();
      after.putToParameters("some.random.property", "foo");
      assertTrue(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Changing location is true.
    {
      Table after = before.deepCopy();
      after.getSd().setLocation("path/to/foo");
      assertTrue(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Changing the master addresses is false.
    {
      Table after = before.deepCopy();
      after.putToParameters(KuduMetastorePlugin.KUDU_MASTER_ADDRS_KEY, "somehost");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Changing the table name is false.
    {
      Table after = before.deepCopy();
      after.setTableName("different");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Changing the table owner is false.
    {
      Table after = before.deepCopy();
      after.setOwner("different");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Changing the table comment is false.
    {
      Table after = before.deepCopy();
      after.putToParameters(KuduMetastorePlugin.COMMENT_KEY, "new comment");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }

    // Adding a column or removing a column is false.
    {
      Table after = before.deepCopy();
      after.getSd().addToCols(new FieldSchema("b", "int", ""));
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(after, before));
    }

    // Changing a column comment is false.
    {
      Table after = before.deepCopy();
      after.getSd().getCols().get(0).setComment("new comment");
      assertFalse(KuduMetastorePlugin.kuduMetadataUnchanged(before, after));
    }
  }
}
