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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.kudu.client.HiveMetastoreConfig;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * The {@code KuduMetastorePlugin} intercepts DDL operations on Kudu table entries
 * in the HMS, and validates that they are consistent. It's meant to be run as a
 * transactional event listener, which requires it being on the classpath of the
 * Hive Metastore, and the following configuration in {@code hive-site.xml}:
 *
 * <pre>
 * {@code
 *  <property>
 *    <name>hive.metastore.transactional.event.listeners</name>
 *    <value>org.apache.kudu.hive.metastore.KuduMetastorePlugin</value>
 *  </property>
 * }
 * </pre>
 *
 * The plugin enforces that managed Kudu table entries in the HMS always contain
 * two properties: a Kudu table ID and the Kudu master addresses. It also
 * enforces that non-Kudu tables do not have these properties (except cases
 * when upgrading tables with legacy Kudu storage handler to be Kudu tables
 * or downgrading from the other way around). The plugin considers entries
 * to be Kudu tables if they contain the Kudu storage handler.
 *
 * Additionally, the plugin checks that when particular events have an
 * environment containing a Kudu table ID, that event only applies
 * to the specified Kudu table. This provides some amount of concurrency
 * safety, so that the Kudu Master can ensure it is operating on the correct
 * table entry.
 *
 * Note that such validation does not apply to tables with legacy Kudu
 * storage handler and will be skipped if system env KUDU_SKIP_HMS_PLUGIN_VALIDATION
 * is set to non-zero.
 */
public class KuduMetastorePlugin extends MetaStoreEventListener {

  @VisibleForTesting
  static final String KUDU_STORAGE_HANDLER = "org.apache.hadoop.hive.kudu.KuduStorageHandler";
  @VisibleForTesting
  static final String LEGACY_KUDU_STORAGE_HANDLER = "com.cloudera.kudu.hive.KuduStorageHandler";
  @VisibleForTesting
  static final String KUDU_CLUSTER_ID_KEY = "kudu.cluster_id";
  @VisibleForTesting
  static final String KUDU_TABLE_ID_KEY = "kudu.table_id";
  @VisibleForTesting
  static final String KUDU_TABLE_NAME_KEY = "kudu.table_name";
  @VisibleForTesting
  static final String KUDU_MASTER_ADDRS_KEY = "kudu.master_addresses";
  @VisibleForTesting
  static final String KUDU_MASTER_EVENT_KEY = "kudu.master_event";
  @VisibleForTesting
  static final String KUDU_CHECK_ID_KEY = "kudu.check_id";
  // The key should keep in sync with the one used in
  // org.apache.hadoop.hive.metastore.MetaStoreUtils.isExternalTable().
  @VisibleForTesting
  static final String EXTERNAL_TABLE_KEY = "EXTERNAL";

  static final String EXTERNAL_PURGE_KEY = "external.table.purge";

  static final String COMMENT_KEY = "comment";

  // System env to track if the HMS plugin validation should be skipped.
  static final String SKIP_VALIDATION_ENV = "KUDU_SKIP_HMS_PLUGIN_VALIDATION";

  // System env to force sync enabled/disabled without a call to the master.
  // This is useful for testing and could be useful as an escape hatch if there
  // are too many requests to the master.
  static final String SYNC_ENABLED_ENV = "KUDU_HMS_SYNC_ENABLED";

  // System env to set a custom sasl protocol name for the Kudu client.
  // TODO(ghenke): Use a Hive config parameter from the KuduStorageHandler instead.
  static final String SASL_PROTOCOL_NAME_ENV = "KUDU_SASL_PROTOCOL_NAME";

  // Maps lists of master addresses to KuduClients to cache clients.
  private static final Map<String, KuduClient> KUDU_CLIENTS =
      new ConcurrentHashMap<String, KuduClient>();

  public KuduMetastorePlugin(Configuration config) {
    super(config);
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    super.onCreateTable(tableEvent);

    if (skipsValidation()) {
      return;
    }

    Table table = tableEvent.getTable();

    // Only validate synchronized tables.
    if (!isSynchronizedTable(table)) {
      return;
    }

    // Allow non-Kudu tables to be created.
    if (!isKuduTable(table)) {
      // But ensure that the new table does not contain Kudu-specific properties.
      checkNoKuduProperties(table);
      return;
    }

    // In the case of table creation all Kudu tables must have
    // the master addresses property. We explicitly check for it here
    // so that `kuduSyncEnabled` below doesn't return false for Kudu
    // tables that are missing the master addresses property.
    checkMasterAddrsProperty(table);

    // Only validate tables for clusters with HMS sync enabled.
    if (!kuduSyncEnabled(tableEvent, table)) {
      return;
    }

    checkKuduProperties(table);

    if (!isKuduMasterAction(tableEvent)) {
      throw new MetaException("Kudu tables may not be created through Hive");
    }
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    super.onDropTable(tableEvent);

    if (skipsValidation()) {
      return;
    }

    Table table = tableEvent.getTable();

    // Only validate synchronized tables.
    if (!isSynchronizedTable(table)) {
      return;
    }

    EnvironmentContext environmentContext = tableEvent.getEnvironmentContext();
    String targetTableId = environmentContext == null ? null :
        environmentContext.getProperties().get(KUDU_TABLE_ID_KEY);

    // Allow non-Kudu tables to be dropped.
    if (!isKuduTable(table)) {
      // However, make sure it doesn't have a table id from the context.
      // The table id is only meant for Kudu tables and set by the Kudu master.
      if (targetTableId != null) {
        throw new MetaException("Kudu table ID does not match the non-Kudu HMS entry");
      }
      return;
    }

    // Only validate tables for clusters with HMS sync enabled.
    if (!kuduSyncEnabled(tableEvent, table)) {
      return;
    }

    // If this request doesn't specify a Kudu table ID then allow it to proceed.
    // Drop table requests that don't come from the Kudu master may not set the table ID,
    // e.g. when dropping tables via Hive, or dropping orphaned HMS entries.
    // Such tables are dropped in Kudu by name via the notification listener.
    if (targetTableId == null) {
      return;
    }

    // The kudu.master_event property isn't checked, because the kudu.table_id
    // property already implies this event is coming from a Kudu Master.

    // Check that the table's ID matches the request's table ID.
    if (!targetTableId.equals(table.getParameters().get(KUDU_TABLE_ID_KEY))) {
      throw new MetaException("Kudu table ID does not match the HMS entry");
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    super.onAlterTable(tableEvent);

    if (skipsValidation()) {
      return;
    }

    Table oldTable = tableEvent.getOldTable();
    Table newTable = tableEvent.getNewTable();

    // Allow non-Kudu tables to be altered.
    if (!isKuduTable(oldTable) && !isLegacyKuduTable(oldTable)) {
      // Allow non-Kudu tables to be altered without introducing Kudu-specific
      // properties.
      checkNoKuduProperties(newTable);
      return;
    }

    // Check if the alter changes any of the Kudu metadata.
    // If not, we can skip checking for synchronization given Kudu doesn't care about the changes.
    // We primarily expect this case to occur when a table is migrated from a managed table
    // to an external table with the purge property. This change is effectively a no-op to Kudu.
    if (kuduMetadataUnchanged(oldTable, newTable)) {
      return;
    }

    // Only validate tables for clusters with HMS sync enabled.
    if (!kuduSyncEnabled(tableEvent, oldTable)) {
      return;
    }

    // Prevent altering the table type (managed/external) of Kudu tables (or via
    // altering table properties 'EXTERNAL' or `external.table.purge`) in a way
    // that changes if a table is synchronized. This can cause orphaned tables.
    // Note: This doesn't prevent altering the table type for legacy tables
    // because they should continue to work as they always have primarily for
    // migration purposes.
    // The Kudu master is allowed to make these changes if necessary as it is a trusted user.
    if (isKuduTable(oldTable) &&
        !isKuduMasterAction(tableEvent) &&
        isSynchronizedTable(oldTable) != isSynchronizedTable(newTable) ) {
      throw new MetaException("Kudu table type may not be altered");
    }

    // Only validate synchronized tables.
    if (!isSynchronizedTable(oldTable)) {
      return;
    }

    if (isLegacyKuduTable(oldTable)) {
      if (isKuduTable(newTable)) {
        // Allow legacy tables to be upgraded to Kudu tables. Validate the upgraded
        // table entry contains the required Kudu table properties, and that any
        // potential schema alterations are coming from the Kudu master.
        checkKuduProperties(newTable);
        checkOnlyKuduMasterCanAlterSchema(tableEvent, oldTable, newTable);
        return;
      }
      // Allow legacy tables to be altered without introducing Kudu-specific
      // properties.
      checkNoKuduProperties(newTable);
    } else if (isKuduTable(oldTable)) {
      if (isLegacyKuduTable(newTable)) {
        // Allow Kudu tables to be downgraded to legacy tables. Validate the downgraded
        // table entry does not contain Kudu-specific properties, and that any potential
        // schema alterations are coming from the Kudu master.
        checkNoKuduProperties(newTable);
        checkOnlyKuduMasterCanAlterSchema(tableEvent, oldTable, newTable);
        return;
      }
      // Validate the new table entry contains the required Kudu table properties, and
      // that any potential schema alterations are coming from the Kudu master.
      checkKuduProperties(newTable);
      checkOnlyKuduMasterCanAlterSchema(tableEvent, oldTable, newTable);
      // Check that the Kudu table ID isn't changing.

      if (checkTableID(tableEvent)) {
        String oldTableId = oldTable.getParameters().get(KUDU_TABLE_ID_KEY);
        String newTableId = newTable.getParameters().get(KUDU_TABLE_ID_KEY);
        if (!newTableId.equals(oldTableId)) {
          throw new MetaException("Kudu table ID does not match the existing HMS entry");
        }
      }
    }
  }

  /**
   * Checks whether the table is a Kudu table.
   * @param table the table to check
   * @return {@code true} if the table is a Kudu table, otherwise {@code false}
   */
  private static boolean isKuduTable(Table table) {
    String storageHandler = table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
    return KUDU_STORAGE_HANDLER.equals(storageHandler);
  }

  /**
   * Checks whether the table is a Kudu table with legacy Kudu
   * storage handler.
   *
   * @param table the table to check
   * @return {@code true} if the table is a legacy Kudu table,
   *         otherwise {@code false}
   */
  private static boolean isLegacyKuduTable(Table table) {
    return LEGACY_KUDU_STORAGE_HANDLER.equals(table.getParameters()
        .get(hive_metastoreConstants.META_TABLE_STORAGE));
  }

  /**
   * Checks whether the table is an external table.
   *
   * @param table the table to check
   * @return {@code true} if the table is an external table,
   *         otherwise {@code false}
   */
  private static boolean isExternalTable(Table table) {
    String isExternal = table.getParameters().get(EXTERNAL_TABLE_KEY);
    if (isExternal == null) {
      return false;
    }
    return Boolean.parseBoolean(isExternal);
  }

  /**
   * Checks whether the table should be purged when deleted, i.e. the
   * underlying Kudu table should be deleted when the HMS table entry is
   * deleted.
   *
   * @param table the table to check
   * @return {@code true} if the table is a managed table or has external.table.purge = true,
   *         otherwise {@code false}
   */
  private static boolean isPurgeTable(Table table) {
    boolean externalPurge =
        Boolean.parseBoolean(table.getParameters().getOrDefault(EXTERNAL_PURGE_KEY, "false"));
    return TableType.MANAGED_TABLE.name().equals(table.getTableType()) || externalPurge;
  }

  /**
   * Checks whether the table is considered a synchronized Kudu table.
   *
   * @param table the table to check
   * @return {@code true} if the table is a managed table or an external table with
   *         `external.table.purge = true`, otherwise {@code false}
   */
  private static boolean isSynchronizedTable(Table table) {
    return TableType.MANAGED_TABLE.name().equals(table.getTableType()) ||
        (isExternalTable(table) && isPurgeTable(table));
  }

  /**
   * Checks that the Kudu table entry contains the required Kudu table properties.
   * @param table the table to check
   */
  private static void checkKuduProperties(Table table) throws MetaException {
    if (!isKuduTable(table)) {
      throw new MetaException(String.format(
          "Kudu table entry must contain a Kudu storage handler property (%s=%s)",
          hive_metastoreConstants.META_TABLE_STORAGE,
          KUDU_STORAGE_HANDLER));
    }
    String tableId = table.getParameters().get(KUDU_TABLE_ID_KEY);
    if (tableId == null || tableId.isEmpty()) {
      throw new MetaException(String.format(
          "Kudu table entry must contain a table ID property (%s)", KUDU_TABLE_ID_KEY));
    }
    checkMasterAddrsProperty(table);
  }

  /**
   * Checks that the Kudu table entry contains the `kudu.master_addresses` property.
   * @param table the table to check
   */
  private static void checkMasterAddrsProperty(Table table) throws MetaException {
    String masterAddresses = table.getParameters().get(KUDU_MASTER_ADDRS_KEY);
    if (masterAddresses == null || masterAddresses.isEmpty()) {
      throw new MetaException(String.format(
          "Kudu table entry must contain a Master addresses property (%s)", KUDU_MASTER_ADDRS_KEY));
    }
  }

  /**
   * Checks that the non-Kudu table entry does not contain Kudu-specific table properties.
   * @param table the table to check
   */
  private static void checkNoKuduProperties(Table table) throws MetaException {
    if (isKuduTable(table)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain the Kudu storage handler (%s=%s)",
          hive_metastoreConstants.META_TABLE_STORAGE,
          KUDU_STORAGE_HANDLER));
    }
    if (table.getParameters().containsKey(KUDU_TABLE_ID_KEY)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain a table ID property (%s)",
          KUDU_TABLE_ID_KEY));
    }
    if (table.getParameters().containsKey(KUDU_CLUSTER_ID_KEY)) {
      throw new MetaException(String.format(
          "non-Kudu table entry must not contain a cluster ID property (%s)",
          KUDU_CLUSTER_ID_KEY));
    }
  }

  /**
   * Checks that the metadata relevant to Kudu is unchanged between the before and after table.
   * See HmsCatalog::PopulateTable in hms_catalog.cc for a reference to the relevant metadata.
   *
   * @param before the table to be altered
   * @param after the new altered table
   * @return true if no Kudu relevant metadata has changed
   */
  @VisibleForTesting
  static boolean kuduMetadataUnchanged(Table before, Table after) {
    // If any of the Kudu table properties have changed, return false.
    Map<String, String> beforeParams = before.getParameters();
    Map<String, String> afterParams = after.getParameters();
    if (!Objects.equals(beforeParams.get(hive_metastoreConstants.META_TABLE_STORAGE),
            afterParams.get(hive_metastoreConstants.META_TABLE_STORAGE)) ||
        !Objects.equals(beforeParams.get(KUDU_MASTER_ADDRS_KEY),
            afterParams.get(KUDU_MASTER_ADDRS_KEY)) ||
        !Objects.equals(beforeParams.get(KUDU_TABLE_ID_KEY),
            afterParams.get(KUDU_TABLE_ID_KEY)) ||
        !Objects.equals(beforeParams.get(KUDU_TABLE_NAME_KEY),
            afterParams.get(KUDU_TABLE_NAME_KEY)) ||
        !Objects.equals(beforeParams.get(KUDU_CLUSTER_ID_KEY),
            afterParams.get(KUDU_CLUSTER_ID_KEY))) {
      return false;
    }

    // If the table synchronization has changed, return false.
    // Kudu doesn't care if the table is managed vs external with the purge property set
    // to true, it just cares that he table is synchronized.
    if (isSynchronizedTable(before) != isSynchronizedTable(after)) {
      return false;
    }

    // If the table database, name, owner, or comment have changed, return false.
    if (!Objects.equals(before.getDbName(), after.getDbName()) ||
        !Objects.equals(before.getTableName(), after.getTableName()) ||
        !Objects.equals(before.getOwner(), after.getOwner()) ||
        !Objects.equals(beforeParams.get(COMMENT_KEY),
            afterParams.get(COMMENT_KEY))) {
      return false;
    }

    // If the column count has changed, return false.
    List<FieldSchema> beforeCols = before.getSd().getCols();
    List<FieldSchema> afterCols = after.getSd().getCols();
    if (beforeCols.size() != afterCols.size()) {
      return false;
    }

    // If any of the columns have changed (name, type, or comment), return false.
    // We don't have the Kudu internal column ID, so we assume the column index
    // in both tables aligns if there are no changes.
    for (int i = 0; i < beforeCols.size(); i++) {
      FieldSchema beforeCol = beforeCols.get(i);
      FieldSchema afterCol = afterCols.get(i);
      if (!Objects.equals(beforeCol.getName(), afterCol.getName()) ||
          !Objects.equals(beforeCol.getType(), afterCol.getType()) ||
          !Objects.equals(beforeCol.getComment(), afterCol.getComment())) {
        return false;
      }
    }

    // Kudu doesn't have metadata related to all other changes.
    return true;
  }

  /**
   * Checks that the table schema can only be altered by an action from the Kudu Master.
   * @param tableEvent
   * @param oldTable the table to be altered
   * @param newTable the new altered table
   */
  private static void checkOnlyKuduMasterCanAlterSchema(AlterTableEvent tableEvent,
      Table oldTable, Table newTable) throws MetaException {
    if (!isKuduMasterAction(tableEvent) &&
        !oldTable.getSd().getCols().equals(newTable.getSd().getCols())) {
      throw new MetaException("Kudu table columns may not be altered through Hive");
    }
  }

  /**
   * Returns true if the event is from the Kudu Master.
   */
  private static boolean isKuduMasterAction(ListenerEvent event) {
    EnvironmentContext environmentContext = event.getEnvironmentContext();
    if (environmentContext == null) {
      return false;
    }

    Map<String, String> properties = environmentContext.getProperties();
    if (properties == null) {
      return false;
    }

    if (!properties.containsKey(KUDU_MASTER_EVENT_KEY)) {
      return false;
    }

    return Boolean.parseBoolean(properties.get(KUDU_MASTER_EVENT_KEY));
  }

  /**
   * Returns true if the table ID should be verified on an event.
   * Defaults to true.
   */
  private static boolean checkTableID(ListenerEvent event) {
    EnvironmentContext environmentContext = event.getEnvironmentContext();
    if (environmentContext == null) {
      return true;
    }

    Map<String, String> properties = environmentContext.getProperties();
    if (properties == null) {
      return true;
    }

    if (!properties.containsKey(KUDU_CHECK_ID_KEY)) {
      return true;
    }

    return Boolean.parseBoolean(properties.get(KUDU_CHECK_ID_KEY));
  }

  /**
   * Returns true if the system env is set to skip validation.
   */
  private static boolean skipsValidation() {
    String skipValidation = System.getenv(SKIP_VALIDATION_ENV);
    if (skipValidation == null || skipValidation.isEmpty() ||
        Integer.parseInt(skipValidation) == 0) {
      return false;
    }
    return true;
  }

  /**
   * Returns true if HMS synchronization is configured on the Kudu cluster
   * backing the HMS table.
   */
  private static boolean kuduSyncEnabled(ListenerEvent event, Table table) throws MetaException {
    // If SYNC_ENABLED_ENV is set, use it instead of contacting the Kudu master.
    String envEnabled = System.getenv(SYNC_ENABLED_ENV);
    if (envEnabled != null && !envEnabled.isEmpty()) {
      return Integer.parseInt(envEnabled) == 1;
    }

    // If the request is from the Kudu Master, we know HMS sync is enabled
    // and can avoid another request.
    if (isKuduMasterAction(event)) {
      return true;
    }

    String masterAddresses = table.getParameters().get(KUDU_MASTER_ADDRS_KEY);
    if (masterAddresses == null || masterAddresses.isEmpty()) {
      // A table without master addresses is not synchronized,
      // it may not even be a Kudu table.
      return false;
    }

    KuduClient kuduClient = getKuduClient(masterAddresses);
    HiveMetastoreConfig hmsConfig;
    try {
      hmsConfig = kuduClient.getHiveMetastoreConfig();
    } catch (KuduException e) {
      throw new MetaException(
          String.format("Error determining if Kudu's integration with " +
              "the Hive Metastore is enabled: %s", e.getMessage()));
    }

    // If the HiveMetastoreConfig is not null, then the HMS synchronization
    // is enabled in the Kudu cluster.
    return hmsConfig != null;
  }

  private static KuduClient getKuduClient(String kuduMasters) {
    KuduClient client = KUDU_CLIENTS.get(kuduMasters);
    if (client == null) {
      try {
        client = UserGroupInformation.getLoginUser().doAs(
            (PrivilegedExceptionAction<KuduClient>) () ->
                new KuduClient.KuduClientBuilder(kuduMasters)
                    .saslProtocolName(getSaslProtocolName())
                    .build()
        );
      } catch (IOException | InterruptedException e) {
        throw new RuntimeException("Failed to create the Kudu client");
      }
      KUDU_CLIENTS.put(kuduMasters, client);
    }
    return client;
  }

  private static String getSaslProtocolName() {
    String saslProtocolName = System.getenv(SASL_PROTOCOL_NAME_ENV);
    if (saslProtocolName == null || saslProtocolName.isEmpty()) {
      saslProtocolName = "kudu";
    }
    return saslProtocolName;
  }
}
