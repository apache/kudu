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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

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
 * The plugin enforces that Kudu table entries in the HMS always
 * contain two properties: a Kudu table ID and the Kudu master addresses. It also
 * enforces that non-Kudu tables do not have these properties. The plugin
 * considers entries to be Kudu tables if they contain the Kudu storage handler.
 *
 * Additionally, the plugin checks that when particular events have an
 * environment containing a Kudu table ID, that event only applies
 * to the specified Kudu table. This provides some amount of concurrency safety,
 * so that the Kudu Master can ensure it is operating on the correct table entry.
 */
public class KuduMetastorePlugin extends MetaStoreEventListener {

  @VisibleForTesting
  static final String KUDU_STORAGE_HANDLER = "org.apache.kudu.hive.KuduStorageHandler";
  @VisibleForTesting
  static final String LEGACY_KUDU_STORAGE_HANDLER = "com.cloudera.kudu.hive.KuduStorageHandler";
  @VisibleForTesting
  static final String KUDU_TABLE_ID_KEY = "kudu.table_id";
  @VisibleForTesting
  static final String LEGACY_KUDU_TABLE_NAME = "kudu.table_name";
  @VisibleForTesting
  static final String KUDU_MASTER_ADDRS_KEY = "kudu.master_addresses";
  @VisibleForTesting
  static final String KUDU_MASTER_EVENT = "kudu.master_event";

  public KuduMetastorePlugin(Configuration config) {
    super(config);
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    super.onCreateTable(tableEvent);
    Table table = tableEvent.getTable();

    // Allow non-Kudu tables to be created.
    if (!isKuduTable(table)) {
      // But ensure that the new table does not contain Kudu-specific properties.
      checkNoKuduProperties(table);
      return;
    }

    if (!isKuduMasterAction(tableEvent)) {
      throw new MetaException("Kudu tables may not be created through Hive");
    }

    checkKuduProperties(table);
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) throws MetaException {
    super.onDropTable(tableEvent);

    EnvironmentContext environmentContext = tableEvent.getEnvironmentContext();
    String targetTableId = environmentContext == null ? null :
        environmentContext.getProperties().get(KUDU_TABLE_ID_KEY);

    // If this request doesn't specify a Kudu table ID then allow it to proceed.
    if (targetTableId == null) {
      return;
    }

    // The kudu.master_event property isn't checked, because the kudu.table_id
    // property already implies this event is coming from a Kudu Master.

    Table table = tableEvent.getTable();

    // Check that the table being dropped is a Kudu table.
    if (!isKuduTable(table)) {
      throw new MetaException("Kudu table ID does not match the non-Kudu HMS entry");
    }

    // Check that the table's ID matches the request's table ID.
    if (!targetTableId.equals(table.getParameters().get(KUDU_TABLE_ID_KEY))) {
      throw new MetaException("Kudu table ID does not match the HMS entry");
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
    super.onAlterTable(tableEvent);

    Table oldTable = tableEvent.getOldTable();
    Table newTable = tableEvent.getNewTable();

    // Allow non-Kudu tables (even the legacy ones) to be altered.
    if (!isKuduTable(oldTable) && !isLegacyKuduTable(oldTable)) {
      // But ensure that the alteration isn't introducing Kudu-specific properties.
      checkNoKuduProperties(newTable);
      return;
    }

    // Check the altered table's properties. Kudu tables can be downgraded
    // to the legacy format.
    if (!isLegacyKuduTable(newTable)) {
      checkKuduProperties(newTable);
    }

    // Check that the non legacy Kudu table ID isn't changing.
    if (!isLegacyKuduTable(oldTable) && !isLegacyKuduTable(newTable)) {
      String oldTableId = oldTable.getParameters().get(KUDU_TABLE_ID_KEY);
      String newTableId = newTable.getParameters().get(KUDU_TABLE_ID_KEY);
      if (!newTableId.equals(oldTableId)) {
        throw new MetaException("Kudu table ID does not match the existing HMS entry");
      }
    }

    if (!isKuduMasterAction(tableEvent) &&
        !oldTable.getSd().getCols().equals(newTable.getSd().getCols())) {
      throw new MetaException("Kudu table columns may not be altered through Hive");
    }
  }

  /**
   * Checks whether the table is a Kudu table.
   * @param table the table to check
   * @return {@code true} if the table is a Kudu table, otherwise {@code false}
   */
  private boolean isKuduTable(Table table) {
    return KUDU_STORAGE_HANDLER.equals(table.getParameters()
        .get(hive_metastoreConstants.META_TABLE_STORAGE));
  }

  /**
   * Checks whether the table is a Kudu table with legacy Kudu
   * storage handler.
   *
   * @param table the table to check
   * @return {@code true} if the table is a legacy Kudu table,
   *         otherwise {@code false}
   */
  private boolean isLegacyKuduTable(Table table) {
    return LEGACY_KUDU_STORAGE_HANDLER.equals(table.getParameters()
        .get(hive_metastoreConstants.META_TABLE_STORAGE));
  }

  /**
   * Checks that the Kudu table entry contains the required Kudu table properties.
   * @param table the table to check
   */
  private void checkKuduProperties(Table table) throws MetaException {
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
  private void checkNoKuduProperties(Table table) throws MetaException {
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
  }

  /**
   * Returns true if the event is from the Kudu Master.
   */
  private boolean isKuduMasterAction(ListenerEvent event) {
    EnvironmentContext environmentContext = event.getEnvironmentContext();
    if (environmentContext == null) {
      return false;
    }

    Map<String, String> properties = environmentContext.getProperties();
    if (properties == null) {
      return false;
    }

    return Boolean.parseBoolean(properties.get(KUDU_MASTER_EVENT));
  }
}
