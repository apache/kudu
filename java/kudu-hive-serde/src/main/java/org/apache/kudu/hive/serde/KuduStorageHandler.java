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

package org.apache.kudu.hive.serde;

import static org.apache.kudu.hive.serde.HiveKuduConstants.KEY_COLUMNS;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.hive.serde.input.KuduTableInputFormat;
import org.apache.kudu.hive.serde.output.HiveKuduOutputFormat;

/**
 * Created by bimal on 4/11/16.
 */

@SuppressWarnings({"RedundantThrows"})
public class KuduStorageHandler extends DefaultStorageHandler
    implements HiveMetaHook, HiveStoragePredicateHandler {

  private static final Log LOG = LogFactory.getLog(KuduStorageHandler.class);

  private Configuration conf;


  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return KuduTableInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveKuduOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return HiveKuduSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {
    configureJobProperties(tableDesc, jobProperties);
  }

  private void configureJobProperties(TableDesc tableDesc,
      Map<String, String> jobProperties) {

    Properties tblProps = tableDesc.getProperties();
    copyPropertiesFromTable(jobProperties, tblProps);

    // set extra configuration properties for kudu-mapreduce
    for (String key : tblProps.stringPropertyNames()) {
      if (key.startsWith(HiveKuduConstants.MR_PROPERTY_PREFIX)) {
        String value = tblProps.getProperty(key);
        jobProperties.put(key, value);
        //Also set configuration for Non Map Reduce Hive calls to the Handler
        conf.set(key, value);
      }
    }
  }

  private void copyPropertiesFromTable(Map<String, String> jobProperties, Properties tblProps) {
    for (Map.Entry<String, String> propToCopy : HiveKuduConstants.KUDU_TO_MAPREDUCE_MAPPING.entrySet()) {
      if (tblProps.containsKey(propToCopy.getValue())) {
        String value = tblProps.getProperty(propToCopy.getValue());
        conf.set(propToCopy.getKey(), value);
        jobProperties.put(propToCopy.getKey(), value);
      }
    }
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider()
      throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf,
      Deserializer deserializer, ExprNodeDesc predicate) {

    return KuduPredicateAnalyzer.decomposePredicate(jobConf, predicate);
  }

  private String getKuduTableName(Table tbl) {

    String tableName;
    tableName = conf.get(HiveKuduConstants.TABLE_NAME);
    if (tableName == null) {
      LOG.info("searching for " + HiveKuduConstants.TABLE_NAME + " in table parameters");
      tableName = tbl.getParameters().get(HiveKuduConstants.TABLE_NAME);
      if (tableName == null) {
        LOG.warn("Kudu Table name was not provided in table properties.");
        LOG.warn("Attempting to use Hive Table name");
        tableName = tbl.getTableName().replaceAll(".*\\.", "");
        LOG.warn("Kudu Table name will be: " + tableName);
      }
    }
    return tableName;
  }


  @Override
  public void preCreateTable(Table tbl)
      throws MetaException {

    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
    if (!isExternal) {
      throw new MetaException("Creating non-external Hive Tables on Kudu Storage is not supported");
    }

    String kuduTableName = getKuduTableName(tbl);
    KuduClient client = HiveKuduBridgeUtils
        .getKuduClient(conf, tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));

    try {
      if (!client.tableExists(kuduTableName)) {
        throw new MetaException("Tried to create external table on non-existing Kudu table");
      }
      String keyColumns = client.openTable(kuduTableName).getSchema().getPrimaryKeyColumns()
          .stream().map(ColumnSchema::getName).collect(Collectors.joining(","));
      tbl.getParameters().put(KEY_COLUMNS, keyColumns);
    } catch (KuduException e) {
      throw new MetaException("Failed to connect to kudu" + e);
    }
  }

  @Override
  public void commitCreateTable(Table tbl) throws MetaException {
    // Nothing to do
  }

  @Override
  public void preDropTable(Table tbl) throws MetaException {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table tbl, boolean deleteData)
      throws MetaException {
    // nothing to do. we will not delete the kudu table.
  }

  @Override
  public void rollbackCreateTable(Table tbl) throws MetaException {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table tbl) throws MetaException {
    // Nothing to do
  }

}
