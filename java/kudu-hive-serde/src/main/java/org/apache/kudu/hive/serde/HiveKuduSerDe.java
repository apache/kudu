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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.hive.serde.utils.objectinspectorutils.OIUtils;
import org.apache.kudu.hive.serde.utils.objectinspectorutils.inpectors.RenameStructFieldsStructObjectInspector;


public class HiveKuduSerDe extends AbstractSerDe {


  private KuduStructObjectInspector objectInspector;
  private Schema schema;
  private RenameStructFieldsStructObjectInspector renamed;
  private Map<String, String> fromTo;

  @SuppressWarnings("unused")
  public HiveKuduSerDe() {
  }

  // For Testing Only
  public HiveKuduSerDe(Schema schema) throws SerDeException {
    this.schema = schema;
    this.initOI();
  }

  @Override
  public void initialize(Configuration sysConf, Properties tblProps)
      throws SerDeException {
    try (KuduClient client = HiveKuduBridgeUtils.getKuduClient(sysConf, tblProps)) {
      String tableName = tblProps.getProperty(HiveKuduConstants.TABLE_NAME);
      if (client.tableExists(tableName)) {
        schema = client.openTable(tableName).getSchema();
      }
    } catch (KuduException ex) {
      throw new SerDeException(this.getClass().getName() + " encountered an error connecting to Kudu", ex);
    }
    this.initOI();
  }

  private void initOI() throws SerDeException {
    List<ColumnSchema> columns = schema.getColumns();

    fromTo = new HashMap<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      fromTo.put("_col" + i, columns.get(i).getName());
    }

    this.objectInspector = new KuduStructObjectInspector(schema);
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return PartialRowWritable.class;
  }

  @Override
  public PartialRowWritable serialize(Object rowIn, ObjectInspector inspector)
      throws SerDeException {

    if (fromTo != null) {
      if (this.renamed == null) {
        this.renamed = new RenameStructFieldsStructObjectInspector(fromTo, (StructObjectInspector) inspector);
      }
      PartialRow rowOut = (PartialRow) OIUtils.copyToOtherObjectInspector(rowIn, this.renamed, this.objectInspector);
      return new PartialRowWritable(schema, rowOut);
    }
    throw new IllegalStateException("no column mapping while trying to write Kudu Row");
  }

  @SuppressWarnings("RedundantThrows")
  @Override
  public Object deserialize(Writable record) throws SerDeException {
    return ((PartialRowWritable) record).getRow();
  }


  @Override
  public SerDeStats getSerDeStats() {
    // not available AFAIK
    return null;
  }
}


