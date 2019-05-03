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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;

public class KuduStructObjectInspector extends SettableStructObjectInspector {

  private Schema schema;
  private Map<String, KuduStructField> structFields;

  @SuppressWarnings("WeakerAccess")
  public KuduStructObjectInspector(Schema schema) throws SerDeException {
    this.schema = schema;
    structFields = new LinkedHashMap<>();
    for (ColumnSchema column : schema.getColumns()) {
      KuduStructField field = new KuduStructField(column, schema.getColumnIndex(column.getName()));
      structFields.put(field.getFieldName(), field);
    }
  }


  @Override
  public Object create() {
    return schema.newPartialRow();
  }

  @Override
  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    PartialRow row = (PartialRow) struct;
    KuduStructField kuduStructField = ((KuduStructField) field);
    try {
      HiveKuduBridgeUtils.setPartialRowValue(row, kuduStructField.getColumnSchema(), fieldValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return row;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return Lists.newArrayList(structFields.values());
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return structFields.get(fieldName.toLowerCase());
  }


  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {

    if (data == null) {
      return null;
    }
    if (fieldRef == null) {
      throw new RuntimeException("null structField was passed");
    }
    if (!(fieldRef instanceof KuduStructField)) {
      throw new IllegalArgumentException(this.getClass() + "can only use KuduStructField");
    }
    KuduStructField kuduStructField = ((KuduStructField) fieldRef);
    PartialRow row = (PartialRow) data;
    Object result;
    try {
      result = HiveKuduBridgeUtils.getRowValue(row, kuduStructField.getColumnSchema());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    List<Object> result = new ArrayList<>(structFields.size());
    for (KuduStructField fd : structFields.values()) {
      result.add(getStructFieldData(data, fd));
    }
    return result;
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }


  @Override
  public String toString() {
    return this.getTypeName();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (o instanceof KuduStructObjectInspector) {
      return Objects.equal(this.schema, ((KuduStructObjectInspector) o).schema);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return schema.hashCode();
  }
}
