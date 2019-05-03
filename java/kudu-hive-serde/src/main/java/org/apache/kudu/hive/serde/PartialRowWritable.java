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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;

@SuppressWarnings("WeakerAccess")
public class PartialRowWritable implements Writable {

  private PartialRow row;
  private Schema schema;


  public PartialRowWritable(Schema schema, PartialRow row) {
    this.schema = schema;
    this.row = row;
  }

  // for reading the first value in the Kudu KuduRecordReader.createValue()
  public PartialRowWritable(RowResult rowResult) throws IOException {
    this.schema = rowResult.getSchema();
    this.row = schema.newPartialRow();
    HiveKuduBridgeUtils.copyRowResultToPartialRow(rowResult, this.row, this.schema);
  }

  public PartialRow getRow() {
    return row;
  }

  // for updating the value in the Kudu KuduRecordReader.next()
  public void setRow(RowResult rowResult) throws IOException {
    this.schema = rowResult.getSchema();
    this.row = schema.newPartialRow();
    HiveKuduBridgeUtils.copyRowResultToPartialRow(rowResult, this.row, this.schema);
  }

  // Writing our RartialRow into the PartialRow of the Operation that will be applied by the KuduRecordUpserter
  public void mergeInto(PartialRow out) throws IOException {
    HiveKuduBridgeUtils.copyPartialRowToPartialRow(this.row, out, this.schema);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object obj = HiveKuduBridgeUtils.readObject(in, columnSchema.getType());
      HiveKuduBridgeUtils.setPartialRowValue(this.row, columnSchema, obj);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    for (ColumnSchema columnSchema : schema.getColumns()) {
      Object obj = HiveKuduBridgeUtils.getRowValue(this.row, columnSchema);
      HiveKuduBridgeUtils.writeObject(obj, columnSchema.getType(), out);
    }
  }
}
