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

package org.apache.kudu.mapreduce.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.RowResult;

/**
 * Mapper that ingests Kudu rows and turns them into CSV lines.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ExportCsvMapper extends Mapper<NullWritable, RowResult, NullWritable,Text> {

  private static final NullWritable NULL_KEY = NullWritable.get();

  /** Column seperator */
  private String separator;

  private Schema schema;

  /**
   * Handles initializing this class with objects specific to it.
   */
  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();
    this.separator = conf.get(ExportCsv.SEPARATOR_CONF_KEY, ExportCsv.DEFAULT_SEPARATOR);
  }

  /**
   * Converts Kudu RowResult into a line of CSV text.
   */
  @Override
  public void map(NullWritable key, RowResult value, Context context) throws IOException {
    this.schema = value.getSchema();
    try {
      context.write(NULL_KEY, new Text(rowResultToString(value)));
    } catch (InterruptedException e) {
      throw new IOException("Failing task since it was interrupted", e);
    }
  }

  private String rowResultToString(RowResult value) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);
      if (i != 0) {
        buf.append(this.separator);
      }

      switch (col.getType()) {
        case INT8:
          buf.append(value.getByte(i));
          break;
        case INT16:
          buf.append(value.getShort(i));
          break;
        case INT32:
          buf.append(value.getInt(i));
          break;
        case INT64:
        case UNIXTIME_MICROS:
          buf.append(value.getLong(i));
          break;
        case STRING:
          buf.append(value.getString(i));
          break;
        case BINARY:
          buf.append(Bytes.pretty(value.getBinaryCopy(i)));
          break;
        case FLOAT:
          buf.append(value.getFloat(i));
          break;
        case DOUBLE:
          buf.append(value.getDouble(i));
          break;
        case BOOL:
          buf.append(value.getBoolean(i));
          break;
        case DATE:
          buf.append(value.getDate(i).toString());
          break;
        default:
          buf.append("<unknown type!>");
          break;
      }
    }
    return buf.toString();
  }
}
