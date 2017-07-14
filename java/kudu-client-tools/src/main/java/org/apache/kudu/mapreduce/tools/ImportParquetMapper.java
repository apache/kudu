/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.kudu.mapreduce.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.mapreduce.KuduTableMapReduceUtil;

/**
 * Mapper that ingests Apache Parquet lines and turns them into Kudu Inserts.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ImportParquetMapper extends Mapper<LongWritable, Group, NullWritable, Operation> {

  private static final NullWritable NULL_KEY = NullWritable.get();

  private MessageType parquetSchema;

  private KuduTable table;
  private Schema schema;

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   */
  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();
    parquetSchema = MessageTypeParser.parseMessageType(conf.get(
      ImportParquet.PARQUET_INPUT_SCHEMA));

    this.table = KuduTableMapReduceUtil.getTableFromContext(context);
    this.schema = this.table.getSchema();
  }

  /**
   * Convert a line of Parquet data into a Kudu Insert
   */
  @Override
  public void map(LongWritable key, Group value, Context context)
      throws IOException {

    try {
      Insert insert = this.table.newInsert();
      PartialRow row = insert.getRow();
      for (int i = 0; i < parquetSchema.getFields().size(); i++) {
        String colName = parquetSchema.getFields().get(i).getName();
        ColumnSchema col = this.schema.getColumn(colName);
        String colValue = value.getValueToString(i, 0);
        switch (col.getType()) {
          case BOOL:
            row.addBoolean(colName, Boolean.parseBoolean(colValue));
            break;
          case INT8:
            row.addByte(colName, Byte.parseByte(colValue));
            break;
          case INT16:
            row.addShort(colName, Short.parseShort(colValue));
            break;
          case INT32:
            row.addInt(colName, Integer.parseInt(colValue));
            break;
          case INT64:
            row.addLong(colName, Long.parseLong(colValue));
            break;
          case STRING:
            row.addString(colName, colValue);
            break;
          case FLOAT:
            row.addFloat(colName, Float.parseFloat(colValue));
            break;
          case DOUBLE:
            row.addDouble(colName, Double.parseDouble(colValue));
            break;
          default:
            throw new IllegalArgumentException("Type " + col.getType() + " not recognized");
        }
      }
      context.write(NULL_KEY, insert);
    } catch (InterruptedException e) {
      throw new IOException("Failing task since it was interrupted", e);
    }
  }
}
