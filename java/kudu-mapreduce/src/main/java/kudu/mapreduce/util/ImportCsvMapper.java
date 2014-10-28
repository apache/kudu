/**
 * Portions copyright (c) 2014 Cloudera, Inc.
 * Confidential Cloudera Information: Covered by NDA.
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
package kudu.mapreduce.util;

import kudu.ColumnSchema;
import kudu.Schema;
import kudu.Type;
import kudu.mapreduce.KuduTableMapReduceUtil;
import kudu.rpc.Bytes;
import kudu.rpc.Insert;
import kudu.rpc.KuduTable;
import kudu.rpc.Operation;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Mapper that ingests CSV lines and turns them into Kudu Inserts.
 */
public class ImportCsvMapper extends Mapper<LongWritable, Text, NullWritable, Operation> {

  private static final NullWritable NULL_KEY = NullWritable.get();

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;

  private CsvParser parser;

  private KuduTable table;
  private Schema schema;

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   */
  @Override
  protected void setup(Context context) {
    Configuration conf = context.getConfiguration();

    this.separator = conf.get(ImportCsv.SEPARATOR_CONF_KEY);
    if (this.separator == null) {
      this.separator = ImportCsv.DEFAULT_SEPARATOR;
    }

    this.skipBadLines = conf.getBoolean(ImportCsv.SKIP_LINES_CONF_KEY, true);
    this.badLineCount = context.getCounter(ImportCsv.Counters.BAD_LINES);

    this.parser = new CsvParser(conf.get(ImportCsv.COLUMNS_NAMES_KEY), this.separator);

    this.table = KuduTableMapReduceUtil.getTableFromContext(context);
    this.schema = this.table.getSchema();
  }

  /**
   * Convert a line of CSV text into a Kudu Insert
   */
  @Override
  public void map(LongWritable offset, Text value,
                  Context context)
      throws IOException {
    byte[] lineBytes = value.getBytes();

    try {
      CsvParser.ParsedLine parsed = this.parser.parse(lineBytes, value.getLength());

      Insert insert = this.table.newInsert();
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        String colName = parsed.getColumnName(i);
        ColumnSchema col = this.schema.getColumn(colName);
        String colValue = Bytes.getString(parsed.getLineBytes(), parsed.getColumnOffset(i),
            parsed.getColumnLength(i));
        // We don't have floating point data types, but for the moment we need to be able to
        // parse those numbers. We just remove the period, assuming that we always have 2
        // decimals (like tpch does)
        // TODO remove once we can handle floating points
        // TODO consider having a more general purpose handling in the mean time for when we have
        // any number of decimals aftert the period.
        if (col.getType() != Type.STRING) {
          colValue = StringUtils.replace(colValue, ".", "");
        }
        switch (col.getType()) {
          case INT8:
            insert.addByte(colName, Byte.parseByte(colValue));
            break;
          case UINT8:
            insert.addUnsignedByte(colName, Short.parseShort(colValue));
            break;
          case INT16:
            insert.addShort(colName, Short.parseShort(colValue));
            break;
          case UINT16:
            insert.addUnsignedShort(colName, Integer.parseInt(colValue));
            break;
          case INT32:
            insert.addInt(colName, Integer.parseInt(colValue));
            break;
          case UINT32:
            insert.addUnsignedInt(colName, Integer.parseInt(colValue));
            break;
          case INT64:
            insert.addLong(colName, Long.parseLong(colValue));
            break;
          case UINT64:
            insert.addUnsignedLong(colName, new BigInteger(colValue));
            break;
          case STRING:
            insert.addString(colName, colValue);
            break;
          default:
            throw new IllegalArgumentException("Type " + col.getType() + " not recognized");
        }
      }
      context.write(NULL_KEY, insert);
    } catch (CsvParser.BadCsvLineException badLine) {
      if (this.skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n" + badLine.getMessage());
        this.badLineCount.increment(1);
        return;
      } else {
        throw new IOException("Failing task because of a bad line", badLine);
      }
    } catch (IllegalArgumentException e) {
      if (this.skipBadLines) {
        System.err.println("Bad line at offset: " + offset.get() + ":\n" + e.getMessage());
        this.badLineCount.increment(1);
        return;
      } else {
        throw new IOException("Failing task because of an illegal argument", e);
      }
    } catch (InterruptedException e) {
      throw new IOException("Failing task since it was interrupted", e);
    }
  }
}
