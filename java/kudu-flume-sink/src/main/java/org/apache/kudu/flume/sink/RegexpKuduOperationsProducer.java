/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kudu.flume.sink;

import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

/**
 * A regular expression operations producer that generates one or more Kudu
 * {@link Insert} or {@link Upsert} operations per Flume {@link Event} by
 * parsing the event {@code body} using a regular expression. Values are
 * coerced to the types of the named columns in the Kudu table.
 *
 * <p>Example: If the Kudu table has the schema:
 *
 * <pre>
 * key INT32
 * name STRING</pre>
 *
 * <p>and {@code producer.pattern = (?<key>\\d+),(?<name>\\w+)} then
 * {@code RegexpKuduOperationsProducer} will parse the string:
 *
 * <pre>|12345,Mike||54321,Todd|</pre>
 *
 * into the rows: {@code (key=12345, name=Mike)} and {@code (key=54321, name=Todd)}.
 *
 * <p>Note: This class relies on JDK7 named capturing groups, which are
 * documented in {@link Pattern}. The name of each capturing group must
 * correspond to a column name in the destination Kudu table.
 *
 * <p><strong><code>RegexpKuduOperationsProducer</code> Flume Configuration Parameters</strong></p>
 *
 * <table cellpadding=3 cellspacing=0 border=1 summary="Flume Configuration Parameters">
 * <tr>
 *   <th>Property Name</th>
 *   <th>Default</th>
 *   <th>Required?</th>
 *   <th>Description</th>
 * </tr>
 * <tr>
 *   <td>producer.pattern</td>
 *   <td></td>
 *   <td>Yes</td>
 *   <td>The regular expression used to parse the event body.</td>
 * </tr>
 * <tr>
 *   <td>producer.charset</td>
 *   <td>utf-8</td>
 *   <td>No</td>
 *   <td>The character set of the event body.</td>
 * </tr>
 * <tr>
 *   <td>producer.operation</td>
 *   <td>upsert</td>
 *   <td>No</td>
 *   <td>Operation type used to write the event to Kudu. Must be either
 *   {@code insert} or {@code upsert}.</td>
 * </tr>
 * <tr>
 *   <td>producer.skipMissingColumn</td>
 *   <td>false</td>
 *   <td>No</td>
 *   <td>What to do if a column in the Kudu table has no corresponding capture group.
 *   If set to {@code true}, a warning message is logged and the operation is still attempted.
 *   If set to {@code false}, an exception is thrown and the sink will not process the
 *   {@code Event}, causing a Flume {@code Channel} rollback.
 * </tr>
 * <tr>
 *   <td>producer.skipBadColumnValue</td>
 *   <td>false</td>
 *   <td>No</td>
 *   <td>What to do if a value in the pattern match cannot be coerced to the required type.
 *   If set to {@code true}, a warning message is logged and the operation is still attempted.
 *   If set to {@code false}, an exception is thrown and the sink will not process the
 *   {@code Event}, causing a Flume {@code Channel} rollback.
 * </tr>
 * <tr>
 *   <td>producer.warnUnmatchedRows</td>
 *   <td>true</td>
 *   <td>No</td>
 *   <td>Whether to log a warning about payloads that do not match the pattern. If set to
 *   {@code false}, event bodies with no matches will be silently dropped.</td>
 * </tr>
 * </table>
 *
 * @see Pattern
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegexpKuduOperationsProducer implements KuduOperationsProducer {
  private static final Logger logger = LoggerFactory.getLogger(RegexpKuduOperationsProducer.class);
  private static final String INSERT = "insert";
  private static final String UPSERT = "upsert";
  private static final List<String> validOperations = Lists.newArrayList(UPSERT, INSERT);

  public static final String PATTERN_PROP = "pattern";
  public static final String ENCODING_PROP = "encoding";
  public static final String DEFAULT_ENCODING = "utf-8";
  public static final String OPERATION_PROP = "operation";
  public static final String DEFAULT_OPERATION = UPSERT;
  public static final String SKIP_MISSING_COLUMN_PROP = "skipMissingColumn";
  public static final boolean DEFAULT_SKIP_MISSING_COLUMN = false;
  public static final String SKIP_BAD_COLUMN_VALUE_PROP = "skipBadColumnValue";
  public static final boolean DEFAULT_SKIP_BAD_COLUMN_VALUE = false;
  public static final String WARN_UNMATCHED_ROWS_PROP = "skipUnmatchedRows";
  public static final boolean DEFAULT_WARN_UNMATCHED_ROWS = true;

  private KuduTable table;
  private Pattern pattern;
  private Charset charset;
  private String operation;
  private boolean skipMissingColumn;
  private boolean skipBadColumnValue;
  private boolean warnUnmatchedRows;

  public RegexpKuduOperationsProducer() {
  }

  @Override
  public void configure(Context context) {
    String regexp = context.getString(PATTERN_PROP);
    Preconditions.checkArgument(regexp != null,
        "Required parameter %s is not specified",
        PATTERN_PROP);
    try {
      pattern = Pattern.compile(regexp);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format("The pattern '%s' is invalid", regexp), e);
    }
    String charsetName = context.getString(ENCODING_PROP, DEFAULT_ENCODING);
    try {
      charset = Charset.forName(charsetName);
    } catch (IllegalArgumentException e) {
      throw new FlumeException(
          String.format("Invalid or unsupported charset %s", charsetName), e);
    }
    operation = context.getString(OPERATION_PROP, DEFAULT_OPERATION).toLowerCase();
    Preconditions.checkArgument(
        validOperations.contains(operation),
        "Unrecognized operation '%s'",
        operation);
    skipMissingColumn = context.getBoolean(SKIP_MISSING_COLUMN_PROP,
        DEFAULT_SKIP_MISSING_COLUMN);
    skipBadColumnValue = context.getBoolean(SKIP_BAD_COLUMN_VALUE_PROP,
        DEFAULT_SKIP_BAD_COLUMN_VALUE);
    warnUnmatchedRows = context.getBoolean(WARN_UNMATCHED_ROWS_PROP,
        DEFAULT_WARN_UNMATCHED_ROWS);
  }

  @Override
  public void initialize(KuduTable table) {
    this.table = table;
  }

  @Override
  public List<Operation> getOperations(Event event) throws FlumeException {
    String raw = new String(event.getBody(), charset);
    Matcher m = pattern.matcher(raw);
    boolean match = false;
    Schema schema = table.getSchema();
    List<Operation> ops = Lists.newArrayList();
    while (m.find()) {
      match = true;
      Operation op;
      switch (operation) {
        case UPSERT:
          op = table.newUpsert();
          break;
        case INSERT:
          op = table.newInsert();
          break;
        default:
          throw new FlumeException(
              String.format("Unrecognized operation type '%s' in getOperations(): " +
                  "this should never happen!", operation));
      }
      PartialRow row = op.getRow();
      for (ColumnSchema col : schema.getColumns()) {
        try {
          coerceAndSet(m.group(col.getName()), col.getName(), col.getType(), row);
        } catch (NumberFormatException e) {
          String msg = String.format(
              "Raw value '%s' couldn't be parsed to type %s for column '%s'",
              raw, col.getType(), col.getName());
          logOrThrow(skipBadColumnValue, msg, e);
        } catch (IllegalArgumentException e) {
          String msg = String.format(
              "Column '%s' has no matching group in '%s'",
              col.getName(), raw);
          logOrThrow(skipMissingColumn, msg, e);
        } catch (Exception e) {
          throw new FlumeException("Failed to create Kudu operation", e);
        }
      }
      ops.add(op);
    }
    if (!match && warnUnmatchedRows) {
      logger.warn("Failed to match the pattern '{}' in '{}'", pattern, raw);
    }
    return ops;
  }

  /**
   * Coerces the string `rawVal` to the type `type` and sets the resulting
   * value for column `colName` in `row`.
   *
   * @param rawVal the raw string column value
   * @param colName the name of the column
   * @param type the Kudu type to convert `rawVal` to
   * @param row the row to set the value in
   * @throws NumberFormatException if `rawVal` cannot be cast as `type`.
   */
  private void coerceAndSet(String rawVal, String colName, Type type, PartialRow row)
      throws NumberFormatException {
    switch (type) {
      case INT8:
        row.addByte(colName, Byte.parseByte(rawVal));
        break;
      case INT16:
        row.addShort(colName, Short.parseShort(rawVal));
        break;
      case INT32:
        row.addInt(colName, Integer.parseInt(rawVal));
        break;
      case INT64:
        row.addLong(colName, Long.parseLong(rawVal));
        break;
      case BINARY:
        row.addBinary(colName, rawVal.getBytes(charset));
        break;
      case STRING:
        row.addString(colName, rawVal);
        break;
      case BOOL:
        row.addBoolean(colName, Boolean.parseBoolean(rawVal));
        break;
      case FLOAT:
        row.addFloat(colName, Float.parseFloat(rawVal));
        break;
      case DOUBLE:
        row.addDouble(colName, Double.parseDouble(rawVal));
        break;
      case UNIXTIME_MICROS:
        row.addLong(colName, Long.parseLong(rawVal));
        break;
      default:
        logger.warn("got unknown type {} for column '{}'-- ignoring this column",
            type, colName);
    }
  }

  private void logOrThrow(boolean log, String msg, Exception e)
      throws FlumeException {
    if (log) {
      logger.warn(msg, e);
    } else {
      throw new FlumeException(msg, e);
    }
  }

  @Override
  public void close() {
  }
}
