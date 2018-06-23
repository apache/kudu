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

package org.apache.kudu.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilities useful for cluster testing.
 */
public abstract class ClientTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ClientTestUtil.class);

  public static final Callback<Object, Object> defaultErrorCB = new Callback<Object, Object>() {
    @Override
    public Object call(Object arg) throws Exception {
      if (arg == null) {
        return null;
      }
      if (arg instanceof Exception) {
        LOG.warn("Got exception", (Exception) arg);
      } else {
        LOG.warn("Got an error response back {}", arg);
      }
      return new Exception("cannot recover from error: " + arg);
    }
  };

  /**
   * Counts the rows from the {@code scanner} until exhaustion. It doesn't require the scanner to
   * be new, so it can be used to finish scanning a previously-started scan.
   */
  public static int countRowsInScan(AsyncKuduScanner scanner, long timeoutMs) throws Exception {
    final AtomicInteger counter = new AtomicInteger();

    Callback<Object, RowResultIterator> cb = new Callback<Object, RowResultIterator>() {
      @Override
      public Object call(RowResultIterator arg) throws Exception {
        if (arg == null) return null;
        counter.addAndGet(arg.getNumRows());
        return null;
      }
    };

    while (scanner.hasMoreRows()) {
      Deferred<RowResultIterator> data = scanner.nextRows();
      data.addCallbacks(cb, defaultErrorCB);
      data.join(timeoutMs);
    }
    return counter.get();
  }

  /**
   * Same as {@link #countRowsInScan(AsyncKuduScanner, long)}, but defaults the timeout to 60
   * seconds.
   */
  public static int countRowsInScan(AsyncKuduScanner scanner) throws Exception {
    return countRowsInScan(scanner, 60000);
  }

  public static int countRowsInScan(KuduScanner scanner) throws KuduException {
    int counter = 0;
    while (scanner.hasMoreRows()) {
      counter += scanner.nextRows().getNumRows();
    }
    return counter;
  }

  /**
   * Scans the table and returns the number of rows.
   * @param table the table
   * @param predicates optional predicates to apply to the scan
   * @return the number of rows in the table matching the predicates
   */
  public static long countRowsInTable(KuduTable table, KuduPredicate... predicates) throws KuduException {
    KuduScanner.KuduScannerBuilder scanBuilder =
        table.getAsyncClient().syncClient().newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }
    scanBuilder.setProjectedColumnIndexes(ImmutableList.<Integer>of());
    return countRowsInScan(scanBuilder.build());
  }

  public static List<String> scanTableToStrings(KuduTable table,
                                                KuduPredicate... predicates) throws Exception {
    List<String> rowStrings = Lists.newArrayList();
    KuduScanner.KuduScannerBuilder scanBuilder =
        table.getAsyncClient().syncClient().newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }
    KuduScanner scanner = scanBuilder.build();
    while (scanner.hasMoreRows()) {
      RowResultIterator rows = scanner.nextRows();
      for (RowResult r : rows) {
        rowStrings.add(r.rowToString());
      }
    }
    Collections.sort(rowStrings);
    return rowStrings;
  }

  public static Schema getSchemaWithAllTypes() {
    List<ColumnSchema> columns =
        ImmutableList.of(
            new ColumnSchema.ColumnSchemaBuilder("int8", Type.INT8).key(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("int16", Type.INT16).build(),
            new ColumnSchema.ColumnSchemaBuilder("int32", Type.INT32).build(),
            new ColumnSchema.ColumnSchemaBuilder("int64", Type.INT64).build(),
            new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build(),
            new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build(),
            new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build(),
            new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary-array", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("binary-bytebuffer", Type.BINARY).build(),
            new ColumnSchema.ColumnSchemaBuilder("null", Type.STRING).nullable(true).build(),
            new ColumnSchema.ColumnSchemaBuilder("timestamp", Type.UNIXTIME_MICROS).build(),
            new ColumnSchema.ColumnSchemaBuilder("decimal", Type.DECIMAL)
                .typeAttributes(DecimalUtil.typeAttributes(5, 3)).build());

    return new Schema(columns);
  }

  public static CreateTableOptions getAllTypesCreateTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("int8"));
  }

  public static Schema getBasicSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column1_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column2_i", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column3_s", Type.STRING)
                    .nullable(true)
                    .desiredBlockSize(4096)
                    .encoding(ColumnSchema.Encoding.DICT_ENCODING)
                    .compressionAlgorithm(ColumnSchema.CompressionAlgorithm.LZ4)
                    .build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("column4_b", Type.BOOL).build());
    return new Schema(columns);
  }

  public static CreateTableOptions getBasicCreateTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"));
  }

  /**
   * Creates table options with non-covering range partitioning for a table with
   * the basic schema. Range partition key ranges fall between the following values:
   *
   * [  0,  50)
   * [ 50, 100)
   * [200, 300)
   */
  public static CreateTableOptions getBasicTableOptionsWithNonCoveredRange() {
    Schema schema = getBasicSchema();
    CreateTableOptions option = new CreateTableOptions();
    option.setRangePartitionColumns(ImmutableList.of("key"));

    PartialRow aLowerBound = schema.newPartialRow();
    aLowerBound.addInt("key", 0);
    PartialRow aUpperBound = schema.newPartialRow();
    aUpperBound.addInt("key", 100);
    option.addRangePartition(aLowerBound, aUpperBound);

    PartialRow bLowerBound = schema.newPartialRow();
    bLowerBound.addInt("key", 200);
    PartialRow bUpperBound = schema.newPartialRow();
    bUpperBound.addInt("key", 300);
    option.addRangePartition(bLowerBound, bUpperBound);

    PartialRow split = schema.newPartialRow();
    split.addInt("key", 50);
    option.addSplitRow(split);
    return option;
  }

  public static Insert createBasicSchemaInsert(KuduTable table, int key) {
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addInt(0, key);
    row.addInt(1, 2);
    row.addInt(2, 3);
    row.addString(3, "a string");
    row.addBoolean(4, true);
    return insert;
  }

  public static KuduTable createFourTabletsTableWithNineRows(AsyncKuduClient client,
                                                             String tableName,
                                                             final long timeoutMs)
      throws Exception {
    final int[] KEYS = new int[] { 10, 20, 30 };
    final Schema basicSchema = getBasicSchema();
    CreateTableOptions builder = getBasicCreateTableOptions();
    for (int i : KEYS) {
      PartialRow splitRow = basicSchema.newPartialRow();
      splitRow.addInt(0, i);
      builder.addSplitRow(splitRow);
    }
    KuduTable table = client.syncClient().createTable(tableName, basicSchema, builder);
    AsyncKuduSession session = client.newSession();

    // create a table with on empty tablet and 3 tablets of 3 rows each
    for (int key1 : KEYS) {
      for (int key2 = 1; key2 <= 3; key2++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, key1 + key2);
        row.addInt(1, key1);
        row.addInt(2, key2);
        row.addString(3, "a string");
        row.addBoolean(4, true);
        session.apply(insert).join(timeoutMs);
      }
    }
    session.close().join(timeoutMs);
    return table;
  }

  public static Schema createManyStringsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithBinaryColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.BINARY).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.BINARY).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithTimestampColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.UNIXTIME_MICROS).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.UNIXTIME_MICROS).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithDecimalColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.DECIMAL).key(true)
        .typeAttributes(
            new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                .precision(DecimalUtil.MAX_DECIMAL64_PRECISION).build()
        ).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.DECIMAL).nullable(true)
        .typeAttributes(
            new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                .precision(DecimalUtil.MAX_DECIMAL128_PRECISION).build()
        ).build());
    return new Schema(columns);
  }
}
