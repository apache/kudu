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

package org.apache.kudu.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.util.CharUtil;
import org.apache.kudu.util.DecimalUtil;

/**
 * Utilities useful for cluster testing.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
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
        if (arg == null) {
          return null;
        }
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
  public static long countRowsInTable(KuduTable table, KuduPredicate... predicates)
      throws KuduException {
    KuduScanner.KuduScannerBuilder scanBuilder =
        table.getAsyncClient().syncClient().newScannerBuilder(table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }
    scanBuilder.setProjectedColumnIndexes(ImmutableList.of());
    return countRowsInScan(scanBuilder.build());
  }

  /**
   * Counts the rows in the provided scan tokens.
   */
  public static int countScanTokenRows(List<KuduScanToken> tokens, final String masterAddresses,
                                       final long operationTimeoutMs)
      throws IOException, InterruptedException {
    final AtomicInteger count = new AtomicInteger(0);
    List<Thread> threads = new ArrayList<>();
    for (final KuduScanToken token : tokens) {
      final byte[] serializedToken = token.serialize();
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try (KuduClient contextClient = new KuduClient.KuduClientBuilder(masterAddresses)
                   .defaultAdminOperationTimeoutMs(operationTimeoutMs)
                   .build()) {
            KuduScanner scanner =
                KuduScanToken.deserializeIntoScanner(serializedToken, contextClient);
            try {
              int localCount = 0;
              while (scanner.hasMoreRows()) {
                localCount += Iterators.size(scanner.nextRows());
              }
              count.addAndGet(localCount);
            } finally {
              scanner.close();
            }
          } catch (Exception e) {
            LOG.error("exception in parallel token scanner", e);
          }
        }
      });
      thread.start();
      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }
    return count.get();
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
    for (RowResult r : scanner) {
      rowStrings.add(r.rowToString());
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
              .typeAttributes(DecimalUtil.typeAttributes(5, 3)).build(),
            new ColumnSchema.ColumnSchemaBuilder("varchar", Type.VARCHAR)
              .typeAttributes(CharUtil.typeAttributes(10)).build());

    return new Schema(columns);
  }

  public static PartialRow getPartialRowWithAllTypes() {
    Schema schema = getSchemaWithAllTypes();
    // Ensure we aren't missing any types
    assertEquals(14, schema.getColumnCount());

    PartialRow row = schema.newPartialRow();
    row.addByte("int8", (byte) 42);
    row.addShort("int16", (short) 43);
    row.addInt("int32", 44);
    row.addLong("int64", 45);
    row.addTimestamp("timestamp", new Timestamp(1234567890));
    row.addBoolean("bool", true);
    row.addFloat("float", 52.35F);
    row.addDouble("double", 53.35);
    row.addString("string", "fun with ütf\0");
    row.addVarchar("varchar", "árvíztűrő tükörfúrógép");
    row.addBinary("binary-array", new byte[] { 0, 1, 2, 3, 4 });
    ByteBuffer binaryBuffer = ByteBuffer.wrap(new byte[] { 5, 6, 7, 8, 9 });
    row.addBinary("binary-bytebuffer", binaryBuffer);
    row.setNull("null");
    row.addDecimal("decimal", BigDecimal.valueOf(12345, 3));
    return row;
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

    PartialRow lowerBoundA = schema.newPartialRow();
    lowerBoundA.addInt("key", 0);
    PartialRow upperBoundA = schema.newPartialRow();
    upperBoundA.addInt("key", 100);
    option.addRangePartition(lowerBoundA, upperBoundA);

    PartialRow lowerBoundB = schema.newPartialRow();
    lowerBoundB.addInt("key", 200);
    PartialRow upperBoundB = schema.newPartialRow();
    upperBoundB.addInt("key", 300);
    option.addRangePartition(lowerBoundB, upperBoundB);

    PartialRow split = schema.newPartialRow();
    split.addInt("key", 50);
    option.addSplitRow(split);
    return option;
  }

  /**
   * A generic helper function to create a table with default test options.
   */
  public static KuduTable createDefaultTable(KuduClient client, String tableName)
      throws KuduException {
    return client.createTable(tableName, getBasicSchema(), getBasicCreateTableOptions());
  }

  /**
   * Load a table of default schema with the specified number of records, in ascending key order.
   */
  public static void loadDefaultTable(KuduClient client, String tableName, int numRows)
      throws KuduException {
    KuduTable table = client.openTable(tableName);
    KuduSession session = client.newSession();
    for (int i = 0; i < numRows; i++) {
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
    }
    session.flush();
    session.close();
  }

  public static Upsert createBasicSchemaUpsert(KuduTable table, int key) {
    Upsert upsert = table.newUpsert();
    PartialRow row = upsert.getRow();
    row.addInt(0, key);
    row.addInt(1, 3);
    row.addInt(2, 4);
    row.addString(3, "another string");
    row.addBoolean(4, false);
    return upsert;
  }

  public static Upsert createBasicSchemaUpsertWithDataSize(KuduTable table, int key, int dataSize) {
    Upsert upsert = table.newUpsert();
    PartialRow row = upsert.getRow();
    row.addInt(0, key);
    row.addInt(1, 3);
    row.addInt(2, 4);

    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < dataSize; i++) {
      builder.append("*");
    }
    String val = builder.toString();
    row.addString(3, val);
    row.addBoolean(4, false);
    return upsert;
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

  public static KuduTable createTableWithOneThousandRows(AsyncKuduClient client,
                                                         String tableName,
                                                         final int rowDataSize,
                                                         final long timeoutMs)
      throws Exception {
    final int[] KEYS = new int[] { 250, 500, 750 };
    final Schema basicSchema = getBasicSchema();
    CreateTableOptions builder = getBasicCreateTableOptions();
    for (int i : KEYS) {
      PartialRow splitRow = basicSchema.newPartialRow();
      splitRow.addInt(0, i);
      builder.addSplitRow(splitRow);
    }
    KuduTable table = client.syncClient().createTable(tableName, basicSchema, builder);
    AsyncKuduSession session = client.newSession();

    // create a table with on 4 tablets of 250 rows each
    for (int key = 0; key < 1000; key++) {
      Upsert upsert = createBasicSchemaUpsertWithDataSize(table, key, rowDataSize);
      session.apply(upsert).join(timeoutMs);
    }
    session.close().join(timeoutMs);
    return table;
  }

  public static Schema createManyVarcharsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.VARCHAR)
                  .typeAttributes(CharUtil.typeAttributes(10)).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.VARCHAR)
                  .typeAttributes(CharUtil.typeAttributes(10)).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.VARCHAR)
                  .typeAttributes(CharUtil.typeAttributes(10)).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.VARCHAR)
                  .typeAttributes(CharUtil.typeAttributes(10)).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.VARCHAR)
                  .typeAttributes(CharUtil.typeAttributes(10)).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createManyStringsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithBinaryColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.BINARY).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.BINARY).nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithTimestampColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.UNIXTIME_MICROS)
        .key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.UNIXTIME_MICROS)
        .nullable(true).build());
    return new Schema(columns);
  }

  public static Schema createSchemaWithDecimalColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<>();
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
