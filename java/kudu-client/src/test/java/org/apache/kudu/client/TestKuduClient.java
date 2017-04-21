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
package org.apache.kudu.client;

import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL;
import static org.apache.kudu.client.RowResult.timestampToString;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.util.CapturingLogAppender;
import org.apache.log4j.AppenderSkeleton;

public class TestKuduClient extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClient.class);
  private String tableName;

  @Before
  public void setTableName() {
    tableName = TestKuduClient.class.getName() + "-" + System.currentTimeMillis();
  }

  private Schema createManyStringsSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>(4);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING).nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING).nullable(true).build());
    return new Schema(columns);
  }

  private Schema createSchemaWithBinaryColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.BINARY).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.BINARY).nullable(true).build());
    return new Schema(columns);
  }

  private Schema createSchemaWithTimestampColumns() {
    ArrayList<ColumnSchema> columns = new ArrayList<ColumnSchema>();
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.UNIXTIME_MICROS).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.UNIXTIME_MICROS).nullable(true).build());
    return new Schema(columns);
  }

  private static CreateTableOptions createTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"));
  }

  /**
   * Test setting and reading the most recent propagated timestamp.
   */
  @Test(timeout = 100000)
  public void testLastPropagatedTimestamps() throws Exception {
    long initial_ts = syncClient.getLastPropagatedTimestamp();

    // Check that the initial timestamp is consistent with the asynchronous client.
    assertEquals(initial_ts, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts, syncClient.getLastPropagatedTimestamp());

    // Attempt to change the timestamp to a lower value. This should not change
    // the internal timestamp, as it must be monotonically increasing.
    syncClient.updateLastPropagatedTimestamp(initial_ts - 1);
    assertEquals(initial_ts, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts, syncClient.getLastPropagatedTimestamp());

    // Use the synchronous client to update the last propagated timestamp and
    // check with both clients that the timestamp was updated.
    syncClient.updateLastPropagatedTimestamp(initial_ts + 1);
    assertEquals(initial_ts + 1, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts + 1, syncClient.getLastPropagatedTimestamp());
  }

  /**
   * Test creating and deleting a table through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testCreateDeleteTable() throws Exception {
    // Check that we can create a table.
    syncClient.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    assertFalse(syncClient.getTablesList().getTablesList().isEmpty());
    assertTrue(syncClient.getTablesList().getTablesList().contains(tableName));

    // Check that we can delete it.
    syncClient.deleteTable(tableName);
    assertFalse(syncClient.getTablesList().getTablesList().contains(tableName));

    // Check that we can re-recreate it, with a different schema.
    List<ColumnSchema> columns = new ArrayList<>(basicSchema.getColumns());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("one more", Type.STRING).build());
    Schema newSchema = new Schema(columns);
    syncClient.createTable(tableName, newSchema, getBasicCreateTableOptions());

    // Check that we can open a table and see that it has the new schema.
    KuduTable table = syncClient.openTable(tableName);
    assertEquals(newSchema.getColumnCount(), table.getSchema().getColumnCount());
    assertTrue(table.getPartitionSchema().isSimpleRangePartitioning());

    // Check that the block size parameter we specified in the schema is respected.
    assertEquals(4096, newSchema.getColumn("column3_s").getDesiredBlockSize());
    assertEquals(ColumnSchema.Encoding.DICT_ENCODING,
                 newSchema.getColumn("column3_s").getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4,
                 newSchema.getColumn("column3_s").getCompressionAlgorithm());
  }


  /**
   * Test creating a table with various invalid schema cases.
   */
  @Test(timeout = 100000)
  public void testCreateTableTooManyColumns() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
             .key(true)
             .build());
    for (int i = 0; i < 1000; i++) {
      // not null with default
      cols.add(new ColumnSchema.ColumnSchemaBuilder("c" + i, Type.STRING)
               .build());
    }
    Schema schema = new Schema(cols);
    try {
      syncClient.createTable(tableName, schema, getBasicCreateTableOptions());
    } catch (NonRecoverableException nre) {
      assertThat(nre.toString(), containsString(
          "number of columns 1001 is greater than the permitted maximum"));
    }
  }


  /**
   * Test creating a table with columns with different combinations of NOT NULL and
   * default values, inserting rows, and checking the results are as expected.
   * Regression test for KUDU-180.
   */
  @Test(timeout = 100000)
  public void testTableWithDefaults() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
             .key(true)
             .build());
    // nullable with no default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.STRING)
             .nullable(true)
             .build());
    // nullable with default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c2", Type.STRING)
             .nullable(true)
             .defaultValue("def")
             .build());
    // not null with no default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c3", Type.STRING)
             .nullable(false)
             .build());
    // not null with default
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c4", Type.STRING)
             .nullable(false)
             .defaultValue("def")
             .build());
    Schema schema = new Schema(cols);
    syncClient.createTable(tableName, schema, getBasicCreateTableOptions());
    KuduSession session = syncClient.newSession();
    KuduTable table = syncClient.openTable(tableName);

    // Insert various rows. '-' indicates leaving the row unset in the insert.
    List<String> rows = ImmutableList.of(
        // Specify all columns
        "r1,a,b,c,d",
        // Specify all, set nullable ones to NULL.
        "r2,NULL,NULL,c,d",
        // Don't specify any columns except for the one that is NOT NULL
        // with no default.
        "r3,-,-,c,-",
        // Two rows which should not succeed.
        "fail_1,a,b,c,NULL",
        "fail_2,a,b,NULL,d");
    List<String> expectedStrings = ImmutableList.of(
        "STRING key=r1, STRING c1=a, STRING c2=b, STRING c3=c, STRING c4=d",
        "STRING key=r2, STRING c1=NULL, STRING c2=NULL, STRING c3=c, STRING c4=d",
        "STRING key=r3, STRING c1=NULL, STRING c2=def, STRING c3=c, STRING c4=def");
    for (String row : rows) {
      try {
        String[] fields = row.split(",");
        Insert insert = table.newInsert();
        for (int i = 0; i < fields.length; i++) {
          if (fields[i].equals("-")) { // leave unset
            continue;
          }
          if (fields[i].equals("NULL")) {
            insert.getRow().setNull(i);
          } else {
            insert.getRow().addString(i, fields[i]);
          }
        }
        session.apply(insert);
      } catch (IllegalArgumentException e) {
        // We expect two of the inserts to fail when we try to set NULL values for
        // nullable columns.
        assertTrue(e.getMessage(),
                   e.getMessage().matches("c[34] cannot be set to null"));
      }
    }
    session.flush();

    // Check that we got the results we expected.
    List<String> rowStrings = scanTableToStrings(table);
    Collections.sort(rowStrings);
    assertArrayEquals(rowStrings.toArray(new String[0]),
                      expectedStrings.toArray(new String[0]));
  }

  /**
   * Test inserting and retrieving string columns.
   */
  @Test(timeout = 100000)
  public void testStrings() throws Exception {
    Schema schema = createManyStringsSchema();
    syncClient.createTable(tableName, schema, createTableOptions());

    KuduSession session = syncClient.newSession();
    KuduTable table = syncClient.openTable(tableName);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addString("c3", "c3_" + i);
      }
      row.addString("c4", "c4_" + i);
      // NOTE: we purposefully add the strings in a non-left-to-right
      // order to verify that we still place them in the right position in
      // the row.
      row.addString("c1", "c1_" + i);
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "STRING key=key_03, STRING c1=c1_3, STRING c2=c2_3, STRING c3=c3_3, STRING c4=c4_3",
        rowStrings.get(3));
    assertEquals(
        "STRING key=key_04, STRING c1=c1_4, STRING c2=c2_4, STRING c3=NULL, STRING c4=c4_4",
        rowStrings.get(4));

    KuduScanner scanner = syncClient.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    // Do negative testing on string type.
    try {
      next.getInt("c2");
      fail("IllegalArgumentException was not thrown when accessing " +
              "a string column with getInt");
    } catch (IllegalArgumentException ignored) {}
  }

  /**
   * Test to verify that we can write in and read back UTF8.
   */
  @Test(timeout = 100000)
  public void testUTF8() throws Exception {
    Schema schema = createManyStringsSchema();
    syncClient.createTable(tableName, schema, createTableOptions());

    KuduSession session = syncClient.newSession();
    KuduTable table = syncClient.openTable(tableName);
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "กขฃคฅฆง"); // some thai
    row.addString("c1", "✁✂✃✄✆"); // some icons

    row.addString("c2", "hello"); // some normal chars
    row.addString("c4", "🐱"); // supplemental plane
    session.apply(insert);
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(1, rowStrings.size());
    assertEquals(
        "STRING key=กขฃคฅฆง, STRING c1=✁✂✃✄✆, STRING c2=hello, STRING c3=NULL, STRING c4=🐱",
        rowStrings.get(0));
  }

  /**
   * Test inserting and retrieving binary columns.
   */
  @Test(timeout = 100000)
  public void testBinaryColumns() throws Exception {
    Schema schema = createSchemaWithBinaryColumns();
    syncClient.createTable(tableName, schema, createTableOptions());

    byte[] testArray = new byte[] {1, 2, 3, 4, 5, 6 ,7, 8, 9};

    KuduSession session = syncClient.newSession();
    KuduTable table = syncClient.openTable(tableName);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addBinary("key", String.format("key_%02d", i).getBytes());
      row.addString("c1", "✁✂✃✄✆");
      row.addDouble("c2", i);
      if (i % 2 == 1) {
        row.addBinary("c3", testArray);
      }
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("BINARY key=\"key_%02d\", STRING c1=✁✂✃✄✆, DOUBLE c2=%.1f,"
          + " BINARY c3=", i, (double) i));
      if (i % 2 == 1) {
        expectedRow.append(Bytes.pretty(testArray));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test inserting and retrieving timestamp columns.
   */
  @Test(timeout = 100000)
  public void testTimestampColumns() throws Exception {
    Schema schema = createSchemaWithTimestampColumns();
    syncClient.createTable(tableName, schema, createTableOptions());

    List<Long> timestamps = new ArrayList<>();

    KuduSession session = syncClient.newSession();
    KuduTable table = syncClient.openTable(tableName);
    long lastTimestamp = 0;
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      long timestamp = System.currentTimeMillis() * 1000;
      while(timestamp == lastTimestamp) {
        timestamp = System.currentTimeMillis() * 1000;
      }
      timestamps.add(timestamp);
      row.addLong("key", timestamp);
      if (i % 2 == 1) {
        row.addLong("c1", timestamp);
      }
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
      lastTimestamp = timestamp;
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("UNIXTIME_MICROS key=%s, UNIXTIME_MICROS c1=",
          timestampToString(timestamps.get(i))));
      if (i % 2 == 1) {
        expectedRow.append(timestampToString(timestamps.get(i)));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test scanning with predicates.
   */
  @Test
  public void testScanWithPredicates() throws Exception {
    Schema schema = createManyStringsSchema();
    syncClient.createTable(tableName, schema, createTableOptions());

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = syncClient.openTable(tableName);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", String.format("key_%02d", i));
      row.addString("c1", "c1_" + i);
      row.addString("c2", "c2_" + i);
      if (i % 2 == 0) {
        row.addString("c3", "c3_" + i);
      }
      session.apply(insert);
    }
    session.flush();

    assertEquals(100, scanTableToStrings(table).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, "key_50")
    ).size());
    assertEquals(25, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_74")
    ).size());
    assertEquals(25, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_24"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c1"), LESS_EQUAL, "c1_49")
    ).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER, "key_24"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("key"), GREATER_EQUAL, "key_50")
    ).size());
    assertEquals(0, scanTableToStrings(table,
        KuduPredicate.newComparisonPredicate(schema.getColumn("c1"), GREATER, "c1_30"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"), LESS, "c2_20")
    ).size());
    assertEquals(0, scanTableToStrings(table,
        // Short circuit scan
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"), GREATER, "c2_30"),
        KuduPredicate.newComparisonPredicate(schema.getColumn("c2"), LESS, "c2_20")
    ).size());

    // IS NOT NULL
    assertEquals(100, scanTableToStrings(table,
       KuduPredicate.newIsNotNullPredicate(schema.getColumn("c1")),
       KuduPredicate.newIsNotNullPredicate(schema.getColumn("key"))
    ).size());
    assertEquals(50, scanTableToStrings(table,
        KuduPredicate.newIsNotNullPredicate(schema.getColumn("c3"))
    ).size());

    // IS NULL
    assertEquals(0, scanTableToStrings(table,
            KuduPredicate.newIsNullPredicate(schema.getColumn("c2")),
            KuduPredicate.newIsNullPredicate(schema.getColumn("key"))
    ).size());
    assertEquals(50, scanTableToStrings(table,
            KuduPredicate.newIsNullPredicate(schema.getColumn("c3"))
    ).size());

    // IN list
    assertEquals(3, scanTableToStrings(table,
       KuduPredicate.newInListPredicate(schema.getColumn("key"),
                                        ImmutableList.of("key_30", "key_01", "invalid", "key_99"))
    ).size());
    assertEquals(3, scanTableToStrings(table,
       KuduPredicate.newInListPredicate(schema.getColumn("c2"),
                                        ImmutableList.of("c2_30", "c2_1", "invalid", "c2_99"))
    ).size());
    assertEquals(2, scanTableToStrings(table,
       KuduPredicate.newInListPredicate(schema.getColumn("c2"),
                                        ImmutableList.of("c2_30", "c2_1", "invalid", "c2_99")),
       KuduPredicate.newIsNotNullPredicate(schema.getColumn("c2")),
       KuduPredicate.newInListPredicate(schema.getColumn("key"),
                                        ImmutableList.of("key_30", "key_45", "invalid", "key_99"))
    ).size());
  }

  /**
   * Counts the rows in the provided scan tokens.
   */
  private int countScanTokenRows(List<KuduScanToken> tokens) throws Exception {
    final AtomicInteger count = new AtomicInteger(0);
    List<Thread> threads = new ArrayList<>();
    for (final KuduScanToken token : tokens) {
      final byte[] serializedToken = token.serialize();
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try (KuduClient contextClient = new KuduClient.KuduClientBuilder(masterAddresses)
              .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
              .build()) {
            KuduScanner scanner = KuduScanToken.deserializeIntoScanner(serializedToken, contextClient);
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
      thread.run();
      threads.add(thread);
    }

    for (Thread thread : threads) {
      thread.join();
    }
    return count.get();
  }

  @Test
  public void testGetAuthnToken() throws Exception {
    byte[] token = client.exportAuthenticationCredentials().join();
    assertNotNull(token);
  }

  /**
   * Tests scan tokens by creating a set of scan tokens, serializing them, and
   * then executing them in parallel with separate client instances. This
   * simulates the normal usecase of scan tokens being created at a central
   * planner and distributed to remote task executors.
   */
  @Test
  public void testScanTokens() throws Exception {
    int saveFetchTablets = AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP;
    try {
      // For this test, make sure that we cover the case that not all tablets
      // are returned in a single batch.
      AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP = 4;

      Schema schema = createManyStringsSchema();
      CreateTableOptions createOptions = new CreateTableOptions();
      createOptions.addHashPartitions(ImmutableList.of("key"), 8);

      PartialRow splitRow = schema.newPartialRow();
      splitRow.addString("key", "key_50");
      createOptions.addSplitRow(splitRow);

      syncClient.createTable(tableName, schema, createOptions);

      KuduSession session = syncClient.newSession();
      session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
      KuduTable table = syncClient.openTable(tableName);
      for (int i = 0; i < 100; i++) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addString("key", String.format("key_%02d", i));
        row.addString("c1", "c1_" + i);
        row.addString("c2", "c2_" + i);
        session.apply(insert);
      }
      session.flush();

      KuduScanToken.KuduScanTokenBuilder tokenBuilder = syncClient.newScanTokenBuilder(table);
      tokenBuilder.setProjectedColumnIndexes(ImmutableList.<Integer>of());
      List<KuduScanToken> tokens = tokenBuilder.build();
      assertEquals(16, tokens.size());

      for (KuduScanToken token : tokens) {
        // Sanity check to make sure the debug printing does not throw.
        LOG.debug(KuduScanToken.stringifySerializedToken(token.serialize(), syncClient));
      }
    } finally {
      AsyncKuduClient.FETCH_TABLETS_PER_RANGE_LOOKUP = saveFetchTablets;
    }
  }

  /**
   * Tests scan token creation and execution on a table with non-covering range partitions.
   */
  @Test
  public void testScanTokensNonCoveringRangePartitions() throws Exception {
    Schema schema = createManyStringsSchema();
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.addHashPartitions(ImmutableList.of("key"), 2);

    PartialRow lower = schema.newPartialRow();
    PartialRow upper = schema.newPartialRow();
    lower.addString("key", "a");
    upper.addString("key", "f");
    createOptions.addRangePartition(lower, upper);

    lower = schema.newPartialRow();
    upper = schema.newPartialRow();
    lower.addString("key", "h");
    upper.addString("key", "z");
    createOptions.addRangePartition(lower, upper);

    PartialRow split = schema.newPartialRow();
    split.addString("key", "k");
    createOptions.addSplitRow(split);

    syncClient.createTable(tableName, schema, createOptions);

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = syncClient.openTable(tableName);
    for (char c = 'a'; c < 'f'; c++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", "" + c);
      row.addString("c1", "c1_" + c);
      row.addString("c2", "c2_" + c);
      session.apply(insert);
    }
    for (char c = 'h'; c < 'z'; c++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addString("key", "" + c);
      row.addString("c1", "c1_" + c);
      row.addString("c2", "c2_" + c);
      session.apply(insert);
    }
    session.flush();

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = syncClient.newScanTokenBuilder(table);
    tokenBuilder.setProjectedColumnIndexes(ImmutableList.<Integer>of());
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(6, tokens.size());
    assertEquals('f' - 'a' + 'z' - 'h', countScanTokenRows(tokens));

    for (KuduScanToken token : tokens) {
      // Sanity check to make sure the debug printing does not throw.
      LOG.debug(KuduScanToken.stringifySerializedToken(token.serialize(), syncClient));
    }
  }

  /**
   * Tests the results of creating scan tokens, altering the columns being
   * scanned, and then executing the scan tokens.
   */
  @Test
  public void testScanTokensConcurrentAlterTable() throws Exception {
    Schema schema = new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).nullable(false).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64).nullable(false).key(false).build()
    ));
    CreateTableOptions createOptions = new CreateTableOptions();
    createOptions.setRangePartitionColumns(ImmutableList.<String>of());
    createOptions.setNumReplicas(1);
    syncClient.createTable(tableName, schema, createOptions);

    KuduTable table = syncClient.openTable(tableName);

    KuduScanToken.KuduScanTokenBuilder tokenBuilder = syncClient.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());
    KuduScanToken token = tokens.get(0);

    // Drop a column
    syncClient.alterTable(tableName, new AlterTableOptions().dropColumn("a"));
    assertTrue(syncClient.isAlterTableDone(tableName));
    try {
      token.intoScanner(syncClient);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unknown column"));
    }

    // Add back the column with the wrong type.
    syncClient.alterTable(
        tableName,
        new AlterTableOptions().addColumn(
            new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).nullable(true).build()));
    assertTrue(syncClient.isAlterTableDone(tableName));
    try {
      token.intoScanner(syncClient);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          "invalid type INT64 for column 'a' in scan token, expected: STRING"));
    }

    // Add the column with the wrong nullability.
    syncClient.alterTable(
        tableName,
        new AlterTableOptions().dropColumn("a")
                               .addColumn(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64)
                                                          .nullable(true).build()));
    assertTrue(syncClient.isAlterTableDone(tableName));
    try {
      token.intoScanner(syncClient);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(
          "invalid nullability for column 'a' in scan token, expected: NOT NULL"));
    }

    // Add the column with the correct type and nullability.
    syncClient.alterTable(
        tableName,
        new AlterTableOptions().dropColumn("a")
                               .addColumn(new ColumnSchema.ColumnSchemaBuilder("a", Type.INT64)
                                                          .nullable(false)
                                                          .defaultValue(0L).build()));
    assertTrue(syncClient.isAlterTableDone(tableName));
    token.intoScanner(syncClient);
  }

  /**
   * Counts the rows in a table between two optional bounds.
   * @param table the table to scan, must have the basic schema
   * @param lowerBound an optional lower bound key
   * @param upperBound an optional upper bound key
   * @return the row count
   * @throws Exception on error
   */
  private int countRowsForTestScanNonCoveredTable(KuduTable table,
                                                  Integer lowerBound,
                                                  Integer upperBound) throws Exception {

    KuduScanner.KuduScannerBuilder scanBuilder = syncClient.newScannerBuilder(table);
    if (lowerBound != null) {
      PartialRow bound = basicSchema.newPartialRow();
      bound.addInt(0, lowerBound);
      scanBuilder.lowerBound(bound);
    }
    if (upperBound != null) {
      PartialRow bound = basicSchema.newPartialRow();
      bound.addInt(0, upperBound);
      scanBuilder.exclusiveUpperBound(bound);
    }

    KuduScanner scanner = scanBuilder.build();
    int count = 0;
    while (scanner.hasMoreRows()) {
      count += scanner.nextRows().getNumRows();
    }
    return count;
  }

  /**
   * Tests scanning a table with non-covering range partitions.
   */
  @Test(timeout = 100000)
  public void testScanNonCoveredTable() throws Exception {

    Schema schema = basicSchema;
    syncClient.createTable(tableName, schema, getBasicTableOptionsWithNonCoveredRange());

    KuduSession session = syncClient.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = syncClient.openTable(tableName);

    for (int key = 0; key < 100; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }
    for (int key = 200; key < 300; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }
    session.flush();
    assertEquals(0, session.countPendingErrors());

    assertEquals(200, countRowsForTestScanNonCoveredTable(table, null, null));
    assertEquals(100, countRowsForTestScanNonCoveredTable(table, null, 200));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, null, -1));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, 120, 180));
    assertEquals(0, countRowsForTestScanNonCoveredTable(table, 300, null));
  }

  /**
   * Creates a local client that we auto-close while buffering one row, then makes sure that after
   * closing that we can read the row.
   */
  @Test(timeout = 100000)
  public void testAutoClose() throws Exception {
    try (KuduClient localClient = new KuduClient.KuduClientBuilder(masterAddresses).build()) {
      localClient.createTable(tableName, basicSchema, getBasicCreateTableOptions());
      KuduTable table = localClient.openTable(tableName);
      KuduSession session = localClient.newSession();

      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      Insert insert = createBasicSchemaInsert(table, 0);
      session.apply(insert);
    }

    KuduTable table = syncClient.openTable(tableName);
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(client, table).build();
    assertEquals(1, countRowsInScan(scanner));
  }

  /**
   * Regression test for some log spew which occurred in short-lived client instances which
   * had outbound connections.
   */
  @Test(timeout = 100000)
  public void testCloseShortlyAfterOpen() throws Exception {
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      try (KuduClient localClient = new KuduClient.KuduClientBuilder(masterAddresses).build()) {
        // Force the client to connect to the masters.
        localClient.exportAuthenticationCredentials();
      }
      // Wait a little bit since the "channel disconnected" exceptions could come
      // from threads that don't get synchronously joined by client.close().
      Thread.sleep(500);
    }
    assertFalse(cla.getAppendedText(), cla.getAppendedText().contains("Exception"));
  }

  @Test(timeout = 100000)
  public void testCustomNioExecutor() throws Exception {
    long startTime = System.nanoTime();
    final KuduClient localClient = new KuduClient.KuduClientBuilder(masterAddresses)
        .nioExecutors(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(2))
        .bossCount(1)
        .workerCount(2)
        .build();
    long buildTime = (System.nanoTime() - startTime) / 1000000000L;
    assertTrue("Building KuduClient is slow, maybe netty get stuck", buildTime < 3);
    localClient.createTable(tableName, basicSchema, getBasicCreateTableOptions());
    Thread[] threads = new Thread[4];
    for (int t = 0; t < 4; t++) {
      final int id = t;
      threads[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            KuduTable table = localClient.openTable(tableName);
            KuduSession session = localClient.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
            for (int i = 0; i < 100; i++) {
              Insert insert = createBasicSchemaInsert(table, id * 100 + i);
              session.apply(insert);
            }
            session.close();
          } catch (Exception e) {
            fail("insert thread should not throw exception: " + e);
          }
        }
      });
      threads[t].start();
    }
    for (int t = 0; t< 4;t++) {
      threads[t].join();
    }
    localClient.shutdown();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNoDefaultPartitioning() throws Exception {
    syncClient.createTable(tableName, basicSchema, new CreateTableOptions());
  }

  @Test(timeout = 100000)
  public void testOpenTableClearsNonCoveringRangePartitions() throws KuduException {
    CreateTableOptions options = createTableOptions();
    PartialRow lower = basicSchema.newPartialRow();
    PartialRow upper = basicSchema.newPartialRow();
    lower.addInt("key", 0);
    upper.addInt("key", 1);
    options.addRangePartition(lower, upper);

    syncClient.createTable(tableName, basicSchema, options);
    KuduTable table = syncClient.openTable(tableName);

    // Count the number of tablets.
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = syncClient.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Add a range partition with a separate client. The new client is necessary
    // in order to avoid clearing the meta cache as part of the alter operation.
    try (KuduClient alterClient = new KuduClient.KuduClientBuilder(masterAddresses)
                                                .defaultAdminOperationTimeoutMs(DEFAULT_SLEEP)
                                                .build()) {
      AlterTableOptions alter = new AlterTableOptions();
      lower = basicSchema.newPartialRow();
      upper = basicSchema.newPartialRow();
      lower.addInt("key", 1);
      alter.addRangePartition(lower, upper);
      alterClient.alterTable(tableName, alter);
      assertTrue(syncClient.isAlterTableDone(tableName));
    }

    // Count the number of tablets.  The result should still be the same, since
    // the new tablet is still cached as a non-covered range.
    tokenBuilder = syncClient.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Reopen the table and count the tablets again. The new tablet should now show up.
    table = syncClient.openTable(tableName);
    tokenBuilder = syncClient.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(2, tokens.size());
  }
}
