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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL;
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithBinaryColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDecimalColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithTimestampColumns;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableList;
import com.stumbleupon.async.Deferred;

import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.LocationConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.util.TimestampUtil;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.util.DecimalUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestKuduClient {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClient.class);

  private static final String TABLE_NAME = "TestKuduClient";

  private static final int SHORT_SCANNER_TTL_MS = 5000;
  private static final int SHORT_SCANNER_GC_US = SHORT_SCANNER_TTL_MS * 100; // 10% of the TTL.

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  /**
   * Test setting and reading the most recent propagated timestamp.
   */
  @Test(timeout = 100000)
  public void testLastPropagatedTimestamps() throws Exception {
    // Scan a table to ensure a timestamp is propagated.
    KuduTable table = client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    client.newScannerBuilder(table).build().nextRows().getNumRows();
    assertTrue(client.hasLastPropagatedTimestamp());
    assertTrue(client.hasLastPropagatedTimestamp());
    assertTrue(asyncClient.hasLastPropagatedTimestamp());

    long initial_ts = client.getLastPropagatedTimestamp();

    // Check that the initial timestamp is consistent with the asynchronous client.
    assertEquals(initial_ts, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts, asyncClient.getLastPropagatedTimestamp());

    // Attempt to change the timestamp to a lower value. This should not change
    // the internal timestamp, as it must be monotonically increasing.
    client.updateLastPropagatedTimestamp(initial_ts - 1);
    assertEquals(initial_ts, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts, asyncClient.getLastPropagatedTimestamp());

    // Use the synchronous client to update the last propagated timestamp and
    // check with both clients that the timestamp was updated.
    client.updateLastPropagatedTimestamp(initial_ts + 1);
    assertEquals(initial_ts + 1, client.getLastPropagatedTimestamp());
    assertEquals(initial_ts + 1, asyncClient.getLastPropagatedTimestamp());
  }

  /**
   * Test creating and deleting a table through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testCreateDeleteTable() throws Exception {
    // Check that we can create a table.
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    assertFalse(client.getTablesList().getTablesList().isEmpty());
    assertTrue(client.getTablesList().getTablesList().contains(TABLE_NAME));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME);
    assertFalse(client.getTablesList().getTablesList().contains(TABLE_NAME));

    // Check that we can re-recreate it, with a different schema.
    List<ColumnSchema> columns = new ArrayList<>(basicSchema.getColumns());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("one more", Type.STRING).build());
    Schema newSchema = new Schema(columns);
    client.createTable(TABLE_NAME, newSchema, getBasicCreateTableOptions());

    // Check that we can open a table and see that it has the new schema.
    KuduTable table = client.openTable(TABLE_NAME);
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
      client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
      fail();
    } catch (NonRecoverableException nre) {
      assertThat(nre.toString(), containsString(
          "number of columns 1001 is greater than the permitted maximum"));
    }
  }

  /*
   * Test the scanner behavior when a scanner is used beyond
   * the scanner ttl without calling keepAlive.
   */
  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testScannerExpiration() throws Exception {
    // Create a basic table and load it with data.
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .batchSizeBytes(100) // Use a small batch size so we can call nextRows many times.
        .build();

    // Initialize the scanner and verify we can read rows.
    int rows = scanner.nextRows().getNumRows();
    assertTrue("Scanner did not read any rows", rows > 0);

    // Wait for the scanner to time out.
    Thread.sleep(SHORT_SCANNER_TTL_MS * 2);

    try {
      scanner.nextRows();
      fail("Exception was not thrown when accessing an expired scanner");
    } catch (NonRecoverableException ex) {
      assertTrue("Expected Scanner not found error, got:\n" + ex.toString(),
                 ex.getMessage().matches(".*Scanner .* not found.*"));
    }

    // Closing an expired scanner shouldn't throw an exception.
    scanner.close();
  }

  /*
   * Test keeping a scanner alive beyond scanner ttl.
   */
  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testKeepAlive() throws Exception {
    // Create a basic table and load it with data.
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 2));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
        .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
        .batchSizeBytes(100) // Use a small batch size so we can call nextRows many times.
        .build();

    // KeepAlive on uninitialized scanner should be ok.
    scanner.keepAlive();
    // Get the first batch and initialize the scanner
    int accum = scanner.nextRows().getNumRows();

    while (scanner.hasMoreRows()) {
      int rows = scanner.nextRows().getNumRows();
      accum += rows;
      // Break when we are between tablets.
      if (scanner.currentTablet() == null) {
        LOG.info(String.format("Between tablets after scanning %d rows", accum));
        break;
      }
      // Ensure we actually end up between tablets.
      if (accum == numRows) {
        fail("All rows were in a single tablet.");
      }
    }

    // In between scanners now and should be ok.
    scanner.keepAlive();

    // Initialize the next scanner or keepAlive will have no effect.
    accum += scanner.nextRows().getNumRows();

    // Wait for longer than the scanner ttl calling keepAlive throughout.
    // Each loop sleeps 25% of the scanner ttl and we loop 10 times to ensure
    // we extend over 2x the scanner ttl.
    for (int i = 0; i < 10; i++) {
      Thread.sleep(SHORT_SCANNER_TTL_MS / 4);
      scanner.keepAlive();
    }

    // Finish out the rows.
    while (scanner.hasMoreRows()) {
      accum += scanner.nextRows().getNumRows();
    }
    assertEquals("All rows were not scanned", numRows, accum);

    // At this point the scanner is closed and there is nothing to keep alive.
    try {
      scanner.keepAlive();
      fail("Exception was not thrown when calling keepAlive on a closed scanner");
    } catch (IllegalStateException ex) {
      assertThat(ex.getMessage(), containsString("Scanner has already been closed"));
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
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

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
        String[] fields = row.split(",", -1);
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
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
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

    KuduScanner scanner = client.newScannerBuilder(table).build();

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
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
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
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    byte[] testArray = new byte[] {1, 2, 3, 4, 5, 6 ,7, 8, 9};

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addBinary("key", String.format("key_%02d", i).getBytes(UTF_8));
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
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Long> timestamps = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
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
          TimestampUtil.timestampToString(timestamps.get(i))));
      if (i % 2 == 1) {
        expectedRow.append(TimestampUtil.timestampToString(timestamps.get(i)));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test inserting and retrieving decimal columns.
   */
  @Test(timeout = 100000)
  public void testDecimalColumns() throws Exception {
    Schema schema = createSchemaWithDecimalColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    // Verify ColumnTypeAttributes
    assertEquals(DecimalUtil.MAX_DECIMAL128_PRECISION,
        table.getSchema().getColumn("c1").getTypeAttributes().getPrecision());

    for (int i = 0; i < 9; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addDecimal("key", BigDecimal.valueOf(i));
      if (i % 2 == 1) {
        row.addDecimal("c1", BigDecimal.valueOf(i));
      }
      session.apply(insert);
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(9, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("DECIMAL key(18, 0)=%s, DECIMAL c1(38, 0)=", String.valueOf(i)));
      if (i % 2 == 1) {
        expectedRow.append(String.valueOf(i));
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
  * Test scanning with limits.
  */
  @Test
  public void testScanWithLimit() throws Exception {
    AsyncKuduClient asyncClient = harness.getAsyncClient();
    client.createTable(TABLE_NAME, basicSchema, getBasicTableOptionsWithNonCoveredRange());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();
    int num_rows = 100;
    for (int key = 0; key < num_rows; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }

    // Test with some non-positive limits, expecting to raise an exception.
    int non_positives[] = { -1, 0 };
    for (int limit : non_positives) {
      try {
        KuduScanner scanner = client.newScannerBuilder(table)
                                        .limit(limit)
                                        .build();
        fail();
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("Need a strictly positive number"));
      }
    }

    // Test with a limit and ensure we get the expected number of rows.
    int limits[] = { num_rows - 1, num_rows, num_rows + 1 };
    for (int limit : limits) {
      KuduScanner scanner = client.newScannerBuilder(table)
                                      .limit(limit)
                                      .build();
      int count = 0;
      while (scanner.hasMoreRows()) {
        count += scanner.nextRows().getNumRows();
      }
      assertEquals(String.format("Limit %d returned %d/%d rows", limit, count, num_rows),
          Math.min(num_rows, limit), count);
    }

    // Now test with limits for async scanners.
    for (int limit : limits) {
      AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table)
                                                     .limit(limit)
                                                     .build();
      assertEquals(Math.min(limit, num_rows), countRowsInScan(scanner));
    }
  }

  /**
   * Test scanning with predicates.
   */
  @Test
  public void testScanWithPredicates() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = client.openTable(TABLE_NAME);
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

  @Test
  public void testGetAuthnToken() throws Exception {
    byte[] token = asyncClient.exportAuthenticationCredentials().join();
    assertNotNull(token);
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

    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
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
    client.createTable(TABLE_NAME, basicSchema, getBasicTableOptionsWithNonCoveredRange());

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    KuduTable table = client.openTable(TABLE_NAME);

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
   * Creates a local aclient that we auto-close while buffering one row, then makes sure that after
   * closing that we can read the row.
   */
  @Test(timeout = 100000)
  public void testAutoClose() throws Exception {
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
      localClient.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
      KuduTable table = localClient.openTable(TABLE_NAME);
      KuduSession session = localClient.newSession();

      session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
      Insert insert = createBasicSchemaInsert(table, 0);
      session.apply(insert);
    }

    KuduTable table = client.openTable(TABLE_NAME);
    AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table).build();
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
      try (KuduClient localClient =
               new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
        // Force the client to connect to the masters.
        localClient.exportAuthenticationCredentials();
      }
      // Wait a little for exceptions to come in from threads that don't get
      // synchronously joined by client.close().
      Thread.sleep(500);
    }
    // Ensure there is no log spew due to an unexpected lost connection.
    assertFalse(cla.getAppendedText(), cla.getAppendedText().contains("Exception"));
  }

  /**
   * Test that, if the masters are down when we attempt to connect, we don't end up
   * logging any nonsensical stack traces including Netty internals.
   */
  @Test(timeout = 100000)
  public void testNoLogSpewOnConnectionRefused() throws Exception {
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      harness.killAllMasterServers();
      try (KuduClient localClient =
               new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString()).build()) {
        // Force the client to connect to the masters.
        localClient.exportAuthenticationCredentials();
        fail("Should have failed to connect.");
      } catch (NonRecoverableException e) {
        assertTrue("Bad exception string: " + e.getMessage(),
            e.getMessage().matches(".*Master config .+ has no leader. " +
                "Exceptions received:.*Connection refused.*Connection refused" +
                ".*Connection refused.*"));
      }
    } finally {
      harness.startAllMasterServers();
    }
    // Ensure there is no log spew due to an unexpected lost connection.
    String logText = cla.getAppendedText();
    assertFalse("Should not claim to have lost a connection in the log",
               logText.contains("lost connection to peer"));
    assertFalse("Should not have netty spew in log",
                logText.contains("socket.nio.AbstractNioSelector"));
  }

  @Test(timeout = 100000)
  public void testCustomNioExecutor() throws Exception {
    long startTime = System.nanoTime();
    final KuduClient localClient = new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
        .nioExecutors(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(2))
        .bossCount(1)
        .workerCount(2)
        .build();
    long buildTime = (System.nanoTime() - startTime) / 1000000000L;
    assertTrue("Building KuduClient is slow, maybe netty get stuck", buildTime < 3);
    localClient.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    Thread[] threads = new Thread[4];
    for (int t = 0; t < 4; t++) {
      final int id = t;
      threads[t] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            KuduTable table = localClient.openTable(TABLE_NAME);
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
    client.createTable(TABLE_NAME, basicSchema, new CreateTableOptions());
  }

  @Test(timeout = 100000)
  public void testOpenTableClearsNonCoveringRangePartitions() throws KuduException {
    CreateTableOptions options = getBasicCreateTableOptions();
    PartialRow lower = basicSchema.newPartialRow();
    PartialRow upper = basicSchema.newPartialRow();
    lower.addInt("key", 0);
    upper.addInt("key", 1);
    options.addRangePartition(lower, upper);

    client.createTable(TABLE_NAME, basicSchema, options);
    KuduTable table = client.openTable(TABLE_NAME);

    // Count the number of tablets.
    KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.newScanTokenBuilder(table);
    List<KuduScanToken> tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Add a range partition with a separate client. The new client is necessary
    // in order to avoid clearing the meta cache as part of the alter operation.
    try (KuduClient alterClient = new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                                                .defaultAdminOperationTimeoutMs(harness.DEFAULT_SLEEP)
                                                .build()) {
      AlterTableOptions alter = new AlterTableOptions();
      lower = basicSchema.newPartialRow();
      upper = basicSchema.newPartialRow();
      lower.addInt("key", 1);
      alter.addRangePartition(lower, upper);
      alterClient.alterTable(TABLE_NAME, alter);
    }

    // Count the number of tablets.  The result should still be the same, since
    // the new tablet is still cached as a non-covered range.
    tokenBuilder = client.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(1, tokens.size());

    // Reopen the table and count the tablets again. The new tablet should now show up.
    table = client.openTable(TABLE_NAME);
    tokenBuilder = client.newScanTokenBuilder(table);
    tokens = tokenBuilder.build();
    assertEquals(2, tokens.size());
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentInsert() throws Exception {
    KuduTable table = client.createTable(
        TABLE_NAME, createManyStringsSchema(), getBasicCreateTableOptions().setWait(false));

    // Insert a row.
    //
    // It's very likely that the tablets are still being created, but the client
    // should transparently retry the insert (and associated master lookup)
    // until the operation succeeds.
    Insert insert = table.newInsert();
    insert.getRow().addString("key", "key_0");
    insert.getRow().addString("c1", "c1_0");
    insert.getRow().addString("c2", "c2_0");
    KuduSession session = client.newSession();
    OperationResponse resp = session.apply(insert);
    assertFalse(resp.hasRowError());

    // This won't do anything useful (i.e. if the insert succeeds, we know the
    // table has been created), but it's here for additional code coverage.
    assertTrue(client.isCreateTableDone(TABLE_NAME));
  }

  @Test(timeout = 100000)
  public void testCreateTableWithConcurrentAlter() throws Exception {
    // Kick off an asynchronous table creation.
    Deferred<KuduTable> d = asyncClient.createTable(TABLE_NAME,
        createManyStringsSchema(), getBasicCreateTableOptions());

    // Rename the table that's being created to make sure it doesn't interfere
    // with the "wait for all tablets to be created" behavior of createTable().
    //
    // We have to retry this in a loop because we might run before the table
    // actually exists.
    while (true) {
      try {
        client.alterTable(TABLE_NAME,
            new AlterTableOptions().renameTable("foo"));
        break;
      } catch (KuduException e) {
        if (!e.getStatus().isNotFound()) {
          throw e;
        }
      }
    }

    // If createTable() was disrupted by the alterTable(), this will throw.
    d.join();
  }

  // This is a test that verifies, when multiple clients run
  // simultaneously, a client can get read-your-writes and
  // read-your-reads session guarantees using READ_YOUR_WRITES
  // scan mode, from leader replica. In this test writes are
  // performed in AUTO_FLUSH_SYNC (single operation) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesSyncClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC,
            ReplicaSelection.CLOSEST_REPLICA);
  }

  // Similar to testReadYourWritesSyncLeaderReplica, but in this
  // test writes are performed in MANUAL_FLUSH (batches) flush modes.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchLeaderReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
                   ReplicaSelection.LEADER_ONLY);
  }

  // Similar test as above but scan from the closest replica.
  @Test(timeout = 100000)
  public void testReadYourWritesBatchClosestReplica() throws Exception {
    readYourWrites(SessionConfiguration.FlushMode.MANUAL_FLUSH,
            ReplicaSelection.CLOSEST_REPLICA);
  }

  private void readYourWrites(final SessionConfiguration.FlushMode flushMode,
                              final ReplicaSelection replicaSelection)
          throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    final int tasksNum = 4;
    List<Callable<Void>> callables = new ArrayList<>();
    for (int t = 0; t < tasksNum; t++) {
      Callable<Void> callable = new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          // From the same client continuously performs inserts to a tablet
          // in the given flush mode.
          KuduSession session = client.newSession();
          session.setFlushMode(flushMode);
          KuduTable table = client.openTable(TABLE_NAME);
          for (int i = 0; i < 3; i++) {
            for (int j = 100 * i; j < 100 * (i + 1); j++) {
              Insert insert = table.newInsert();
              PartialRow row = insert.getRow();
              row.addString("key", String.format("key_%02d", j));
              row.addString("c1", "c1_" + j);
              row.addString("c2", "c2_" + j);
              row.addString("c3", "c3_" + j);
              session.apply(insert);
            }
            session.flush();

            // Perform a bunch of READ_YOUR_WRITES scans to all the replicas
            // that count the rows. And verify that the count of the rows
            // never go down from what previously observed, to ensure subsequent
            // reads will not "go back in time" regarding writes that other
            // clients have done.
            for (int k = 0; k < 3; k++) {
              AsyncKuduScanner scanner = asyncClient.newScannerBuilder(table)
                      .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                      .replicaSelection(replicaSelection)
                      .build();
              KuduScanner syncScanner = new KuduScanner(scanner);
              long preTs = asyncClient.getLastPropagatedTimestamp();
              assertNotEquals(AsyncKuduClient.NO_TIMESTAMP,
                  asyncClient.getLastPropagatedTimestamp());

              long row_count = countRowsInScan(syncScanner);
              long expected_count = 100L * (i + 1);
              assertTrue(expected_count <= row_count);

              // After the scan, verify that the chosen snapshot timestamp is
              // returned from the server and it is larger than the previous
              // propagated timestamp.
              assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
              assertTrue(preTs < scanner.getSnapshotTimestamp());
              syncScanner.close();
            }
          }
          return null;
        }
      };
      callables.add(callable);
    }
    ExecutorService executor = Executors.newFixedThreadPool(tasksNum);
    List<Future<Void>> futures = executor.invokeAll(callables);

    // Waits for the spawn tasks to complete, and then retrieves the results.
    // Any exceptions or assertion errors in the spawn tasks will be thrown here.
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  private void runTestCallDuringLeaderElection(String clientMethodName) throws Exception {
    // This bit of reflection helps us avoid duplicating test code.
    Method methodToInvoke = KuduClient.class.getMethod(clientMethodName);

    for (int i = 0; i < 5; i++) {
      KuduClient cl = new KuduClient.KuduClientBuilder(
          harness.getMasterAddressesAsString()).build();
      harness.restartLeaderMaster();

      // There's a good chance that this executes while there's no leader
      // master. It should retry until the leader election completes and a new
      // leader master is elected.
      methodToInvoke.invoke(cl);
    }

    // With all masters down, exportAuthenticationCredentials() should time out.
    harness.killAllMasterServers();
    KuduClient cl = new KuduClient.KuduClientBuilder(
        harness.getMasterAddressesAsString())
                    .defaultAdminOperationTimeoutMs(5000) // speed up the test
                    .build();
    try {
      methodToInvoke.invoke(cl);
      fail();
    } catch (InvocationTargetException ex) {
      assertTrue(ex.getTargetException() instanceof KuduException);
      KuduException realEx = (KuduException)ex.getTargetException();
      assertTrue(realEx.getStatus().isTimedOut());
    }
  }

  @Test(timeout = 100000)
  public void testExportAuthenticationCredentialsDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("exportAuthenticationCredentials");
  }

  @Test(timeout = 100000)
  public void testGetHiveMetastoreConfigDuringLeaderElection() throws Exception {
    runTestCallDuringLeaderElection("getHiveMetastoreConfig");
  }

  /**
   * Test assignment of a location to the client.
   */
  @Test(timeout = 100000)
  public void testClientLocationNoLocation() throws Exception {
    // Do something that will cause the client to connect to the cluster.
    client.listTabletServers();
    assertEquals("", client.getLocationString());
  }

  @Test(timeout = 100000)
  @LocationConfig(locations = {
      "/L0:6", // 3 masters, 1 client, 3 tablet servers: 3 * 1 + 3 = 6.
  })
  public void testClientLocation() throws Exception {
    // Do something that will cause the client to connect to the cluster.
    client.listTabletServers();
    assertEquals("/L0", client.getLocationString());
  }

  @Test(timeout = 100000)
  public void testSessionOnceClosed() throws Exception {
    client.createTable(TABLE_NAME, basicSchema, getBasicCreateTableOptions());
    KuduTable table = client.openTable(TABLE_NAME);
    KuduSession session = client.newSession();

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    Insert insert = createBasicSchemaInsert(table, 0);
    session.apply(insert);
    session.close();
    assertTrue(session.isClosed());

    insert = createBasicSchemaInsert(table, 1);
    CapturingLogAppender cla = new CapturingLogAppender();
    try (Closeable c = cla.attach()) {
      session.apply(insert);
    }
    String loggedText = cla.getAppendedText();
    assertTrue("Missing warning:\n" + loggedText,
               loggedText.contains("this is unsafe"));
  }
}
