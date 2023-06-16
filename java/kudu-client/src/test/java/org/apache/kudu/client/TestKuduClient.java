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
import static org.apache.kudu.test.ClientTestUtil.createManyVarcharsSchema;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithBinaryColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDateColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithDecimalColumns;
import static org.apache.kudu.test.ClientTestUtil.createSchemaWithNonUniqueKey;
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.security.cert.CertificateException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.stumbleupon.async.Deferred;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.CapturingLogAppender;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.LocationConfig;
import org.apache.kudu.test.KuduTestHarness.MasterServerConfig;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.RandomUtils;
import org.apache.kudu.test.cluster.KuduBinaryInfo;
import org.apache.kudu.util.DateUtil;
import org.apache.kudu.util.DecimalUtil;
import org.apache.kudu.util.TimestampUtil;

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

    long initialTs = client.getLastPropagatedTimestamp();

    // Check that the initial timestamp is consistent with the asynchronous client.
    assertEquals(initialTs, client.getLastPropagatedTimestamp());
    assertEquals(initialTs, asyncClient.getLastPropagatedTimestamp());

    // Attempt to change the timestamp to a lower value. This should not change
    // the internal timestamp, as it must be monotonically increasing.
    client.updateLastPropagatedTimestamp(initialTs - 1);
    assertEquals(initialTs, client.getLastPropagatedTimestamp());
    assertEquals(initialTs, asyncClient.getLastPropagatedTimestamp());

    // Use the synchronous client to update the last propagated timestamp and
    // check with both clients that the timestamp was updated.
    client.updateLastPropagatedTimestamp(initialTs + 1);
    assertEquals(initialTs + 1, client.getLastPropagatedTimestamp());
    assertEquals(initialTs + 1, asyncClient.getLastPropagatedTimestamp());
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
   * Test recalling a soft deleted table through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testRecallDeletedTable() throws Exception {
    // Check that we can create a table.
    assertTrue(client.getTablesList().getTablesList().isEmpty());
    final KuduTable table = client.createTable(TABLE_NAME, basicSchema,
        getBasicCreateTableOptions());
    final String tableId = table.getTableId();
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(TABLE_NAME, client.getTablesList().getTablesList().get(0));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME, 600);
    List<String> tables = client.getTablesList().getTablesList();
    assertEquals(0, tables.size());
    tables = client.getSoftDeletedTablesList().getTablesList();
    assertEquals(1, tables.size());
    String softDeletedTable = tables.get(0);
    assertEquals(TABLE_NAME, softDeletedTable);
    // Check that we can recall the soft_deleted table.
    client.recallDeletedTable(tableId);
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(TABLE_NAME, client.getTablesList().getTablesList().get(0));

    // Check that we can delete it.
    client.deleteTable(TABLE_NAME, 600);
    tables = client.getTablesList().getTablesList();
    assertEquals(0, tables.size());
    tables = client.getSoftDeletedTablesList().getTablesList();
    assertEquals(1, tables.size());
    softDeletedTable = tables.get(0);
    assertEquals(TABLE_NAME, softDeletedTable);
    // Check we can recall soft deleted table with new table name.
    final String newTableName = "NewTable";
    client.recallDeletedTable(tableId, newTableName);
    assertEquals(1, client.getTablesList().getTablesList().size());
    assertEquals(newTableName, client.getTablesList().getTablesList().get(0));
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

  /**
   * Test creating and deleting a table with extra-configs through a KuduClient.
   */
  @Test(timeout = 100000)
  public void testCreateDeleteTableWitExtraConfigs() throws Exception {
    // Check that we can create a table.
    Map<String, String> extraConfigs = new HashMap<>();
    extraConfigs.put("kudu.table.history_max_age_sec", "7200");

    client.createTable(
        TABLE_NAME,
        basicSchema,
        getBasicCreateTableOptions().setExtraConfigs(extraConfigs));

    KuduTable table = client.openTable(TABLE_NAME);
    extraConfigs = table.getExtraConfig();
    assertTrue(extraConfigs.containsKey("kudu.table.history_max_age_sec"));
    assertEquals("7200", extraConfigs.get("kudu.table.history_max_age_sec"));
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
    Random random = RandomUtils.getRandom();
    for (int i = 0; i < 10; i++) {
      Thread.sleep(SHORT_SCANNER_TTL_MS / 4);
      // Force 1/3 of the keepAlive requests to retry up to 3 times.
      if (i % 3 == 0) {
        RpcProxy.failNextRpcs(random.nextInt(4),
            new RecoverableException(Status.ServiceUnavailable("testKeepAlive")));
      }
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
    List<String> expectedStrings = ImmutableList.of(
        "STRING key=r1, STRING c1=a, STRING c2=b, STRING c3=c, STRING c4=d",
        "STRING key=r2, STRING c1=NULL, STRING c2=NULL, STRING c3=c, STRING c4=d",
        "STRING key=r3, STRING c1=NULL, STRING c2=def, STRING c3=c, STRING c4=def");
    List<String> rowStrings = scanTableToStrings(table);
    Collections.sort(rowStrings);
    assertArrayEquals(rowStrings.toArray(new String[0]),
                      expectedStrings.toArray(new String[0]));
  }

  /**
   * Test inserting and retrieving VARCHAR columns.
   */
  @Test(timeout = 100000)
  public void testVarchars() throws Exception {
    Schema schema = createManyVarcharsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addVarchar("key", String.format("key_%02d", i));
      row.addVarchar("c2", "c2_" + i);
      if (i % 2 == 1) {
        row.addVarchar("c3", "c3_" + i);
      }
      row.addVarchar("c4", "c4_" + i);
      // NOTE: we purposefully add the strings in a non-left-to-right
      // order to verify that we still place them in the right position in
      // the row.
      row.addVarchar("c1", "c1_" + i);
      session.apply(insert);
      if (i % 50 == 0) {
        session.flush();
      }
    }
    session.flush();

    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(100, rowStrings.size());
    assertEquals(
        "VARCHAR key(10)=key_03, VARCHAR c1(10)=c1_3, VARCHAR c2(10)=c2_3," +
        " VARCHAR c3(10)=c3_3, VARCHAR c4(10)=c4_3", rowStrings.get(3));
    assertEquals(
        "VARCHAR key(10)=key_04, VARCHAR c1(10)=c1_4, VARCHAR c2(10)=c2_4," +
        " VARCHAR c3(10)=NULL, VARCHAR c4(10)=c4_4", rowStrings.get(4));

    KuduScanner scanner = client.newScannerBuilder(table).build();

    assertTrue("Scanner should have returned row", scanner.hasMoreRows());

    RowResultIterator rows = scanner.nextRows();
    final RowResult next = rows.next();

    // Do negative testing on string type.
    try {
      next.getInt("c2");
      fail("IllegalArgumentException was not thrown when accessing " +
          "a VARCHAR column with getInt");
    } catch (IllegalArgumentException ignored) {
      // ignored
    }
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
    } catch (IllegalArgumentException ignored) {
      // ignored
    }
  }

  /**
   * Test to verify that we can write in and read back UTF8.
   */
  @Test(timeout = 100000)
  public void testUTF8() throws Exception {
    Schema schema = createManyStringsSchema();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduTable table = client.openTable(TABLE_NAME);
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "กขฃคฅฆง"); // some thai
    row.addString("c1", "✁✂✃✄✆"); // some icons

    row.addString("c2", "hello"); // some normal chars
    row.addString("c4", "🐱"); // supplemental plane
    KuduSession session = client.newSession();
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
      expectedRow.append(String.format("BINARY key=\"key_%02d\", STRING c1=✁✂✃✄✆, DOUBLE c2=%.1f," +
          " BINARY c3=", i, (double) i));
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
      while (timestamp == lastTimestamp) {
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
   * Test inserting and retrieving date columns.
   */
  @Test(timeout = 100000)
  public void testDateColumns() throws Exception {
    Schema schema = createSchemaWithDateColumns();
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    List<Integer> dates = new ArrayList<>();

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    for (int i = 0; i < 100; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      dates.add(i);
      Date date = DateUtil.epochDaysToSqlDate(i);
      row.addDate("key", date);
      if (i % 2 == 1) {
        row.addDate("c1", date);
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
      String sdate = DateUtil.epochDaysToDateString(dates.get(i));
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("DATE key=%s, DATE c1=", sdate));
      if (i % 2 == 1) {
        expectedRow.append(sdate);
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
      expectedRow.append(String.format("DECIMAL key(18, 0)=%s, DECIMAL c1(38, 0)=",
          String.valueOf(i)));
      if (i % 2 == 1) {
        expectedRow.append(i);
      } else {
        expectedRow.append("NULL");
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }
  }

  /**
   * Test creating a table with non unique primary key in the table schema.
   */
  @Test(timeout = 100000)
  public void testCreateTableWithNonUniquePrimaryKeys() throws Exception {
    // Create a schema with non unique primary key column
    Schema schema = createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    // Verify auto-incrementing column is in the schema
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    // Create a table
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    // Verify that the primary key is not unique, and an auto-incrementing column is
    // added as key column in the position after all key columns.
    schema = table.getSchema();
    assertFalse(schema.isPrimaryKeyUnique());
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    assertEquals(1, schema.getColumnIndex(Schema.getAutoIncrementingColumnName()));
    assertTrue(schema.getColumn(Schema.getAutoIncrementingColumnName()).isKey());
    assertTrue(schema.getColumn(
        Schema.getAutoIncrementingColumnName()).isAutoIncrementing());

    // Insert rows into the table without assigning values for the auto-incrementing
    // column.
    for (int i = 0; i < 3; i++) {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", i);
      row.addInt("c1", i * 10);
      session.apply(insert);
    }
    session.flush();

    // Scan all the rows in the table with all columns.
    // Verify that the auto-incrementing column is included in the rows.
    List<String> rowStrings = scanTableToStrings(table);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      expectedRow.append(String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
          i, Schema.getAutoIncrementingColumnName(), i + 1, i * 10));
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    // Update "c1" column of the first row with "key" and auto-incrementing columns.
    Update update = table.newUpdate();
    PartialRow row = update.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    row.addInt(schema.getColumnByIndex(2).getName(), 100);
    session.apply(update);
    session.flush();

    // Scan all the rows in the table without the auto-incrementing column.
    // Verify that "c1" column of the first row is updated.
    KuduScanner.KuduScannerBuilder scanBuilder = client.newScannerBuilder(table);
    KuduScanner scanner =
        scanBuilder.setProjectedColumnNames(Lists.newArrayList("key", "c1")).build();
    rowStrings.clear();
    for (RowResult r : scanner) {
      rowStrings.add(r.rowToString());
    }
    Collections.sort(rowStrings);
    assertEquals(3, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      StringBuilder expectedRow = new StringBuilder();
      if (i == 0) {
        expectedRow.append(String.format("INT32 key=0, INT32 c1=100"));
      } else {
        expectedRow.append(String.format("INT32 key=%d, INT32 c1=%d", i, i * 10));
      }
      assertEquals(expectedRow.toString(), rowStrings.get(i));
    }

    // Upsert rows into the table after assigning values for the auto-incrementing
    // column. The first three rows will be applied as updates and the next three as
    // inserts.
    for (int i = 0; i < 6; i++) {
      Upsert upsert = table.newUpsert();
      row = upsert.getRow();
      row.addInt("key", i);
      row.addLong(Schema.getAutoIncrementingColumnName(), i + 1);
      row.addInt("c1", i * 20);
      session.apply(upsert);
    }
    session.flush();

    // Scan all the rows in the table with all columns.
    // Verify that the auto-incrementing column is included in the rows.
    rowStrings = scanTableToStrings(table);
    assertEquals(6, rowStrings.size());
    for (int i = 0; i < rowStrings.size(); i++) {
      String expectedRow = String.format("INT32 key=%d, INT64 %s=%d, INT32 c1=%d",
              i, Schema.getAutoIncrementingColumnName(), i + 1, i * 20);
      assertEquals(expectedRow, rowStrings.get(i));
    }

    // Delete the first row with "key" and auto-incrementing columns.
    // Verify that number of rows is decreased by 1.
    Delete delete = table.newDelete();
    row = delete.getRow();
    row.addInt(schema.getColumnByIndex(0).getName(), 0);
    row.addLong(schema.getColumnByIndex(1).getName(), 1);
    session.apply(delete);
    session.flush();
    assertEquals(5, countRowsInScan(client.newScannerBuilder(table).build()));

    // Check that we can delete the table.
    client.deleteTable(TABLE_NAME);
  }

  /**
   * Test operations for table with auto-incrementing column.
   */
  @Test(timeout = 100000)
  public void testTableWithAutoIncrementingColumn() throws Exception {
    // Create a schema with non unique primary key column
    Schema schema = createSchemaWithNonUniqueKey();
    assertFalse(schema.isPrimaryKeyUnique());
    // Verify auto-incrementing column is in the schema
    assertTrue(schema.hasAutoIncrementingColumn());
    assertEquals(3, schema.getColumnCount());
    assertEquals(2, schema.getPrimaryKeyColumnCount());
    // Create a table
    client.createTable(TABLE_NAME, schema, getBasicCreateTableOptions());

    final KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);
    schema = table.getSchema();
    assertTrue(schema.hasAutoIncrementingColumn());

    // Verify that UPSERT is allowed for table with auto-incrementing column
    Upsert upsert = table.newUpsert();
    PartialRow rowUpsert = upsert.getRow();
    rowUpsert.addInt("key", 0);
    rowUpsert.addLong(Schema.getAutoIncrementingColumnName(), 1);
    rowUpsert.addInt("c1", 10);
    session.apply(upsert);

    // Verify that UPSERT_IGNORE is allowed for table with auto-incrementing column
    UpsertIgnore upsertIgnore = table.newUpsertIgnore();
    PartialRow rowUpsertIgnore = upsertIgnore.getRow();
    rowUpsertIgnore.addInt("key", 1);
    rowUpsertIgnore.addLong(Schema.getAutoIncrementingColumnName(), 2);
    rowUpsertIgnore.addInt("c1", 20);
    session.apply(upsertIgnore);

    // Change desired block size for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeDesiredBlockSize(
        Schema.getAutoIncrementingColumnName(), 1));
    // Change encoding for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeEncoding(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.Encoding.PLAIN_ENCODING));
    // Change compression algorithm for auto-incrementing column
    client.alterTable(TABLE_NAME, new AlterTableOptions().changeCompressionAlgorithm(
        Schema.getAutoIncrementingColumnName(), ColumnSchema.CompressionAlgorithm.NO_COMPRESSION));
    session.flush();

    // Verify that auto-incrementing column value cannot be specified in an INSERT operation.
    try {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insert);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }

    // Verify that auto-incrementing column value cannot be specified in an INSERT_IGNORE operation.
    try {
      InsertIgnore insertIgnore = table.newInsertIgnore();
      PartialRow row = insertIgnore.getRow();
      row.addInt("key", 1);
      row.addLong(Schema.getAutoIncrementingColumnName(), 1);
      row.addInt("c1", 10);
      session.apply(insertIgnore);
      fail("INSERT on table with auto-incrementing column set");
    } catch (KuduException ex) {
      assertTrue(ex.getMessage().contains("Auto-Incrementing column should not " +
          "be specified for INSERT operation"));
    }
    // Verify that auto-incrementing column cannot be added
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          Schema.getAutoIncrementingColumnName(), Schema.getAutoIncrementingColumnType(), 0));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().addColumn(
          new ColumnSchema.AutoIncrementingColumnSchemaBuilder().build()));
      fail("Add auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Column name " +
          Schema.getAutoIncrementingColumnName() + " is reserved by Kudu engine"));
    }

    // Verify that auto-incrementing column cannot be removed
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().dropColumn(
          Schema.getAutoIncrementingColumnName()));
      fail("Drop auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot remove auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be renamed
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().renameColumn(
          Schema.getAutoIncrementingColumnName(), "new_auto_incrementing"));
      fail("Rename auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot rename auto-incrementing column " +
          Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be changed by removing default
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().removeDefault(
          Schema.getAutoIncrementingColumnName()));
      fail("Remove default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Auto-incrementing column " +
          Schema.getAutoIncrementingColumnName() + " does not have default value"));
    }

    // Verify that auto-incrementing column cannot be changed with default value
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeDefault(
          Schema.getAutoIncrementingColumnName(), 0));
      fail("Change default value for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot set default value for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    // Verify that auto-incrementing column cannot be changed for its immutable
    try {
      client.alterTable(TABLE_NAME, new AlterTableOptions().changeImmutable(
          Schema.getAutoIncrementingColumnName(), true));
      fail("Change immutable for auto-incrementing column");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Cannot change immutable for " +
          "auto-incrementing column " + Schema.getAutoIncrementingColumnName()));
    }

    client.deleteTable(TABLE_NAME);
  }

  /**
   * Test inserting and retrieving rows from a table that has a range partition
   * with custom hash schema.
   */
  @Test(timeout = 100000)
  public void testRangeWithCustomHashSchema() throws Exception {
    List<ColumnSchema> cols = new ArrayList<>();
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c0", Type.INT64).key(true).build());
    cols.add(new ColumnSchema.ColumnSchemaBuilder("c1", Type.INT32).nullable(true).build());
    Schema schema = new Schema(cols);

    CreateTableOptions options = new CreateTableOptions();
    options.setRangePartitionColumns(ImmutableList.of("c0"));
    options.addHashPartitions(ImmutableList.of("c0"), 2);

    // Add range partition with table-wide hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", -100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 100);
      options.addRangePartition(lower, upper);
    }

    // Add a partition with custom hash schema.
    {
      PartialRow lower = schema.newPartialRow();
      lower.addLong("c0", 100);
      PartialRow upper = schema.newPartialRow();
      upper.addLong("c0", 200);

      RangePartitionWithCustomHashSchema rangePartition =
          new RangePartitionWithCustomHashSchema(
              lower,
              upper,
              RangePartitionBound.INCLUSIVE_BOUND,
              RangePartitionBound.EXCLUSIVE_BOUND);
      rangePartition.addHashPartitions(ImmutableList.of("c0"), 5, 0);
      options.addRangePartition(rangePartition);
    }

    client.createTable(TABLE_NAME, schema, options);

    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
    KuduTable table = client.openTable(TABLE_NAME);

    // Check the range with the table-wide hash schema.
    {
      for (int i = 0; i < 10; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 1000 * i);
        session.apply(insert);
      }

      // Scan all the rows in the table.
      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(10, rowStringsAll.size());

      // Now scan the rows that are in the range with the table-wide hash schema.
      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 0),
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), LESS, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d", i, 1000 * i));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
    }

    // Check the range with the custom hash schema.
    {
      for (int i = 100; i < 110; ++i) {
        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addLong("c0", i);
        row.addInt("c1", 2 * i);
        session.apply(insert);
      }

      // Scan all the rows in the table.
      List<String> rowStringsAll = scanTableToStrings(table);
      assertEquals(20, rowStringsAll.size());

      // Now scan the rows that are in the range with the custom hash schema.
      List<String> rowStrings = scanTableToStrings(table,
          KuduPredicate.newComparisonPredicate(schema.getColumn("c0"), GREATER_EQUAL, 100));
      assertEquals(10, rowStrings.size());
      for (int i = 0; i < rowStrings.size(); ++i) {
        StringBuilder expectedRow = new StringBuilder();
        expectedRow.append(String.format("INT64 c0=%d, INT32 c1=%d",
            i + 100, 2 * (i + 100)));
        assertEquals(expectedRow.toString(), rowStrings.get(i));
      }
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
    int numRows = 100;
    for (int key = 0; key < numRows; key++) {
      session.apply(createBasicSchemaInsert(table, key));
    }

    // Test with some non-positive limits, expecting to raise an exception.
    int[] nonPositives = { -1, 0 };
    for (int limit : nonPositives) {
      try {
        client.newScannerBuilder(table).limit(limit).build();
        fail();
      } catch (IllegalArgumentException e) {
        assertTrue(e.getMessage().contains("Need a strictly positive number"));
      }
    }

    // Test with a limit and ensure we get the expected number of rows.
    int[] limits = { numRows - 1, numRows, numRows + 1 };
    for (int limit : limits) {
      KuduScanner scanner = client.newScannerBuilder(table)
                                      .limit(limit)
                                      .build();
      int count = 0;
      while (scanner.hasMoreRows()) {
        count += scanner.nextRows().getNumRows();
      }
      assertEquals(String.format("Limit %d returned %d/%d rows", limit, count, numRows),
          Math.min(numRows, limit), count);
    }

    // Now test with limits for async scanners.
    for (int limit : limits) {
      AsyncKuduScanner scanner = new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table)
                                                     .limit(limit)
                                                     .build();
      assertEquals(Math.min(limit, numRows), countRowsInScan(scanner));
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
    AsyncKuduScanner scanner =
        new AsyncKuduScanner.AsyncKuduScannerBuilder(asyncClient, table).build();
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
    try (KuduClient localClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .nioExecutors(Executors.newFixedThreadPool(1),
                     Executors.newFixedThreadPool(2))
                 .bossCount(1)
                 .workerCount(2)
                 .build()) {
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
      for (int t = 0; t < 4; t++) {
        threads[t].join();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
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
    try (KuduClient alterClient =
             new KuduClient.KuduClientBuilder(harness.getMasterAddressesAsString())
                 .defaultAdminOperationTimeoutMs(KuduTestHarness.DEFAULT_SLEEP)
                 .build()) {
      lower = basicSchema.newPartialRow();
      upper = basicSchema.newPartialRow();
      lower.addInt("key", 1);
      AlterTableOptions alter = new AlterTableOptions();
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
          // Create a new client.
          AsyncKuduClient asyncKuduClient = new AsyncKuduClient
                  .AsyncKuduClientBuilder(harness.getMasterAddressesAsString())
                  .defaultAdminOperationTimeoutMs(KuduTestHarness.DEFAULT_SLEEP)
                  .build();
          // From the same client continuously performs inserts to a tablet
          // in the given flush mode.
          try (KuduClient kuduClient = asyncKuduClient.syncClient()) {
            KuduSession session = kuduClient.newSession();
            session.setFlushMode(flushMode);
            KuduTable table = kuduClient.openTable(TABLE_NAME);
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
                AsyncKuduScanner scanner = asyncKuduClient.newScannerBuilder(table)
                                           .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
                                           .replicaSelection(replicaSelection)
                                           .build();
                KuduScanner syncScanner = new KuduScanner(scanner);
                long preTs = asyncKuduClient.getLastPropagatedTimestamp();
                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, preTs);

                long rowCount = countRowsInScan(syncScanner);
                long expectedCount = 100L * (i + 1);
                assertTrue(expectedCount <= rowCount);

                // After the scan, verify that the chosen snapshot timestamp is
                // returned from the server and it is larger than the previous
                // propagated timestamp.
                assertNotEquals(AsyncKuduClient.NO_TIMESTAMP, scanner.getSnapshotTimestamp());
                assertTrue(preTs < scanner.getSnapshotTimestamp());
                syncScanner.close();
              }
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
      try (KuduClient cl = new KuduClient.KuduClientBuilder(
          harness.getMasterAddressesAsString()).build()) {
        harness.restartLeaderMaster();

        // There's a good chance that this executes while there's no leader
        // master. It should retry until the leader election completes and a new
        // leader master is elected.
        methodToInvoke.invoke(cl);
      }
    }

    // With all masters down, exportAuthenticationCredentials() should time out.
    harness.killAllMasterServers();
    try (KuduClient cl = new KuduClient.KuduClientBuilder(
        harness.getMasterAddressesAsString())
         .defaultAdminOperationTimeoutMs(5000) // speed up the test
         .build()) {
      try {
        methodToInvoke.invoke(cl);
        fail();
      } catch (InvocationTargetException ex) {
        assertTrue(ex.getTargetException() instanceof KuduException);
        KuduException realEx = (KuduException) ex.getTargetException();
        assertTrue(realEx.getStatus().isTimedOut());
      }
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
  @MasterServerConfig(flags = {
      "--master_client_location_assignment_enabled=true",
  })
  public void testClientLocation() throws Exception {
    // Do something that will cause the client to connect to the cluster.
    client.listTabletServers();
    assertEquals("/L0", client.getLocationString());
  }

  @Test(timeout = 100000)
  public void testClusterId() throws Exception {
    assertTrue(client.getClusterId().isEmpty());
    // Do something that will cause the client to connect to the cluster.
    client.listTabletServers();
    assertFalse(client.getClusterId().isEmpty());
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

  @Test(timeout = 100000)
  public void testSchemaDriftPattern() throws Exception {
    KuduTable table = client.createTable(
            TABLE_NAME, createManyStringsSchema(), getBasicCreateTableOptions().setWait(false));
    KuduSession session = client.newSession();

    // Insert a row.
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addString("key", "key_0");
    row.addString("c1", "c1_0");
    row.addString("c2", "c2_0");
    row.addString("c3", "c3_0");
    row.addString("c4", "c4_0");
    OperationResponse resp = session.apply(insert);
    assertFalse(resp.hasRowError());

    // Insert a row with an extra column.
    boolean retried = false;
    while (true) {
      try {
        Insert insertExtra = table.newInsert();
        PartialRow rowExtra = insertExtra.getRow();
        rowExtra.addString("key", "key_1");
        rowExtra.addString("c1", "c1_1");
        rowExtra.addString("c2", "c2_1");
        rowExtra.addString("c3", "c2_1");
        rowExtra.addString("c4", "c2_1");
        rowExtra.addString("c5", "c5_1");
        OperationResponse respExtra = session.apply(insertExtra);
        assertFalse(respExtra.hasRowError());
        break;
      } catch (IllegalArgumentException e) {
        if (retried) {
          throw e;
        }
        // Add the missing column and retry.
        if (e.getMessage().contains("Unknown column")) {
          client.alterTable(TABLE_NAME, new AlterTableOptions()
                  .addNullableColumn("c5", Type.STRING));
          // We need to re-open the table to ensure it has the new schema.
          table = client.openTable(TABLE_NAME);
          retried = true;
        } else {
          throw e;
        }
      }
    }
    // Make sure we actually retried.
    assertTrue(retried);

    // Insert a row with the old schema.
    Insert insertOld = table.newInsert();
    PartialRow rowOld = insertOld.getRow();
    rowOld.addString("key", "key_3");
    rowOld.addString("c1", "c1_3");
    rowOld.addString("c2", "c2_3");
    rowOld.addString("c3", "c3_3");
    rowOld.addString("c4", "c4_3");
    OperationResponse respOld = session.apply(insertOld);
    assertFalse(respOld.hasRowError());
  }

  /**
   * This is a test scenario to reproduce conditions described in KUDU-3277.
   * The scenario was failing before the fix:
   *   ** 'java.lang.AssertionError: This Deferred was already called' was
   *       encountered multiple times with the stack exactly as described in
   *       KUDU-3277
   *   ** some flusher threads were unable to join since KuduSession.flush()
   *      would hang (i.e. would not return)
   */
  @MasterServerConfig(flags = {
      // A shorter TTL for tablet locations is necessary to induce more frequent
      // calls to TabletLookupCB.queueBuffer().
      "--table_locations_ttl_ms=500",
  })
  @Test(timeout = 100000)
  public void testConcurrentFlush() throws Exception {
    // This is a very intensive and stressful test scenario, so run it only
    // against Kudu binaries built without sanitizers.
    assumeTrue("this scenario is to run against non-sanitized binaries only",
        KuduBinaryInfo.getSanitizerType() == KuduBinaryInfo.SanitizerType.NONE);
    try {
      AsyncKuduSession.injectLatencyBufferFlushCb(true);

      CreateTableOptions opts = new CreateTableOptions()
          .addHashPartitions(ImmutableList.of("key"), 8)
          .setRangePartitionColumns(ImmutableList.of("key"));

      Schema schema = ClientTestUtil.getBasicSchema();
      PartialRow lowerBoundA = schema.newPartialRow();
      PartialRow upperBoundA = schema.newPartialRow();
      upperBoundA.addInt("key", 0);
      opts.addRangePartition(lowerBoundA, upperBoundA);

      PartialRow lowerBoundB = schema.newPartialRow();
      lowerBoundB.addInt("key", 0);
      PartialRow upperBoundB = schema.newPartialRow();
      opts.addRangePartition(lowerBoundB, upperBoundB);

      KuduTable table = client.createTable(TABLE_NAME, schema, opts);

      final CountDownLatch keepRunning = new CountDownLatch(1);
      final int numSessions = 50;

      List<KuduSession> sessions = new ArrayList<>(numSessions);
      for (int i = 0; i < numSessions; ++i) {
        KuduSession session = client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        sessions.add(session);
      }

      List<Thread> flushers = new ArrayList<>(numSessions);
      Random random = RandomUtils.getRandom();
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          Thread flusher = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                while (!keepRunning.await(random.nextInt(250), TimeUnit.MILLISECONDS)) {
                  session.flush();
                  assertEquals(0, session.countPendingErrors());
                }
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
            }
          });
          flushers.add(flusher);
        }
      }

      final int numRowsPerSession = 10000;
      final CountDownLatch insertersCompleted = new CountDownLatch(numSessions);
      List<Thread> inserters = new ArrayList<>(numSessions);
      {
        for (int idx = 0; idx < numSessions; ++idx) {
          final int threadIdx = idx;
          final int keyStart = threadIdx * numRowsPerSession;
          Thread inserter = new Thread(new Runnable() {
            @Override
            public void run() {
              KuduSession session = sessions.get(threadIdx);
              try {
                for (int key = keyStart; key < keyStart + numRowsPerSession; ++key) {
                  Insert insert = ClientTestUtil.createBasicSchemaInsert(table, key);
                  assertNull(session.apply(insert));
                }
                session.flush();
              } catch (Exception e) {
                fail("unexpected exception: " + e);
              }
              insertersCompleted.countDown();
            }
          });
          inserters.add(inserter);
        }
      }

      for (Thread flusher : flushers) {
        flusher.start();
      }
      for (Thread inserter : inserters) {
        inserter.start();
      }

      // Wait for the inserter threads to finish.
      insertersCompleted.await();
      // Signal the flusher threads to stop.
      keepRunning.countDown();

      for (Thread inserter : inserters) {
        inserter.join();
      }
      for (Thread flusher : flushers) {
        flusher.join();
      }

      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES)
          .build();
      assertEquals(numSessions * numRowsPerSession, countRowsInScan(scanner));
    } finally {
      AsyncKuduSession.injectLatencyBufferFlushCb(false);
    }
  }

  @Test(timeout = 50000)
  public void testImportInvalidCert() throws Exception {
    // An empty certificate to import.
    byte[] caCert = new byte[0];
    CertificateException e = assertThrows(CertificateException.class, () -> {
      client.trustedCertificates(Arrays.asList(ByteString.copyFrom(caCert)));
    });
    assertTrue(e.getMessage().contains("Could not parse certificate"));
  }
}
