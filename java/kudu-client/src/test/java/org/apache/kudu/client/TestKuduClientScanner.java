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
import static org.apache.kudu.test.ClientTestUtil.countRowsInScan;
import static org.apache.kudu.test.ClientTestUtil.createBasicSchemaInsert;
import static org.apache.kudu.test.ClientTestUtil.createManyStringsSchema;
import static org.apache.kudu.test.ClientTestUtil.getBasicCreateTableOptions;
import static org.apache.kudu.test.ClientTestUtil.getBasicTableOptionsWithNonCoveredRange;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Random;

import com.google.common.collect.ImmutableList;
import io.netty.util.Timeout;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.Schema;
import org.apache.kudu.test.ClientTestUtil;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.test.KuduTestHarness.TabletServerConfig;
import org.apache.kudu.test.RandomUtils;

/**
 * Scanner-related tests for KuduClient including expiration, keep-alive,
 * limits, and predicates.
 */
public class TestKuduClientScanner {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClientScanner.class);

  private static final String TABLE_NAME = "TestKuduClientScanner";

  private static final int SHORT_SCANNER_TTL_MS = 5000;
  private static final int SHORT_SCANNER_GC_US = SHORT_SCANNER_TTL_MS * 100; // 10% of the TTL.

  private static final Schema basicSchema = ClientTestUtil.getBasicSchema();

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Rule(order = Integer.MIN_VALUE)
  public TestRule watcherRule = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      System.out.println("[ TEST: STARTING  ] " + description.getMethodName());
    }

    @Override
    protected void succeeded(Description description) {
      System.out.println("[ TEST: SUCCEEDED ] " + description.getMethodName());
    }

    @Override
    protected void failed(Throwable e, Description description) {
      System.out.println("[ TEST: FAILED    ] " + description.getMethodName());
    }
  };

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
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
      assertTrue(ex.getMessage().contains("Scanner has already been closed"));
    }
  }

  /*
   * Test keeping a scanner alive periodically beyond scanner ttl.
   */
  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS / 5,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testKeepAlivePeriodically() throws Exception {
    // Create a basic table and load it with data.
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 3));
    KuduSession session = client.newSession();
    KuduTable table = client.openTable(TABLE_NAME);

    for (int i = 0; i < numRows; i++) {
      Insert insert = createBasicSchemaInsert(table, i);
      session.apply(insert);
    }

    // Start keep-alive timer and read all data out. After read out all data,
    // the keep-alive timer will be cancelled.
    {
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
          .batchSizeBytes(100)
          .build();

      scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10);
      int rowCount = 0;
      while (scanner.hasMoreRows()) {
        // Sleep a long time to make scanner easy to be expired.
        Thread.sleep(SHORT_SCANNER_TTL_MS / 2);
        rowCount += scanner.nextRows().getNumRows();
      }
      assertEquals(numRows, rowCount);
      // Check that keepAliveTimeout is cancelled.
      Field fieldAsyncScanner = KuduScanner.class.getDeclaredField("asyncScanner");
      fieldAsyncScanner.setAccessible(true);
      AsyncKuduScanner asyncScanner = (AsyncKuduScanner) fieldAsyncScanner.get(scanner);
      Field fieldKeepaliveTimeout =
          AsyncKuduScanner.class.getDeclaredField("keepAliveTimeout");
      fieldKeepaliveTimeout.setAccessible(true);
      Timeout keepAliveTimeout = (Timeout) fieldKeepaliveTimeout.get(asyncScanner);
      assertTrue(keepAliveTimeout.isCancelled());
    }

    // Start keep-alive timer then close it. After closing the client,
    // the keep-alive timer will be closed.
    {
      KuduScanner scanner = new KuduScanner.KuduScannerBuilder(asyncClient, table)
          .replicaSelection(ReplicaSelection.CLOSEST_REPLICA)
          .batchSizeBytes(100)
          .build();

      scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10);

      // Check that keepAliveTimeout is not cancelled.
      Field fieldAsyncScanner = KuduScanner.class.getDeclaredField("asyncScanner");
      fieldAsyncScanner.setAccessible(true);
      AsyncKuduScanner asyncScanner = (AsyncKuduScanner) fieldAsyncScanner.get(scanner);
      Field fieldKeepaliveTimeout =
          AsyncKuduScanner.class.getDeclaredField("keepAliveTimeout");
      fieldKeepaliveTimeout.setAccessible(true);
      Timeout keepAliveTimeout = (Timeout) fieldKeepaliveTimeout.get(asyncScanner);
      assertFalse(keepAliveTimeout.isCancelled());

      // Check that keepAliveTimeout is cancelled.
      scanner.close();
      assertTrue(keepAliveTimeout.isCancelled());
    }
  }

  /*
   * Test stopping the keep-alive timer.
   */
  @Test(timeout = 100000)
  @TabletServerConfig(flags = {
      "--scanner_ttl_ms=" + SHORT_SCANNER_TTL_MS / 5,
      "--scanner_gc_check_interval_us=" + SHORT_SCANNER_GC_US,
  })
  public void testStopKeepAlivePeriodically() throws Exception {
    // Create a basic table and load it with data.
    int numRows = 1000;
    client.createTable(
        TABLE_NAME,
        basicSchema,
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 3));
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
    // Start the keep-alive timer and then close it. Read data will timeout.
    assertTrue(scanner.startKeepAlivePeriodically(SHORT_SCANNER_TTL_MS / 10));
    assertTrue(scanner.stopKeepAlivePeriodically());
    while (scanner.hasMoreRows()) {
      try {
        // Sleep a long time to make scanner easy to be expired.
        Thread.sleep(SHORT_SCANNER_TTL_MS / 2);
        scanner.nextRows();
      } catch (Exception e) {
        assertTrue(e.toString().contains("not found (it may have expired)"));
        break;
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
}
