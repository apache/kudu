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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

public class TestFlexiblePartitioning extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestKuduClient.class);
  private String tableName;

  @Before
  public void setTableName() {
    tableName = TestKuduClient.class.getName() + "-" + System.currentTimeMillis();
  }

  private static Schema createSchema() {
    ArrayList<ColumnSchema> columns = new ArrayList<>(3);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("a", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("b", Type.STRING).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("c", Type.STRING).key(true).build());
    return new Schema(columns);
  }

  private static Set<Row> rows() throws KuduException {
    Set<Row> rows = new HashSet<>();
    for (int a = 0; a < 6; a++) {
      for (int b = 0; b < 6; b++) {
        for (int c = 0; c < 6; c++) {
          rows.add(new Row(String.format("%s", a),
                           String.format("%s", b),
                           String.format("%s", c)));
        }
      }
    }
    return rows;
  }

  private void insertRows(KuduTable table, Set<Row> rows) throws KuduException {
    KuduSession session = syncClient.newSession();
    try {
      for (Row row : rows) {
        Insert insert = table.newInsert();
        PartialRow insertRow = insert.getRow();
        row.fillPartialRow(insertRow);
        session.apply(insert);
      }
    } finally {
      session.close();
    }
  }

  private Set<Row> collectRows(KuduScanner scanner) throws KuduException {
    Set<Row> rows = new HashSet<>();
    while (scanner.hasMoreRows()) {
      for (RowResult result : scanner.nextRows()) {
        rows.add(Row.fromResult(result));
      }
    }
    return rows;
  }

  /**
   * Collects the rows from a set of scan tokens.
   *
   * @param scanTokens the scan token builder
   * @return the rows
   */
  private Set<Row> collectRows(KuduScanToken.KuduScanTokenBuilder scanTokens) throws Exception {
    Set<Row> rows = new HashSet<>();
    for (KuduScanToken token : scanTokens.build()) {
      LOG.debug("Scanning token: {}", KuduScanToken.stringifySerializedToken(token.serialize(),
                                                                             syncClient));

      int existingCount = rows.size();
      Set<Row> newRows = collectRows(token.intoScanner(syncClient));
      rows.addAll(newRows);
      assertEquals(existingCount + newRows.size(), rows.size());
    }
    return rows;
  }

  private void testPartitionSchema(CreateTableOptions tableBuilder) throws Exception {
    Schema schema = createSchema();

    syncClient.createTable(tableName, schema, tableBuilder);

    KuduTable table = syncClient.openTable(tableName);

    Set<Row> rows = rows();
    insertRows(table, rows);

    // Full table scan
    assertEquals(rows, collectRows(syncClient.newScannerBuilder(table).build()));

    { // Lower bound
      Row minRow = new Row("1", "3", "5");
      PartialRow lowerBound = schema.newPartialRow();
      minRow.fillPartialRow(lowerBound);

      Set<Row> expected = Sets.filter(rows, minRow.gtePred());

      KuduScanner scanner = syncClient.newScannerBuilder(table).lowerBound(lowerBound).build();
      Set<Row> results = collectRows(scanner);
      assertEquals(expected, results);

      KuduScanToken.KuduScanTokenBuilder scanTokens =
          syncClient.newScanTokenBuilder(table).lowerBound(lowerBound);
      Set<Row> tokenResults = collectRows(scanTokens);
      assertEquals(expected, tokenResults);
    }

    { // Upper bound
      Row maxRow = new Row("1", "3", "5");
      PartialRow upperBound = schema.newPartialRow();
      maxRow.fillPartialRow(upperBound);

      Set<Row> expected = Sets.filter(rows, maxRow.ltPred());

      KuduScanner scanner = syncClient.newScannerBuilder(table)
                                      .exclusiveUpperBound(upperBound)
                                      .build();
      Set<Row> results = collectRows(scanner);
      assertEquals(expected, results);

      KuduScanToken.KuduScanTokenBuilder scanTokens =
          syncClient.newScanTokenBuilder(table).exclusiveUpperBound(upperBound);
      Set<Row> tokenResults = collectRows(scanTokens);
      assertEquals(expected, tokenResults);
    }

    { // Lower & Upper bounds
      Row minRow = new Row("1", "3", "5");
      Row maxRow = new Row("2", "4", "");
      PartialRow lowerBound = schema.newPartialRow();
      minRow.fillPartialRow(lowerBound);
      PartialRow upperBound = schema.newPartialRow();
      maxRow.fillPartialRow(upperBound);

      Set<Row> expected = Sets.filter(rows, Predicates.and(minRow.gtePred(), maxRow.ltPred()));

      KuduScanner scanner = syncClient.newScannerBuilder(table)
                                      .lowerBound(lowerBound)
                                      .exclusiveUpperBound(upperBound)
                                      .build();
      Set<Row> results = collectRows(scanner);
      assertEquals(expected, results);

      KuduScanToken.KuduScanTokenBuilder scanTokens =
          syncClient.newScanTokenBuilder(table)
                    .lowerBound(lowerBound)
                    .exclusiveUpperBound(upperBound);
      Set<Row> tokenResults = collectRows(scanTokens);
      assertEquals(expected, tokenResults);
    }

    List<LocatedTablet> tablets = table.getTabletsLocations(TestTimeouts.DEFAULT_SLEEP);

    { // Per-tablet scan
      Set<Row> results = new HashSet<>();

      for (LocatedTablet tablet : tablets) {
        KuduScanner scanner = syncClient.newScannerBuilder(table)
                                        .lowerBoundPartitionKeyRaw(tablet.getPartition().getPartitionKeyStart())
                                        .exclusiveUpperBoundPartitionKeyRaw(tablet.getPartition().getPartitionKeyEnd())
                                        .build();
        Set<Row> tabletResults = collectRows(scanner);
        Set<Row> intersection = Sets.intersection(results, tabletResults);
        assertEquals(new HashSet<>(), intersection);
        results.addAll(tabletResults);
      }

      assertEquals(rows, results);
    }

    { // Per-tablet scan with lower & upper bounds
      Row minRow = new Row("1", "3", "5");
      Row maxRow = new Row("2", "4", "");
      PartialRow lowerBound = schema.newPartialRow();
      minRow.fillPartialRow(lowerBound);
      PartialRow upperBound = schema.newPartialRow();
      maxRow.fillPartialRow(upperBound);

      Set<Row> expected = Sets.filter(rows, Predicates.and(minRow.gtePred(), maxRow.ltPred()));
      Set<Row> results = new HashSet<>();

      for (LocatedTablet tablet : tablets) {
        KuduScanner scanner = syncClient.newScannerBuilder(table)
                                        .lowerBound(lowerBound)
                                        .exclusiveUpperBound(upperBound)
                                        .lowerBoundPartitionKeyRaw(tablet.getPartition().getPartitionKeyStart())
                                        .exclusiveUpperBoundPartitionKeyRaw(tablet.getPartition().getPartitionKeyEnd())
                                        .build();
        Set<Row> tabletResults = collectRows(scanner);
        Set<Row> intersection = Sets.intersection(results, tabletResults);
        assertEquals(new HashSet<>(), intersection);
        results.addAll(tabletResults);
      }

      assertEquals(expected, results);
    }
  }

  @Test(timeout = 100000)
  public void testHashBucketedTable() throws Exception {
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 3, 42);
    tableBuilder.setRangePartitionColumns(ImmutableList.<String>of());
    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testNonDefaultRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("c", "b"));

    PartialRow split = schema.newPartialRow();
    split.addString("c", "3");
    tableBuilder.addSplitRow(split);

    split = schema.newPartialRow();
    split.addString("c", "3");
    split.addString("b", "3");
    tableBuilder.addSplitRow(split);

    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testHashBucketedAndRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 3, 42);
    tableBuilder.setRangePartitionColumns(ImmutableList.of("c", "b"));

    PartialRow split = schema.newPartialRow();
    split.addString("c", "3");
    tableBuilder.addSplitRow(split);

    split = schema.newPartialRow();
    split.addString("c", "3");
    split.addString("b", "3");
    tableBuilder.addSplitRow(split);

    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testNonCoveredRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("a", "b", "c"));

    // Create a non covered range between (3, 5, 6) and (4, 0, 0)

    PartialRow lowerBoundA = schema.newPartialRow();
    lowerBoundA.addString("a", "0");
    lowerBoundA.addString("b", "0");
    lowerBoundA.addString("c", "0");
    PartialRow upperBoundA = schema.newPartialRow();
    upperBoundA.addString("a", "3");
    upperBoundA.addString("b", "5");
    upperBoundA.addString("b", "6");
    tableBuilder.addRangePartition(lowerBoundA, upperBoundA);

    PartialRow lowerBoundB = schema.newPartialRow();
    lowerBoundB.addString("a", "4");
    lowerBoundB.addString("b", "0");
    lowerBoundB.addString("c", "0");
    PartialRow upperBoundB = schema.newPartialRow();
    upperBoundB.addString("a", "5");
    upperBoundB.addString("b", "5");
    upperBoundB.addString("b", "6");
    tableBuilder.addRangePartition(lowerBoundB, upperBoundB);

    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testHashBucketedAndNonCoveredRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("a", "b", "c"));

    // Create a non covered range between (3, 5, 6) and (4, 0, 0)

    PartialRow lowerBoundA = schema.newPartialRow();
    lowerBoundA.addString("a", "0");
    lowerBoundA.addString("b", "0");
    lowerBoundA.addString("c", "0");
    PartialRow upperBoundA = schema.newPartialRow();
    upperBoundA.addString("a", "3");
    upperBoundA.addString("b", "5");
    upperBoundA.addString("c", "6");
    tableBuilder.addRangePartition(lowerBoundA, upperBoundA);

    PartialRow lowerBoundB = schema.newPartialRow();
    lowerBoundB.addString("a", "4");
    lowerBoundB.addString("b", "0");
    lowerBoundB.addString("c", "0");
    PartialRow upperBoundB = schema.newPartialRow();
    upperBoundB.addString("a", "5");
    upperBoundB.addString("b", "5");
    upperBoundB.addString("c", "6");
    tableBuilder.addRangePartition(lowerBoundB, upperBoundB);

    tableBuilder.addHashPartitions(ImmutableList.of("a", "b", "c"), 4);

    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testSimplePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableOptions tableBuilder =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("a", "b", "c"));

    PartialRow split = schema.newPartialRow();
    split.addString("c", "3");
    tableBuilder.addSplitRow(split);

    split = schema.newPartialRow();
    split.addString("c", "3");
    split.addString("b", "3");
    tableBuilder.addSplitRow(split);

    testPartitionSchema(tableBuilder);
  }

  @Test(timeout = 100000)
  public void testUnpartitionedTable() throws Exception {
    CreateTableOptions tableBuilder =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.<String>of());
    testPartitionSchema(tableBuilder);
  }

  public static class Row implements Comparable<Row> {
    private final String a;
    private final String b;
    private final String c;

    public Row(String a, String b, String c) {
      this.a = a;
      this.b = b;
      this.c = c;
    }

    public String getA() {
      return a;
    }

    public String getB() {
      return b;
    }

    public String getC() {
      return c;
    }

    public void fillPartialRow(PartialRow row) {
      row.addString("a", a);
      row.addString("b", b);
      row.addString("c", c);
    }

    private static Row fromResult(RowResult result) {
      return new Row(result.getString("a"),
                     result.getString("b"),
                     result.getString("c"));
    }

    public Predicate<Row> gtePred() {
      return new Predicate<Row>() {
        @Override
        public boolean apply(Row other) {
          return other.compareTo(Row.this) >= 0;
        }
      };
    }

    public Predicate<Row> ltPred() {
      return new Predicate<Row>() {
        @Override
        public boolean apply(Row other) {
          return other.compareTo(Row.this) < 0;
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Row row = (Row) o;
      return Objects.equals(a, row.a)
          && Objects.equals(b, row.b)
          && Objects.equals(c, row.c);
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b, c);
    }

    @Override
    public int compareTo(Row other) {
      return ComparisonChain.start()
                            .compare(a, other.a)
                            .compare(b, other.b)
                            .compare(c, other.c)
                            .result();
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
                                           .add("a", a)
                                           .add("b", b)
                                           .add("c", c)
                                           .toString();
    }
  }
}
