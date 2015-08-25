// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestFlexiblePartitioning extends BaseKuduTest {
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

  private static Set<Row> rows() throws Exception {
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

  private void insertRows(KuduTable table, Set<Row> rows) throws Exception {
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

  private Set<Row> collectRows(KuduScanner scanner) throws Exception {
    Set<Row> rows = new HashSet<>();
    while (scanner.hasMoreRows()) {
      for (RowResult result : scanner.nextRows()) {
        rows.add(Row.fromResult(result));
      }
    }
    return rows;
  }

  private void testPartitionSchema(CreateTableBuilder tableBuilder) throws Exception {
    Schema schema = createSchema();

    syncClient.createTable(tableName, schema, tableBuilder);

    KuduTable table = syncClient.openTable(tableName);
    assertFalse(table.getPartitionSchema().isSimpleRangePartitioning());

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
    }
  }

  @Test
  public void testHashBucketedTable() throws Exception {
    CreateTableBuilder tableBuilder = new CreateTableBuilder();
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 3, 42);
    tableBuilder.setRangePartitionColumns(ImmutableList.<String>of());
    testPartitionSchema(tableBuilder);
  }

  @Test
  public void testNonDefaultRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableBuilder tableBuilder = new CreateTableBuilder();
    tableBuilder.setRangePartitionColumns(ImmutableList.of("c", "b"));

    PartialRow split = schema.newPartialRow();
    split.addString("c", "3");
    tableBuilder.addSplitRow(split);

    split.addString("c", "3");
    split.addString("b", "3");
    tableBuilder.addSplitRow(split);

    testPartitionSchema(tableBuilder);
  }

  @Test
  public void testHashBucketedAndRangePartitionedTable() throws Exception {
    Schema schema = createSchema();
    CreateTableBuilder tableBuilder = new CreateTableBuilder();
    tableBuilder.addHashPartitions(ImmutableList.of("a"), 3);
    tableBuilder.addHashPartitions(ImmutableList.of("b", "c"), 3, 42);
    tableBuilder.setRangePartitionColumns(ImmutableList.of("c", "b"));

    PartialRow split = schema.newPartialRow();
    split.addString("c", "3");
    tableBuilder.addSplitRow(split);

    split.addString("c", "3");
    split.addString("b", "3");
    tableBuilder.addSplitRow(split);

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
