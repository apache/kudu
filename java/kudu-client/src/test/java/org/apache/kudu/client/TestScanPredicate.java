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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.apache.kudu.test.KuduTestHarness;
import org.apache.kudu.util.DecimalUtil;

public class TestScanPredicate {

  private KuduClient client;
  private AsyncKuduClient asyncClient;

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() {
    client = harness.getClient();
    asyncClient = harness.getAsyncClient();
  }

  private Schema createTableSchema(Type type) {
    ColumnSchema key = new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).key(true).build();
    ColumnSchema val = new ColumnSchema.ColumnSchemaBuilder("value", type).nullable(true).build();
    return new Schema(ImmutableList.of(key, val));
  }

  private static CreateTableOptions createTableOptions() {
    return new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"));
  }

  private int countRows(KuduTable table, KuduPredicate... predicates) throws Exception {
    KuduScanner.KuduScannerBuilder scanBuilder =  new KuduScanner.KuduScannerBuilder(asyncClient, table);
    for (KuduPredicate predicate : predicates) {
      scanBuilder.addPredicate(predicate);
    }

    KuduScanner scanner = scanBuilder.build();

    int count = 0;
    while (scanner.hasMoreRows()) {
      count += scanner.nextRows().getNumRows();
    }
    return count;
  }

  private NavigableSet<Long> createIntegerValues(Type type) {
    NavigableSet<Long> values = new TreeSet<>();
    for (long i = -50; i < 50; i++) {
      values.add(i);
    }
    values.add(KuduPredicate.minIntValue(type));
    values.add(KuduPredicate.minIntValue(type) + 1);
    values.add(KuduPredicate.maxIntValue(type) - 1);
    values.add(KuduPredicate.maxIntValue(type));
    return values;
  }

  private List<Long> createIntegerTestValues(Type type) {
    return ImmutableList.of(
        KuduPredicate.minIntValue(type),
        KuduPredicate.minIntValue(type) + 1,
        -51L,
        50L,
        0L,
        49L,
        50L,
        KuduPredicate.maxIntValue(type) - 1,
        KuduPredicate.maxIntValue(type));
  }

  private NavigableSet<Float> createFloatValues() {
    NavigableSet<Float> values = new TreeSet<>();
    for (long i = -50; i < 50; i++) {
      values.add((float) i + (float) i / 100.0F);
    }

    values.add(Float.NEGATIVE_INFINITY);
    values.add(-Float.MAX_VALUE);
    values.add(-Float.MIN_NORMAL);
    values.add(-Float.MIN_VALUE);
    values.add(Float.MIN_VALUE);
    values.add(Float.MIN_NORMAL);
    values.add(Float.MAX_VALUE);
    values.add(Float.POSITIVE_INFINITY);

    // TODO: uncomment after fixing KUDU-1386
    // values.add(Float.NaN);
    return values;
  }

  private List<Float> createFloatTestValues() {
    return ImmutableList.of(
        Float.NEGATIVE_INFINITY,
        -Float.MAX_VALUE,
        -100.0F,
        -1.1F,
        -1.0F,
        -Float.MIN_NORMAL,
        -Float.MIN_VALUE,
        0.0F,
        Float.MIN_VALUE,
        Float.MIN_NORMAL,
        1.0F,
        1.1F,
        100.0F,
        Float.MAX_VALUE,
        Float.POSITIVE_INFINITY

        // TODO: uncomment after fixing KUDU-1386
        // Float.NaN
    );
  }

  private NavigableSet<Double> createDoubleValues() {
    NavigableSet<Double> values = new TreeSet<>();
    for (long i = -50; i < 50; i++) {
      values.add((double) i + (double) i / 100.0);
    }

    values.add(Double.NEGATIVE_INFINITY);
    values.add(-Double.MAX_VALUE);
    values.add(-Double.MIN_NORMAL);
    values.add(-Double.MIN_VALUE);
    values.add(Double.MIN_VALUE);
    values.add(Double.MIN_NORMAL);
    values.add(Double.MAX_VALUE);
    values.add(Double.POSITIVE_INFINITY);

    // TODO: uncomment after fixing KUDU-1386
    // values.add(Double.NaN);
    return values;
  }

  private List<Double> createDoubleTestValues() {
    return ImmutableList.of(
        Double.NEGATIVE_INFINITY,
        -Double.MAX_VALUE,
        -100.0,
        -1.1,
        -1.0,
        -Double.MIN_NORMAL,
        -Double.MIN_VALUE,
        0.0,
        Double.MIN_VALUE,
        Double.MIN_NORMAL,
        1.0,
        1.1,
        100.0,
        Double.MAX_VALUE,
        Double.POSITIVE_INFINITY

        // TODO: uncomment after fixing KUDU-1386
        // Double.NaN
    );
  }

  // Returns a vector of decimal(4, 2) numbers from -50.50 (inclusive) to 50.50
  // (exclusive) (100 values) and boundary values.
  private NavigableSet<BigDecimal> createDecimalValues() {
    NavigableSet<BigDecimal> values = new TreeSet<>();
    for (long i = -50; i < 50; i++) {
      values.add(BigDecimal.valueOf(i * 100 + i, 2));
    }

    values.add(BigDecimal.valueOf(-9999, 2));
    values.add(BigDecimal.valueOf(-9998, 2));
    values.add(BigDecimal.valueOf(9998, 2));
    values.add(BigDecimal.valueOf(9999, 2));

    return values;
  }

  private List<BigDecimal> createDecimalTestValues() {
    return ImmutableList.of(
        BigDecimal.valueOf(-9999, 2),
        BigDecimal.valueOf(-9998, 2),
        BigDecimal.valueOf(5100, 2),
        BigDecimal.valueOf(-5000, 2),
        BigDecimal.valueOf(0, 2),
        BigDecimal.valueOf(4900, 2),
        BigDecimal.valueOf(5000, 2),
        BigDecimal.valueOf(9998, 2),
        BigDecimal.valueOf(9999, 2)
    );
  }

  private NavigableSet<String> createStringValues() {
    return ImmutableSortedSet.of("", "\0", "\0\0", "a", "a\0", "a\0a", "aa\0");
  }

  private List<String> createStringTestValues() {
    List<String> values = new ArrayList<>(createStringValues());
    values.add("aa");
    values.add("\1");
    values.add("a\1");
    return values;
  }

  private void checkIntPredicates(KuduTable table,
                                  NavigableSet<Long> values,
                                  List<Long> testValues) throws Exception {
    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));
    for (long v : testValues) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.contains(v) ? 1 : 0, countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }

  @Test
  public void testBoolPredicates() throws Exception {
    Schema schema = createTableSchema(Type.BOOL);
    client.createTable("bool-table", schema, createTableOptions());
    KuduTable table = client.openTable("bool-table");

    NavigableSet<Boolean> values = ImmutableSortedSet.of(false, true);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (boolean value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addBoolean("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (boolean v : values) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.contains(v) ? 1 : 0, countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }
  }

  @Test
  public void testBytePredicates() throws Exception {
    Schema schema = createTableSchema(Type.INT8);
    client.createTable("byte-table", schema, createTableOptions());
    KuduTable table = client.openTable("byte-table");

    NavigableSet<Long> values = createIntegerValues(Type.INT8);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (long value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addByte("value", (byte) value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    checkIntPredicates(table, values, createIntegerTestValues(Type.INT8));
  }

  @Test
  public void testShortPredicates() throws Exception {
    Schema schema = createTableSchema(Type.INT16);
    client.createTable("short-table", schema,
                           new CreateTableOptions().setRangePartitionColumns(
                               ImmutableList.<String>of()));
    KuduTable table = client.openTable("short-table");

    NavigableSet<Long> values = createIntegerValues(Type.INT16);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (long value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addShort("value", (short) value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    checkIntPredicates(table, values, createIntegerTestValues(Type.INT16));
  }

  @Test
  public void testIntPredicates() throws Exception {
    Schema schema = createTableSchema(Type.INT32);
    client.createTable("int-table", schema, createTableOptions());
    KuduTable table = client.openTable("int-table");

    NavigableSet<Long> values = createIntegerValues(Type.INT32);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (long value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addInt("value", (int) value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    checkIntPredicates(table, values, createIntegerTestValues(Type.INT32));
  }

  @Test
  public void testLongPredicates() throws Exception {
    Schema schema = createTableSchema(Type.INT64);
    client.createTable("long-table", schema,
                           new CreateTableOptions().setRangePartitionColumns(
                               ImmutableList.<String>of()));
    KuduTable table = client.openTable("long-table");

    NavigableSet<Long> values = createIntegerValues(Type.INT64);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (long value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addLong("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    checkIntPredicates(table, values, createIntegerTestValues(Type.INT64));
  }

  @Test
  public void testTimestampPredicate() throws Exception {
    Schema schema = createTableSchema(Type.INT64);
    client.createTable("timestamp-table", schema, createTableOptions());
    KuduTable table = client.openTable("timestamp-table");

    NavigableSet<Long> values = createIntegerValues(Type.INT64);
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (long value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addLong("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    checkIntPredicates(table, values, createIntegerTestValues(Type.INT64));
  }

  @Test
  public void testFloatPredicates() throws Exception {
    Schema schema = createTableSchema(Type.FLOAT);
    client.createTable("float-table", schema, createTableOptions());
    KuduTable table = client.openTable("float-table");

    NavigableSet<Float> values = createFloatValues();
    List<Float> testValues = createFloatTestValues();
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (float value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addFloat("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (float v : testValues) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.subSet(v, true, v, true).size(), countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }

  @Test
  public void testDoublePredicates() throws Exception {
    Schema schema = createTableSchema(Type.DOUBLE);
    client.createTable("double-table", schema, createTableOptions());
    KuduTable table = client.openTable("double-table");

    NavigableSet<Double> values = createDoubleValues();
    List<Double> testValues = createDoubleTestValues();
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (double value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addDouble("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (double v : testValues) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.subSet(v, true, v, true).size(), countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }

  @Test
  public void testDecimalPredicates() throws Exception {
    ColumnSchema key = new ColumnSchema.ColumnSchemaBuilder("key", Type.INT64).key(true).build();
    ColumnSchema val = new ColumnSchema.ColumnSchemaBuilder("value", Type.DECIMAL)
        .typeAttributes(DecimalUtil.typeAttributes(4, 2)).nullable(true).build();
    Schema schema = new Schema(ImmutableList.of(key, val));

    client.createTable("decimal-table", schema, createTableOptions());
    KuduTable table = client.openTable("decimal-table");

    NavigableSet<BigDecimal> values = createDecimalValues();
    List<BigDecimal> testValues = createDecimalTestValues();
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (BigDecimal value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addDecimal("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (BigDecimal v : testValues) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.subSet(v, true, v, true).size(), countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }

  @Test
  public void testStringPredicates() throws Exception {
    Schema schema = createTableSchema(Type.STRING);
    client.createTable("string-table", schema, createTableOptions());
    KuduTable table = client.openTable("string-table");

    NavigableSet<String> values = createStringValues();
    List<String> testValues = createStringTestValues();
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (String value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addString("value", value);
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (String v : testValues) {
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.subSet(v, true, v, true).size(), countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(v).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(v, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(v, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(v).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }

  @Test
  public void testBinaryPredicates() throws Exception {
    Schema schema = createTableSchema(Type.BINARY);
    client.createTable("binary-table", schema, createTableOptions());
    KuduTable table = client.openTable("binary-table");

    NavigableSet<String> values = createStringValues();
    List<String> testValues = createStringTestValues();
    KuduSession session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    long i = 0;
    for (String value : values) {
      Insert insert = table.newInsert();
      insert.getRow().addLong("key", i++);
      insert.getRow().addBinary("value", Bytes.fromString(value));
      session.apply(insert);
    }
    Insert nullInsert = table.newInsert();
    nullInsert.getRow().addLong("key", i++);
    nullInsert.getRow().setNull("value");
    session.apply(nullInsert);
    session.flush();

    ColumnSchema col = table.getSchema().getColumn("value");
    Assert.assertEquals(values.size() + 1, countRows(table));

    for (String s : testValues) {
      byte[] v = Bytes.fromString(s);
      // value = v
      KuduPredicate equal = KuduPredicate.newComparisonPredicate(col, ComparisonOp.EQUAL, v);
      Assert.assertEquals(values.subSet(s, true, s, true).size(), countRows(table, equal));

      // value >= v
      KuduPredicate greaterEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER_EQUAL, v);
      Assert.assertEquals(values.tailSet(s).size(), countRows(table, greaterEqual));

      // value <= v
      KuduPredicate lessEqual =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS_EQUAL, v);
      Assert.assertEquals(values.headSet(s, true).size(), countRows(table, lessEqual));

      // value > v
      KuduPredicate greater =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.GREATER, v);
      Assert.assertEquals(values.tailSet(s, false).size(), countRows(table, greater));

      // value < v
      KuduPredicate less =
          KuduPredicate.newComparisonPredicate(col, ComparisonOp.LESS, v);
      Assert.assertEquals(values.headSet(s).size(), countRows(table, less));
    }

    KuduPredicate isNotNull = KuduPredicate.newIsNotNullPredicate(col);
    Assert.assertEquals(values.size(), countRows(table, isNotNull));

    KuduPredicate isNull = KuduPredicate.newIsNullPredicate(col);
    Assert.assertEquals(1, countRows(table, isNull));
  }
}
