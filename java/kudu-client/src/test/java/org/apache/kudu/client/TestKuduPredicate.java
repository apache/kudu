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

import static org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS;
import static org.apache.kudu.client.KuduPredicate.ComparisonOp.LESS_EQUAL;
import static org.apache.kudu.client.KuduPredicate.PredicateType.RANGE;

import java.util.Arrays;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

public class TestKuduPredicate {

  private static final ColumnSchema boolCol =
      new ColumnSchema.ColumnSchemaBuilder("bool", Type.BOOL).build();

  private static final ColumnSchema byteCol =
      new ColumnSchema.ColumnSchemaBuilder("byte", Type.INT8).build();

  private static final ColumnSchema shortCol =
      new ColumnSchema.ColumnSchemaBuilder("short", Type.INT16).build();

  private static final ColumnSchema intCol =
      new ColumnSchema.ColumnSchemaBuilder("int", Type.INT32).build();

  private static final ColumnSchema longCol =
      new ColumnSchema.ColumnSchemaBuilder("long", Type.INT64).build();

  private static final ColumnSchema floatCol =
      new ColumnSchema.ColumnSchemaBuilder("float", Type.FLOAT).build();

  private static final ColumnSchema doubleCol =
      new ColumnSchema.ColumnSchemaBuilder("double", Type.DOUBLE).build();

  private static final ColumnSchema stringCol =
      new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build();

  private static final ColumnSchema binaryCol =
      new ColumnSchema.ColumnSchemaBuilder("binary", Type.BINARY).build();

  private static KuduPredicate intRange(int lower, int upper) {
    Preconditions.checkArgument(lower < upper);
    return new KuduPredicate(RANGE, intCol, Bytes.fromInt(lower), Bytes.fromInt(upper));
  }

  private static KuduPredicate intInList(Integer... values) {
    return KuduPredicate.newInListPredicate(intCol, Arrays.asList(values));
  }

  private static KuduPredicate boolInList(Boolean... values) {
    return KuduPredicate.newInListPredicate(boolCol, Arrays.asList(values));
  }

  private static KuduPredicate stringInList(String... values) {
    return KuduPredicate.newInListPredicate(stringCol, Arrays.asList(values));
  }

  private void testMerge(KuduPredicate a,
                         KuduPredicate b,
                         KuduPredicate expected) {

    Assert.assertEquals(expected, a.merge(b));
    Assert.assertEquals(expected, b.merge(a));
  }

  /**
   * Tests merges on all types of integer predicates.
   */
  @Test
  public void testMergeInt() {

    // Equality + Equality

    // |
    // |
    // =
    // |
    testMerge(KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0));
    // |
    //  |
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 1),
              KuduPredicate.none(intCol));

    // Range + Equality

    // [-------->
    //      |
    // =
    //      |
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10));

    //    [-------->
    //  |
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 10),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0),
              KuduPredicate.none(intCol));

    // <--------)
    //      |
    // =
    //      |
    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 10),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 5));

    // <--------)
    //            |
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10),
              KuduPredicate.none(intCol));

    // Unbounded Range + Unbounded Range

    // [--------> AND
    // [-------->
    // =
    // [-------->

    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0));

    // [--------> AND
    //    [----->
    // =
    //    [----->
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5));

    // <--------) AND
    // <--------)
    // =
    // <--------)

    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 0),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 0),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 0));

    // <--------) AND
    // <----)
    // =
    // <----)

    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 0),
              KuduPredicate.newComparisonPredicate(intCol, LESS, -10),
              KuduPredicate.newComparisonPredicate(intCol, LESS, -10));

    //    [--------> AND
    // <-------)
    // =
    //    [----)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 10),
              intRange(0, 10));

    //     [-----> AND
    // <----)
    // =
    //     |
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 6),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 5));

    //     [-----> AND
    // <---)
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 5),
              KuduPredicate.none(intCol));

    //       [-----> AND
    // <---)
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 3),
              KuduPredicate.none(intCol));

    // Range + Range

    // [--------) AND
    // [--------)
    // =
    // [--------)

    testMerge(intRange(0, 10),
              intRange(0, 10),
              intRange(0, 10));

    // [--------) AND
    // [----)
    // =
    // [----)
    testMerge(intRange(0, 10),
              intRange(0, 5),
              intRange(0, 5));

    // [--------) AND
    //   [----)
    // =
    //   [----)
    testMerge(intRange(0, 10),
              intRange(3, 8),
              intRange(3, 8));

    // [-----) AND
    //   [------)
    // =
    //   [---)
    testMerge(intRange(0, 8),
              intRange(3, 10),
              intRange(3, 8));
    // [--) AND
    //    [---)
    // =
    // None
    testMerge(intRange(0, 5),
              intRange(5, 10),
              KuduPredicate.none(intCol));

    // [--) AND
    //       [---)
    // =
    // None
    testMerge(intRange(0, 3),
              intRange(5, 10),
              KuduPredicate.none(intCol));

    // Lower Bound + Range

    // [------------>
    //       [---)
    // =
    //       [---)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              intRange(5, 10),
              intRange(5, 10));

    // [------------>
    // [--------)
    // =
    // [--------)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              intRange(5, 10),
              intRange(5, 10));

    //      [------------>
    // [--------)
    // =
    //      [---)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 5),
              intRange(0, 10),
              intRange(5, 10));

    //          [------->
    // [-----)
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 10),
              intRange(0, 5),
              KuduPredicate.none(intCol));

    // Upper Bound + Range

    // <------------)
    //       [---)
    // =
    //       [---)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 10),
              intRange(3, 8),
              intRange(3, 8));

    // <------------)
    //     [--------)
    // =
    //     [--------)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 10),
              intRange(5, 10),
              intRange(5, 10));


    // <------------)
    //         [--------)
    // =
    //         [----)
    testMerge(KuduPredicate.newComparisonPredicate(intCol, LESS, 5),
              intRange(0, 10),
              intRange(0, 5));

    // Range + Equality

    //   [---) AND
    // |
    // =
    // None
    testMerge(intRange(3, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 1),
              KuduPredicate.none(intCol));

    // [---) AND
    // |
    // =
    // |
    testMerge(intRange(0, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0));

    // [---) AND
    //   |
    // =
    //   |
    testMerge(intRange(0, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 3),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 3));

    // [---) AND
    //     |
    // =
    // None
    testMerge(intRange(0, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 5),
              KuduPredicate.none(intCol));

    // [---) AND
    //       |
    // =
    // None
    testMerge(intRange(0, 5),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 7),
              KuduPredicate.none(intCol));

    // IN list + IN list

    // | | |
    //   | | |
    testMerge(intInList(0, 10, 20),
              intInList(20, 10, 20, 30),
              intInList(10, 20));

    // |   |
    //    | |
    testMerge(intInList(0, 20),
              intInList(15, 30),
              KuduPredicate.none(intCol));

    // IN list + NOT NULL
    testMerge(intInList(10),
              KuduPredicate.newIsNotNullPredicate(intCol),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10));

    testMerge(intInList(10, -100),
              KuduPredicate.newIsNotNullPredicate(intCol),
              intInList(-100, 10));

    // IN list + Equality

    // | | |
    //   |
    // =
    //   |
    testMerge(intInList(0, 10, 20),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 10));

    // | | |
    //       |
    // =
    // none
    testMerge(intInList(0, 10, 20),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 30),
              KuduPredicate.none(intCol));

    // IN list + Range

    // | | | | |
    //   [---)
    // =
    //   | |
    testMerge(intInList(0, 10, 20, 30, 40),
              intRange(10, 30),
              intInList(10, 20));

    // | |   | |
    //    [--)
    // =
    // none
    testMerge(intInList(0, 10, 20, 30),
              intRange(25, 30),
              KuduPredicate.none(intCol));

    // | | | |
    //    [------>
    // =
    //   | |
    testMerge(intInList(0, 10, 20, 30),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 15),
              intInList(20, 30));

    // | | |
    //    [------>
    // =
    //     |
    testMerge(intInList(0, 10, 20),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 15),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 20));

    // | |
    //    [------>
    // =
    // none
    testMerge(intInList(0, 10),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 15),
              KuduPredicate.none(intCol));

    // | | | |
    // <--)
    // =
    // | |
    testMerge(intInList(0, 10, 20, 30),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 15),
              intInList(0, 10));

    // |  | |
    // <--)
    // =
    // |
    testMerge(intInList(0, 10, 20),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 10),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 0));

    //      | |
    // <--)
    // =
    // none
    testMerge(intInList(10, 20),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 5),
              KuduPredicate.none(intCol));

    // None

    // None AND
    // [---->
    // =
    // None
    testMerge(KuduPredicate.none(intCol),
              KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 0),
              KuduPredicate.none(intCol));
    // None AND
    // <----)
    // =
    // None
    testMerge(KuduPredicate.none(intCol),
              KuduPredicate.newComparisonPredicate(intCol, LESS, 0),
              KuduPredicate.none(intCol));

    // None AND
    // [----)
    // =
    // None
    testMerge(KuduPredicate.none(intCol),
              intRange(3, 7),
              KuduPredicate.none(intCol));

    // None AND
    //  |
    // =
    // None
    testMerge(KuduPredicate.none(intCol),
              KuduPredicate.newComparisonPredicate(intCol, EQUAL, 5),
              KuduPredicate.none(intCol));

    // None AND
    // None
    // =
    // None
    testMerge(KuduPredicate.none(intCol),
              KuduPredicate.none(intCol),
              KuduPredicate.none(intCol));
  }

  /**
   * Tests tricky merges on a var length type.
   */
  @Test
  public void testMergeString() {

    //         [----->
    //  <-----)
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "b\0"),
              KuduPredicate.newComparisonPredicate(stringCol, LESS, "b"),
              KuduPredicate.none(stringCol));

    //        [----->
    //  <-----)
    // =
    // None
    testMerge(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "b"),
              KuduPredicate.newComparisonPredicate(stringCol, LESS, "b"),
              KuduPredicate.none(stringCol));

    //       [----->
    //  <----)
    // =
    //       |
    testMerge(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "b"),
              KuduPredicate.newComparisonPredicate(stringCol, LESS, "b\0"),
              KuduPredicate.newComparisonPredicate(stringCol, EQUAL, "b"));

    //     [----->
    //  <-----)
    // =
    //     [--)
    testMerge(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "a"),
              KuduPredicate.newComparisonPredicate(stringCol, LESS, "a\0\0"),
              new KuduPredicate(RANGE, stringCol,
                                Bytes.fromString("a"), Bytes.fromString("a\0\0")));

    //     [----->
    //   | | | |
    // =
    //     [--)
    testMerge(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "a"),
              stringInList("a", "c", "b", ""),
              stringInList("a", "b", "c"));

    //   IS NOT NULL
    //   | | | |
    // =
    //     [--)
    testMerge(KuduPredicate.newIsNotNullPredicate(stringCol),
              stringInList("a", "c", "b", ""),
              stringInList("", "a", "b", "c"));
  }

  @Test
  public void testBoolean() {

    // b >= false
    Assert.assertEquals(KuduPredicate.newIsNotNullPredicate(boolCol),
                        KuduPredicate.newComparisonPredicate(boolCol, GREATER_EQUAL, false));
    // b > false
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true),
                        KuduPredicate.newComparisonPredicate(boolCol, GREATER, false));
    // b = false
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, false),
                        KuduPredicate.newComparisonPredicate(boolCol, EQUAL, false));
    // b < false
    Assert.assertEquals(KuduPredicate.none(boolCol),
                        KuduPredicate.newComparisonPredicate(boolCol, LESS, false));
    // b <= false
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, false),
                        KuduPredicate.newComparisonPredicate(boolCol, LESS_EQUAL, false));

    // b >= true
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true),
                        KuduPredicate.newComparisonPredicate(boolCol, GREATER_EQUAL, true));
    // b > true
    Assert.assertEquals(KuduPredicate.none(boolCol),
                        KuduPredicate.newComparisonPredicate(boolCol, GREATER, true));
    // b = true
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true),
                        KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true));
    // b < true
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, false),
                        KuduPredicate.newComparisonPredicate(boolCol, LESS, true));
    // b <= true
    Assert.assertEquals(KuduPredicate.newIsNotNullPredicate(boolCol),
                        KuduPredicate.newComparisonPredicate(boolCol, LESS_EQUAL, true));

    // b IN ()
    Assert.assertEquals(KuduPredicate.none(boolCol), boolInList());

    // b IN (true)
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true),
                        boolInList(true, true, true));

    // b IN (false)
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(boolCol, EQUAL, false),
                        boolInList(false));

    // b IN (false, true)
    Assert.assertEquals(KuduPredicate.newIsNotNullPredicate(boolCol),
                        boolInList(false, true, false, true));
  }

  /**
   * Tests basic predicate merges across all types.
   */
  @Test
  public void testAllTypesMerge() {

    testMerge(KuduPredicate.newComparisonPredicate(boolCol, GREATER_EQUAL, false),
              KuduPredicate.newComparisonPredicate(boolCol, LESS, true),
              new KuduPredicate(KuduPredicate.PredicateType.EQUALITY,
                                boolCol,
                                Bytes.fromBoolean(false),
                                null));

    testMerge(KuduPredicate.newComparisonPredicate(boolCol, GREATER_EQUAL, false),
              KuduPredicate.newComparisonPredicate(boolCol, LESS_EQUAL, true),
              KuduPredicate.newIsNotNullPredicate(boolCol));

    testMerge(KuduPredicate.newComparisonPredicate(byteCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(byteCol, LESS, 10),
              new KuduPredicate(RANGE,
                                byteCol,
                                new byte[] { (byte) 0 },
                                new byte[] { (byte) 10 }));

    testMerge(KuduPredicate.newInListPredicate(byteCol, ImmutableList.of((byte) 12, (byte) 14, (byte) 16, (byte) 18)),
              KuduPredicate.newInListPredicate(byteCol, ImmutableList.of((byte) 14, (byte) 18, (byte) 20)),
              KuduPredicate.newInListPredicate(byteCol, ImmutableList.of((byte) 14, (byte) 18)));

    testMerge(KuduPredicate.newComparisonPredicate(shortCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(shortCol, LESS, 10),
              new KuduPredicate(RANGE,
                                shortCol,
                                Bytes.fromShort((short) 0),
                                Bytes.fromShort((short) 10)));

    testMerge(KuduPredicate.newInListPredicate(shortCol, ImmutableList.of((short) 12, (short) 14, (short) 16, (short) 18)),
              KuduPredicate.newInListPredicate(shortCol, ImmutableList.of((short) 14, (short) 18, (short) 20)),
              KuduPredicate.newInListPredicate(shortCol, ImmutableList.of((short) 14, (short) 18)));

    testMerge(KuduPredicate.newComparisonPredicate(longCol, GREATER_EQUAL, 0),
              KuduPredicate.newComparisonPredicate(longCol, LESS, 10),
              new KuduPredicate(RANGE,
                                longCol,
                                Bytes.fromLong(0),
                                Bytes.fromLong(10)));

    testMerge(KuduPredicate.newInListPredicate(longCol, ImmutableList.of(12L, 14L, 16L, 18L)),
              KuduPredicate.newInListPredicate(longCol, ImmutableList.of(14L, 18L, 20L)),
              KuduPredicate.newInListPredicate(longCol, ImmutableList.of(14L, 18L)));

    testMerge(KuduPredicate.newComparisonPredicate(floatCol, GREATER_EQUAL, 123.45f),
              KuduPredicate.newComparisonPredicate(floatCol, LESS, 678.90f),
              new KuduPredicate(RANGE,
                                floatCol,
                                Bytes.fromFloat(123.45f),
                                Bytes.fromFloat(678.90f)));

    testMerge(KuduPredicate.newInListPredicate(floatCol, ImmutableList.of(12f, 14f, 16f, 18f)),
              KuduPredicate.newInListPredicate(floatCol, ImmutableList.of(14f, 18f, 20f)),
              KuduPredicate.newInListPredicate(floatCol, ImmutableList.of(14f, 18f)));

    testMerge(KuduPredicate.newComparisonPredicate(doubleCol, GREATER_EQUAL, 123.45),
              KuduPredicate.newComparisonPredicate(doubleCol, LESS, 678.90),
              new KuduPredicate(RANGE,
                                doubleCol,
                                Bytes.fromDouble(123.45),
                                Bytes.fromDouble(678.90)));

    testMerge(KuduPredicate.newInListPredicate(doubleCol, ImmutableList.of(12d, 14d, 16d, 18d)),
              KuduPredicate.newInListPredicate(doubleCol, ImmutableList.of(14d, 18d, 20d)),
              KuduPredicate.newInListPredicate(doubleCol, ImmutableList.of(14d, 18d)));

    testMerge(KuduPredicate.newComparisonPredicate(binaryCol, GREATER_EQUAL,
                                                   new byte[] { 0, 1, 2, 3, 4, 5, 6 }),
              KuduPredicate.newComparisonPredicate(binaryCol, LESS, new byte[] { 10 }),
              new KuduPredicate(RANGE,
                                binaryCol,
                                new byte[] { 0, 1, 2, 3, 4, 5, 6 },
                                new byte[] { 10 }));

    testMerge(KuduPredicate.newInListPredicate(binaryCol, ImmutableList.of("a".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes())),
              KuduPredicate.newInListPredicate(binaryCol, ImmutableList.of("b".getBytes(), "d".getBytes(), "e".getBytes())),
              KuduPredicate.newInListPredicate(binaryCol, ImmutableList.of("b".getBytes(), "d".getBytes())));
  }

  @Test
  public void testLessEqual() {
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(byteCol, LESS_EQUAL, 10),
                        KuduPredicate.newComparisonPredicate(byteCol, LESS, 11));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(shortCol, LESS_EQUAL, 10),
                        KuduPredicate.newComparisonPredicate(shortCol, LESS, 11));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(intCol, LESS_EQUAL, 10),
                        KuduPredicate.newComparisonPredicate(intCol, LESS, 11));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(longCol, LESS_EQUAL, 10),
                        KuduPredicate.newComparisonPredicate(longCol, LESS, 11));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(floatCol, LESS_EQUAL, 12.345f),
                        KuduPredicate.newComparisonPredicate(floatCol, LESS, Math.nextAfter(12.345f, Float.POSITIVE_INFINITY)));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(doubleCol, LESS_EQUAL, 12.345),
                        KuduPredicate.newComparisonPredicate(doubleCol, LESS, Math.nextAfter(12.345, Float.POSITIVE_INFINITY)));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(stringCol, LESS_EQUAL, "a"),
                        KuduPredicate.newComparisonPredicate(stringCol, LESS, "a\0"));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(binaryCol, LESS_EQUAL, new byte[] { (byte) 10 }),
                        KuduPredicate.newComparisonPredicate(binaryCol, LESS, new byte[] { (byte) 10, (byte) 0 }));

    Assert.assertEquals(KuduPredicate.newComparisonPredicate(byteCol, LESS_EQUAL, Byte.MAX_VALUE),
                        KuduPredicate.newIsNotNullPredicate(byteCol));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(shortCol, LESS_EQUAL, Short.MAX_VALUE),
                        KuduPredicate.newIsNotNullPredicate(shortCol));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(intCol, LESS_EQUAL, Integer.MAX_VALUE),
                        KuduPredicate.newIsNotNullPredicate(intCol));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(longCol, LESS_EQUAL, Long.MAX_VALUE),
                        KuduPredicate.newIsNotNullPredicate(longCol));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(floatCol, LESS_EQUAL, Float.MAX_VALUE),
                        KuduPredicate.newComparisonPredicate(floatCol, LESS, Float.POSITIVE_INFINITY));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(floatCol, LESS_EQUAL, Float.POSITIVE_INFINITY),
                        KuduPredicate.newIsNotNullPredicate(floatCol));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(doubleCol, LESS_EQUAL, Double.MAX_VALUE),
                        KuduPredicate.newComparisonPredicate(doubleCol, LESS, Double.POSITIVE_INFINITY));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(doubleCol, LESS_EQUAL, Double.POSITIVE_INFINITY),
                        KuduPredicate.newIsNotNullPredicate(doubleCol));
  }

  @Test
  public void testGreater() {
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(byteCol, GREATER_EQUAL, 11),
                        KuduPredicate.newComparisonPredicate(byteCol, GREATER, 10));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(shortCol, GREATER_EQUAL, 11),
                        KuduPredicate.newComparisonPredicate(shortCol, GREATER, 10));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(intCol, GREATER_EQUAL, 11),
                        KuduPredicate.newComparisonPredicate(intCol, GREATER, 10));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(longCol, GREATER_EQUAL, 11),
                        KuduPredicate.newComparisonPredicate(longCol, GREATER, 10));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(floatCol, GREATER_EQUAL, Math.nextAfter(12.345f, Float.MAX_VALUE)),
                        KuduPredicate.newComparisonPredicate(floatCol, GREATER, 12.345f));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(doubleCol, GREATER_EQUAL, Math.nextAfter(12.345, Float.MAX_VALUE)),
                        KuduPredicate.newComparisonPredicate(doubleCol, GREATER, 12.345));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(stringCol, GREATER_EQUAL, "a\0"),
                        KuduPredicate.newComparisonPredicate(stringCol, GREATER, "a"));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(binaryCol, GREATER_EQUAL, new byte[] { (byte) 10, (byte) 0 }),
                        KuduPredicate.newComparisonPredicate(binaryCol, GREATER, new byte[] { (byte) 10 }));

    Assert.assertEquals(KuduPredicate.none(byteCol),
                        KuduPredicate.newComparisonPredicate(byteCol, GREATER, Byte.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.none(shortCol),
                        KuduPredicate.newComparisonPredicate(shortCol, GREATER, Short.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.none(intCol),
                        KuduPredicate.newComparisonPredicate(intCol, GREATER, Integer.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.none(longCol),
                        KuduPredicate.newComparisonPredicate(longCol, GREATER, Long.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(floatCol, GREATER_EQUAL, Float.POSITIVE_INFINITY),
                        KuduPredicate.newComparisonPredicate(floatCol, GREATER, Float.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.none(floatCol),
                        KuduPredicate.newComparisonPredicate(floatCol, GREATER, Float.POSITIVE_INFINITY));
    Assert.assertEquals(KuduPredicate.newComparisonPredicate(doubleCol, GREATER_EQUAL, Double.POSITIVE_INFINITY),
                        KuduPredicate.newComparisonPredicate(doubleCol, GREATER, Double.MAX_VALUE));
    Assert.assertEquals(KuduPredicate.none(doubleCol),
                        KuduPredicate.newComparisonPredicate(doubleCol, GREATER, Double.POSITIVE_INFINITY));
  }

  @Test
  public void testToString() {
    Assert.assertEquals("`bool` = true",
                        KuduPredicate.newComparisonPredicate(boolCol, EQUAL, true).toString());
    Assert.assertEquals("`byte` = 11",
                        KuduPredicate.newComparisonPredicate(byteCol, EQUAL, 11).toString());
    Assert.assertEquals("`short` = 11",
                        KuduPredicate.newComparisonPredicate(shortCol, EQUAL, 11).toString());
    Assert.assertEquals("`int` = -123",
                        KuduPredicate.newComparisonPredicate(intCol, EQUAL, -123).toString());
    Assert.assertEquals("`long` = 5454",
                        KuduPredicate.newComparisonPredicate(longCol, EQUAL, 5454).toString());
    Assert.assertEquals("`float` = 123.456",
                        KuduPredicate.newComparisonPredicate(floatCol, EQUAL, 123.456f).toString());
    Assert.assertEquals("`double` = 123.456",
                        KuduPredicate.newComparisonPredicate(doubleCol, EQUAL, 123.456).toString());
    Assert.assertEquals("`string` = \"my string\"",
                        KuduPredicate.newComparisonPredicate(stringCol, EQUAL, "my string").toString());
    Assert.assertEquals("`binary` = 0xAB01CD", KuduPredicate.newComparisonPredicate(
        binaryCol, EQUAL, new byte[] { (byte) 0xAB, (byte) 0x01, (byte) 0xCD }).toString());
    Assert.assertEquals("`int` IN (-10, 0, 10)",
                        intInList(10, 0, -10).toString());
    Assert.assertEquals("`string` IS NOT NULL",
                        KuduPredicate.newIsNotNullPredicate(stringCol).toString());

    Assert.assertEquals("`bool` = true", KuduPredicate.newInListPredicate(
        boolCol, ImmutableList.of(true)).toString());
    Assert.assertEquals("`bool` = false", KuduPredicate.newInListPredicate(
        boolCol, ImmutableList.of(false)).toString());
    Assert.assertEquals("`bool` IS NOT NULL", KuduPredicate.newInListPredicate(
        boolCol, ImmutableList.of(false, true, true)).toString());
    Assert.assertEquals("`byte` IN (1, 10, 100)", KuduPredicate.newInListPredicate(
        byteCol, ImmutableList.of((byte) 1, (byte) 10, (byte) 100)).toString());
    Assert.assertEquals("`short` IN (1, 10, 100)", KuduPredicate.newInListPredicate(
        shortCol, ImmutableList.of((short) 1, (short) 100, (short) 10)).toString());
    Assert.assertEquals("`int` IN (1, 10, 100)", KuduPredicate.newInListPredicate(
        intCol, ImmutableList.of(1, 100, 10)).toString());
    Assert.assertEquals("`long` IN (1, 10, 100)", KuduPredicate.newInListPredicate(
        longCol, ImmutableList.of(1L, 100L, 10L)).toString());
    Assert.assertEquals("`float` IN (78.9, 123.456)", KuduPredicate.newInListPredicate(
        floatCol, ImmutableList.of(123.456f, 78.9f)).toString());
    Assert.assertEquals("`double` IN (78.9, 123.456)", KuduPredicate.newInListPredicate(
        doubleCol, ImmutableList.of(123.456d, 78.9d)).toString());
    Assert.assertEquals("`string` IN (\"a\", \"my string\")",
                        KuduPredicate.newInListPredicate(stringCol, ImmutableList.of("my string", "a")).toString());
    Assert.assertEquals("`binary` IN (0x00, 0xAB01CD)", KuduPredicate.newInListPredicate(
        binaryCol, ImmutableList.of(new byte[] { (byte) 0xAB, (byte) 0x01, (byte) 0xCD },
                                    new byte[] { (byte) 0x00 })).toString());
  }
}
