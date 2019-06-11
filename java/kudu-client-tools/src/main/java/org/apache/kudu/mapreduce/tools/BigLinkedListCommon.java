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

package org.apache.kudu.mapreduce.tools;

import java.math.BigInteger;
import java.util.Collections;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.PartialRow;

/**
 * Static constants, helper methods, and utility classes for BigLinkedList
 * implementations.
 *
 * Any definitions which must be kept in-sync between ITBLL implementations
 * should be kept here.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BigLinkedListCommon {

  public static final String TABLE_NAME_KEY = "IntegrationTestBigLinkedList.table";
  public static final String DEFAULT_TABLE_NAME = "default.IntegrationTestBigLinkedList";
  public static final String HEADS_TABLE_NAME_KEY = "IntegrationTestBigLinkedList.heads_table";
  public static final String DEFAULT_HEADS_TABLE_NAME = "default.IntegrationTestBigLinkedListHeads";

  /** Row key, two times 8 bytes. */
  public static final String COLUMN_KEY_ONE = "key1";
  public static final int COLUMN_KEY_ONE_IDX = 0;
  public static final String COLUMN_KEY_TWO = "key2";
  public static final int COLUMN_KEY_TWO_IDX = 1;

  /** Link to the id of the prev node in the linked list, two times 8 bytes. */
  public static final String COLUMN_PREV_ONE = "prev1";
  public static final int COLUMN_PREV_ONE_IDX = 2;
  public static final String COLUMN_PREV_TWO = "prev2";
  public static final int COLUMN_PREV_TWO_IDX = 3;

  /** the id of the row within the same client. */
  public static final String COLUMN_ROW_ID = "row_id";
  public static final int COLUMN_ROW_ID_IDX = 4;

  /** identifier of the mapred task that generated this row. */
  public static final String COLUMN_CLIENT = "client";
  public static final int COLUMN_CLIENT_IDX = 5;

  /** The number of times this row was updated. */
  public static final String COLUMN_UPDATE_COUNT = "update_count";
  public static final int COLUMN_UPDATE_COUNT_IDX = 6;

  public enum Counts {

    /** Nodes which are not contained in the previous pointer of any other nodes. */
    UNREFERENCED,

    /** Nodes which are referenced from another node, but do not appear in the table. */
    UNDEFINED,

    /** Nodes which have a single reference from another node. */
    REFERENCED,

    /** Nodes which have multiple references from other nodes. */
    EXTRAREFERENCES,
  }

  public static Schema getTableSchema() {
    return new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_KEY_ONE, Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_KEY_TWO, Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_PREV_ONE, Type.INT64).nullable(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_PREV_TWO, Type.INT64).nullable(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_ROW_ID, Type.INT64).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_CLIENT, Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_UPDATE_COUNT, Type.INT32).build()
    ));
  }

  public static Schema getHeadsTableSchema() {
    return new Schema(ImmutableList.of(
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_KEY_ONE, Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(COLUMN_KEY_TWO, Type.INT64).key(true).build()
    ));
  }

  public static CreateTableOptions getCreateTableOptions(Schema schema,
                                                         int numReplicas,
                                                         int rangePartitions,
                                                         int hashPartitions) {
    Preconditions.checkArgument(rangePartitions > 0);
    Preconditions.checkArgument(hashPartitions > 0);

    CreateTableOptions options = new CreateTableOptions().setNumReplicas(numReplicas);

    if (rangePartitions > 1) {
      options.setRangePartitionColumns(ImmutableList.of(COLUMN_KEY_ONE));
      BigInteger min = BigInteger.valueOf(Long.MIN_VALUE);
      BigInteger max = BigInteger.valueOf(Long.MAX_VALUE);
      BigInteger step = max.multiply(BigInteger.valueOf(2))
                           .divide(BigInteger.valueOf(rangePartitions));

      PartialRow splitRow = schema.newPartialRow();
      for (int i = 1; i < rangePartitions; i++) {
        long key = min.add(step.multiply(BigInteger.valueOf(i))).longValue();
        splitRow.addLong(COLUMN_KEY_ONE_IDX, key);
        options.addSplitRow(splitRow);
      }
    } else {
      options.setRangePartitionColumns(Collections.<String>emptyList());
    }

    if (hashPartitions > 1) {
      options.addHashPartitions(ImmutableList.of(COLUMN_KEY_ONE), hashPartitions);
    }

    return options;
  }

  /**
   * Implementation of the Xoroshiro128+ PRNG.
   * Copied under the public domain from SquidLib.
   */
  public static class Xoroshiro128PlusRandom {
    private long state0;
    private long state1;

    public Xoroshiro128PlusRandom() {
      this((long) (Math.random() * Long.MAX_VALUE));
    }

    public Xoroshiro128PlusRandom(long seed) {
      long state = seed + 0x9E3779B97F4A7C15L;
      long z = state;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      state0 = z ^ (z >>> 31);
      state += state0 + 0x9E3779B97F4A7C15L;
      z = state;
      z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
      z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
      state1 = z ^ (z >>> 31);
    }

    public long nextLong() {
      final long s0 = state0;
      long s1 = state1;
      final long result = s0 + s1;

      s1 ^= s0;
      state0 = Long.rotateLeft(s0, 55) ^ s1 ^ (s1 << 14); // a, b
      state1 = Long.rotateLeft(s1, 36); // c

      return result;
    }

    public void nextBytes(final byte[] bytes) {
      int i = bytes.length;
      int n = 0;
      while (i != 0) {
        n = Math.min(i, 8);
        for (long bits = nextLong(); n-- != 0; bits >>>= 8) {
          bytes[--i] = (byte) bits;
        }
      }
    }
  }

  /** Uninstantiable helper class. */
  private BigLinkedListCommon() {}
}