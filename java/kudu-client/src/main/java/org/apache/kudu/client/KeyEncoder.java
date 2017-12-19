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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedLongs;
import com.sangupta.murmur.Murmur2;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;
import org.apache.kudu.util.ByteVec;
import org.apache.kudu.util.DecimalUtil;
import org.apache.kudu.util.Pair;

/**
 * Utility class for encoding rows into primary and partition keys.
 */
@InterfaceAudience.Private
class KeyEncoder {

  private static final BigInteger MIN_VALUE_128 = BigInteger.valueOf(-2).pow(127);

  /** Non-constructable utility class. */
  private KeyEncoder() {
  }

  /**
   * Encodes the primary key of the row.
   *
   * @param row the row to encode
   * @return the encoded primary key of the row
   */
  public static byte[] encodePrimaryKey(final PartialRow row) {
    ByteVec buf = ByteVec.create();
    final Schema schema = row.getSchema();
    for (int columnIdx = 0; columnIdx < schema.getPrimaryKeyColumnCount(); columnIdx++) {
      final boolean isLast = columnIdx + 1 == schema.getPrimaryKeyColumnCount();
      encodeColumn(row, columnIdx, isLast, buf);
    }
    return buf.toArray();
  }

  /**
   * Returns the bucket of the row for the given hash bucket schema. All columns
   * in the hash bucket schema must be set in the row.
   *
   * @param row the row containing hash schema columns
   * @param hashSchema the hash schema
   * @return the hash bucket of the row
   */
  public static int getHashBucket(PartialRow row, HashBucketSchema hashSchema) {
    ByteVec buf = ByteVec.create();
    encodeColumns(row, hashSchema.getColumnIds(), buf);
    long hash = Murmur2.hash64(buf.data(), buf.len(), hashSchema.getSeed());
    return (int) UnsignedLongs.remainder(hash, hashSchema.getNumBuckets());
  }

  /**
   * Encodes the provided row into a partition key according to the partition schema.
   *
   * @param row the row to encode
   * @param partitionSchema the partition schema describing the table's partitioning
   * @return an encoded partition key
   */
  public static byte[] encodePartitionKey(PartialRow row, PartitionSchema partitionSchema) {
    ByteVec buf = ByteVec.create();
    if (!partitionSchema.getHashBucketSchemas().isEmpty()) {
      for (final HashBucketSchema hashSchema : partitionSchema.getHashBucketSchemas()) {
        encodeHashBucket(getHashBucket(row, hashSchema), buf);
      }
    }

    encodeColumns(row, partitionSchema.getRangeSchema().getColumns(), buf);
    return buf.toArray();
  }

  /**
   * Encodes the provided row into a range partition key.
   *
   * @param row the row to encode
   * @param rangeSchema the range partition schema
   * @return the encoded range partition key
   */
  public static byte[] encodeRangePartitionKey(PartialRow row,
                                               PartitionSchema.RangeSchema rangeSchema) {
    ByteVec buf = ByteVec.create();
    encodeColumns(row, rangeSchema.getColumns(), buf);
    return buf.toArray();
  }

  /**
   * Encodes a sequence of columns from the row.
   * @param row the row containing the columns to encode
   * @param columnIds the IDs of each column to encode
   */
  private static void encodeColumns(PartialRow row, List<Integer> columnIds, ByteVec buf) {
    for (int i = 0; i < columnIds.size(); i++) {
      boolean isLast = i + 1 == columnIds.size();
      encodeColumn(row, row.getSchema().getColumnIndex(columnIds.get(i)), isLast, buf);
    }
  }

  /**
   * Encodes a single column of a row into the output buffer.
   * @param row the row being encoded
   * @param columnIdx the column index of the column to encode
   * @param isLast whether the column is the last component of the key
   */
  private static void encodeColumn(PartialRow row,
                                   int columnIdx,
                                   boolean isLast,
                                   ByteVec buf) {
    final Schema schema = row.getSchema();
    final ColumnSchema column = schema.getColumnByIndex(columnIdx);
    if (!row.isSet(columnIdx)) {
      throw new IllegalStateException(String.format("Primary key column %s is not set",
                                                    column.getName()));
    }
    final Type type = column.getType();
    if (type == Type.STRING || type == Type.BINARY) {
      encodeBinary(row.getVarLengthData().get(columnIdx), isLast, buf);
    } else {
      encodeSignedInt(row.getRowAlloc(),
                      schema.getColumnOffset(columnIdx),
                      column.getTypeSize(),
                      buf);
    }
  }

  /**
   * Encodes a variable length binary value into the output buffer.
   * @param value the value to encode
   * @param isLast whether the value is the final component in the key
   * @param buf the output buffer
   */
  private static void encodeBinary(ByteBuffer value, boolean isLast, ByteVec buf) {
    value.reset();

    // TODO find a way to not have to read byte-by-byte that doesn't require extra copies. This is
    // especially slow now that users can pass direct byte buffers.
    while (value.hasRemaining()) {
      byte currentByte = value.get();
      buf.push(currentByte);
      if (!isLast && currentByte == 0x00) {
        // If we're a middle component of a composite key, we need to add a \x00
        // at the end in order to separate this component from the next one. However,
        // if we just did that, we'd have issues where a key that actually has
        // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
        // encode \x00 as \x00\x01. -- key_encoder.h
        buf.push((byte) 0x01);
      }
    }

    if (!isLast) {
      buf.push((byte) 0x00);
      buf.push((byte) 0x00);
    }
  }

  /**
   * Encodes a signed integer into the output buffer
   *
   * @param value an array containing the little-endian encoded integer
   * @param offset the offset of the value into the value array
   * @param len the width of the value
   * @param buf the output buffer
   */
  private static void encodeSignedInt(byte[] value,
                                      int offset,
                                      int len,
                                      ByteVec buf) {
    // Picking the first byte because big endian.
    byte lastByte = value[offset + (len - 1)];
    lastByte = Bytes.xorLeftMostBit(lastByte);
    buf.push(lastByte);
    for (int i = len - 2; i >= 0; i--) {
      buf.push(value[offset + i]);
    }
  }

  /**
   * Encodes a hash bucket into the buffer.
   * @param bucket the bucket
   * @param buf the buffer
   */
  public static void encodeHashBucket(int bucket, ByteVec buf) {
    buf.append(Ints.toByteArray(bucket));
  }

  /**
   * Decodes a primary key into a row
   *
   * @param schema the table schema
   * @param key the encoded key
   * @return the decoded primary key as a row
   */
  public static PartialRow decodePrimaryKey(Schema schema, byte[] key) {
    PartialRow row = schema.newPartialRow();
    ByteBuffer buf = ByteBuffer.wrap(key);
    buf.order(ByteOrder.BIG_ENDIAN);

    for (int idx = 0; idx < schema.getPrimaryKeyColumnCount(); idx++) {
      decodeColumn(buf, row, idx, idx + 1 == schema.getPrimaryKeyColumnCount());
    }

    if (buf.hasRemaining()) {
      throw new IllegalArgumentException("Unable to decode all primary key bytes");
    }
    return row;
  }

  /**
   * Decodes a partition key into a list of hash buckets and range key
   *
   * @param schema the schema of the table
   * @param partitionSchema the partition schema of the table
   * @param key the encoded partition key
   * @return the decoded buckets and range key
   */
  public static Pair<List<Integer>, PartialRow> decodePartitionKey(Schema schema,
                                                                   PartitionSchema partitionSchema,
                                                                   byte[] key) {
    ByteBuffer buf = ByteBuffer.wrap(key);
    buf.order(ByteOrder.BIG_ENDIAN);

    List<Integer> buckets = new ArrayList<>();

    for (HashBucketSchema hashSchema : partitionSchema.getHashBucketSchemas()) {
      if (buf.hasRemaining()) {
        buckets.add(buf.getInt());
      } else {
        buckets.add(0);
      }
    }

    return new Pair<>(buckets, decodeRangePartitionKey(schema, partitionSchema, buf));
  }

  /**
   * Decodes a range partition key into a partial row.
   *
   * @param schema the schema of the table
   * @param partitionSchema the partition schema of the table
   * @param key the encoded range partition key
   * @return the decoded range key
   */
  public static PartialRow decodeRangePartitionKey(Schema schema,
                                                   PartitionSchema partitionSchema,
                                                   byte[] key) {
    ByteBuffer buf = ByteBuffer.wrap(key);
    buf.order(ByteOrder.BIG_ENDIAN);
    return decodeRangePartitionKey(schema, partitionSchema, buf);
  }

  /**
   * Decodes a range partition key into a partial row.
   *
   * @param schema the schema of the table
   * @param partitionSchema the partition schema of the table
   * @param buf the encoded range partition key
   * @return the decoded range key
   */
  private static PartialRow decodeRangePartitionKey(Schema schema,
                                                    PartitionSchema partitionSchema,
                                                    ByteBuffer buf) {
    PartialRow row = schema.newPartialRow();
    Iterator<Integer> rangeIds = partitionSchema.getRangeSchema().getColumns().iterator();
    while (rangeIds.hasNext()) {
      int idx = schema.getColumnIndex(rangeIds.next());
      if (buf.hasRemaining()) {
        decodeColumn(buf, row, idx, !rangeIds.hasNext());
      } else {
        row.setMin(idx);
      }
    }

    if (buf.hasRemaining()) {
      throw new IllegalArgumentException("Unable to decode all partition key bytes");
    }
    return row;
  }

  /**
   * Decoded a key-encoded column into a row.
   *
   * @param buf the buffer containing the column
   * @param row the row to set the column value in
   * @param idx the index of the column to decode
   * @param isLast whether the column is the last column in the key
   */
  private static void decodeColumn(ByteBuffer buf, PartialRow row, int idx, boolean isLast) {
    Schema schema = row.getSchema();
    ColumnSchema column = schema.getColumnByIndex(idx);
    switch (column.getType()) {
      case INT8:
        row.addByte(idx, (byte) (buf.get() ^ Byte.MIN_VALUE));
        break;
      case INT16:
        row.addShort(idx, (short) (buf.getShort() ^ Short.MIN_VALUE));
        break;
      case INT32:
        row.addInt(idx, buf.getInt() ^ Integer.MIN_VALUE);
        break;
      case INT64:
      case UNIXTIME_MICROS:
        row.addLong(idx, buf.getLong() ^ Long.MIN_VALUE);
        break;
      case BINARY: {
        byte[] binary = decodeBinaryColumn(buf, isLast);
        row.addBinary(idx, binary);
        break;
      }
      case STRING: {
        byte[] binary = decodeBinaryColumn(buf, isLast);
        row.addStringUtf8(idx, binary);
        break;
      }
      case DECIMAL: {
        int scale = column.getTypeAttributes().getScale();
        int size = column.getTypeSize();
        switch (size) {
          case  DecimalUtil.DECIMAL32_SIZE:
            int intVal = buf.getInt() ^ Integer.MIN_VALUE;
            row.addDecimal(idx, BigDecimal.valueOf(intVal, scale));
            break;
          case DecimalUtil.DECIMAL64_SIZE:
            long longVal = buf.getLong() ^ Long.MIN_VALUE;
            row.addDecimal(idx, BigDecimal.valueOf(longVal, scale));
            break;
          case DecimalUtil.DECIMAL128_SIZE:
            byte[] bytes = new byte[size];
            buf.get(bytes);
            BigInteger bigIntVal = new BigInteger(bytes).xor(MIN_VALUE_128);
            row.addDecimal(idx, new BigDecimal(bigIntVal, scale));
            break;
          default:
            throw new IllegalArgumentException("Unsupported decimal type size: " + size);
        }
        break;
      }
      default:
        throw new IllegalArgumentException(String.format(
            "The column type %s is not a valid key component type",
            schema.getColumnByIndex(idx).getType()));
    }
  }

  /**
   * Decode a binary key column.
   *
   * @param key the key bytes
   * @param isLast whether the column is the final column in the key.
   * @return the binary value.
   */
  private static byte[] decodeBinaryColumn(ByteBuffer key, boolean isLast) {
    if (isLast) {
      byte[] bytes = Arrays.copyOfRange(key.array(),
                                        key.arrayOffset() + key.position(),
                                        key.arrayOffset() + key.limit());
      key.position(key.limit());
      return bytes;
    }

    // When encoding a binary column that is not the final column in the key, a
    // 0x0000 separator is used to retain lexicographic comparability. Null
    // bytes in the input are escaped as 0x0001.
    ByteVec buf = ByteVec.withCapacity(key.remaining());
    for (int i = key.position(); i < key.limit(); i++) {
      if (key.get(i) == 0) {
        switch (key.get(i + 1)) {
          case 0: {
            buf.append(key.array(),
                       key.arrayOffset() + key.position(),
                       i - key.position());
            key.position(i + 2);
            return buf.toArray();
          }
          case 1: {
            buf.append(key.array(),
                       key.arrayOffset() + key.position(),
                       i + 1 - key.position());
            i++;
            key.position(i + 1);
            break;
          }
          default: throw new IllegalArgumentException("Unexpected binary sequence");
        }
      }
    }

    buf.append(key.array(),
               key.arrayOffset() + key.position(),
               key.remaining());
    key.position(key.limit());
    return buf.toArray();
  }

  /**
   * Debug formats a partition key range.
   *
   * @param schema the table schema
   * @param partitionSchema the table partition schema
   * @param lowerBound the lower bound encoded partition key
   * @param upperBound the upper bound encoded partition key
   * @return a debug string which describes the partition key range.
   */
  public static String formatPartitionKeyRange(Schema schema,
                                               PartitionSchema partitionSchema,
                                               byte[] lowerBound,
                                               byte[] upperBound) {
    if (partitionSchema.getRangeSchema().getColumns().isEmpty() &&
        partitionSchema.getHashBucketSchemas().isEmpty()) {
      assert lowerBound.length == 0 && upperBound.length == 0;
      return "<no-partitioning>";
    }

    // Even though we parse hash buckets for the upper and lower bound partition
    // keys, we only use the lower bound set. Upper bound partition keys are
    // exclusive, so the upper bound partition key may include an incremented
    // bucket in the last position.
    Pair<List<Integer>, PartialRow> lower = decodePartitionKey(schema, partitionSchema, lowerBound);
    Pair<List<Integer>, PartialRow> upper = decodePartitionKey(schema, partitionSchema, upperBound);

    StringBuilder sb = new StringBuilder();

    List<Integer> hashBuckets = lower.getFirst();
    if (!hashBuckets.isEmpty()) {
      sb.append("hash-partition-buckets: ");
      sb.append(hashBuckets);
    }

    if (partitionSchema.getRangeSchema().getColumns().size() > 0) {
      if (!hashBuckets.isEmpty()) {
        sb.append(", ");
      }

      List<Integer> idxs = new ArrayList<>();
      for (int id : partitionSchema.getRangeSchema().getColumns()) {
        idxs.add(schema.getColumnIndex(id));
      }

      sb.append("range-partition: [");
      if (lowerBound.length > 4 * hashBuckets.size()) {
        sb.append('(');
        lower.getSecond().appendDebugString(idxs, sb);
        sb.append(')');
      } else {
        sb.append("<start>");
      }
      sb.append(", ");
      if (upperBound.length > 4 * hashBuckets.size()) {
        sb.append('(');
        upper.getSecond().appendDebugString(idxs, sb);
        sb.append(')');
      } else {
        sb.append("<end>");
      }
      sb.append(')');
    }
    return sb.toString();
  }
}
