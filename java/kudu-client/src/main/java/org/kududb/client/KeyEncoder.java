// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.primitives.UnsignedLongs;
import com.sangupta.murmur.Murmur2;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.client.PartitionSchema.HashBucketSchema;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

/**
 * Utility class for encoding rows into primary and partition keys.
 */
@InterfaceAudience.Private
class KeyEncoder {

  private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

  /**
   * Encodes the primary key of the row.
   *
   * @param row the row to encode
   * @return the encoded primary key of the row
   */
  public byte[] encodePrimaryKey(final PartialRow row) {
    buf.reset();

    final Schema schema = row.getSchema();
    for (int columnIdx = 0; columnIdx < schema.getPrimaryKeyColumnCount(); columnIdx++) {
      final boolean isLast = columnIdx + 1 == schema.getPrimaryKeyColumnCount();
      encodeColumn(row, columnIdx, isLast);
    }
    return extractByteArray();
  }

  /**
   * Encodes the provided row into a partition key according to the partition schema.
   *
   * @param row the row to encode
   * @param partitionSchema the partition schema describing the table's partitioning
   * @return an encoded partition key
   */
  public byte[] encodePartitionKey(PartialRow row, PartitionSchema partitionSchema) {
    buf.reset();
    if (!partitionSchema.getHashBucketSchemas().isEmpty()) {
      ByteBuffer bucketBuf = ByteBuffer.allocate(4 * partitionSchema.getHashBucketSchemas().size());
      bucketBuf.order(ByteOrder.BIG_ENDIAN);

      for (final HashBucketSchema hashBucketSchema : partitionSchema.getHashBucketSchemas()) {
        encodeColumns(row, hashBucketSchema.getColumnIds());
        byte[] encodedColumns = extractByteArray();
        long hash = Murmur2.hash64(encodedColumns,
                                   encodedColumns.length,
                                   hashBucketSchema.getSeed());
        int bucket = (int) UnsignedLongs.remainder(hash, hashBucketSchema.getNumBuckets());
        bucketBuf.putInt(bucket);
      }

      assert bucketBuf.arrayOffset() == 0;
      buf.write(bucketBuf.array(), 0, bucketBuf.position());
    }

    encodeColumns(row, partitionSchema.getRangeSchema().getColumns());
    return extractByteArray();
  }

  /**
   * Encodes a sequence of columns from the row.
   * @param row the row containing the columns to encode
   * @param columnIds the IDs of each column to encode
   */
  private void encodeColumns(PartialRow row, List<Integer> columnIds) {
    for (int i = 0; i < columnIds.size(); i++) {
      boolean isLast = i + 1 == columnIds.size();
      encodeColumn(row, row.getSchema().getColumnIndex(columnIds.get(i)), isLast);
    }
  }

  /**
   * Encodes a single column of a row.
   * @param row the row being encoded
   * @param columnIdx the column index of the column to encode
   * @param isLast whether the column is the last component of the key
   */
  private void encodeColumn(PartialRow row, int columnIdx, boolean isLast) {
    final Schema schema = row.getSchema();
    final ColumnSchema column = schema.getColumnByIndex(columnIdx);
    if (!row.isSet(columnIdx)) {
      throw new IllegalStateException(String.format("Primary key column %s is not set",
                                                    column.getName()));
    }
    final Type type = column.getType();

    if (type == Type.STRING || type == Type.BINARY) {
      final long varLenIdx = Bytes.getLong(row.getRowAlloc(), schema.getColumnOffset(columnIdx));
      addComponent(row.getVarLengthData().get((int) varLenIdx), type, isLast);
    } else {
      addComponent(row.getRowAlloc(),
                   schema.getColumnOffset(columnIdx),
                   type.getSize(),
                   type,
                   isLast);
    }
  }

  /**
   * Encodes a value of the given type into the key.
   * @param value the value to encode
   * @param type the type of the value to encode
   * @param isLast whether the value is the final component in the key
   */
  private void addComponent(byte[] value, Type type, boolean isLast) {
    addComponent(value, 0, value.length, type, isLast);
  }

  /**
   * Encodes a value of the given type into the key.
   * @param value the value to encode
   * @param offset the offset into the {@code value} buffer that the value begins
   * @param len the length of the value
   * @param type the type of the value to encode
   * @param isLast whether the value is the final component in the key
   */
  private void addComponent(byte[] value, int offset, int len, Type type, boolean isLast) {
    switch (type) {
      case BOOL:
        assert len == 1;
        buf.write(value[0]);
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case TIMESTAMP:
        // picking the first byte because big endian
        byte lastByte = value[offset + (len - 1)];
        lastByte = Bytes.xorLeftMostBit(lastByte);
        buf.write(lastByte);
        if (len > 1) {
          for (int i = len - 2; i >= 0; i--) {
            buf.write(value[offset + i]);
          }
        }
        break;
      case BINARY:
      case STRING:
        // if this is the last component, just add
        if (isLast) {
          buf.write(value, offset, len);
        } else {
          // If we're a middle component of a composite key, we need to add a \x00
          // at the end in order to separate this component from the next one. However,
          // if we just did that, we'd have issues where a key that actually has
          // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
          // encode \x00 as \x00\x01. -- key_encoder.h
          for (int b = offset; b < (offset + len); b++) {
            buf.write(value[b]);
            if (value[b] == 0x00) {
              buf.write(0x01);
            }
          }
          buf.write(0x00);
          buf.write(0x00);
        }
        break;
      default:
        throw new IllegalArgumentException(String.format(
            "The column type %s is not a valid key component type", type));
    }
  }

  /**
   * Returns the encoded key, and resets the key encoder to be used for another key.
   * @return the encoded key which has been built through calls to {@link #addComponent}
   */
  private byte[] extractByteArray() {
    byte[] bytes = buf.toByteArray();
    buf.reset();
    return bytes;
  }
}
