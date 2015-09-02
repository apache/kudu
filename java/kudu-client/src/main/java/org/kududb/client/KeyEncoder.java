// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.kududb.ColumnSchema;
import org.kududb.Schema;

import java.io.ByteArrayOutputStream;

/**
 * Utility class used to encode row keys in a format that is mainly used for tablet lookups.
 * Converts the non-string columns to big-endian order to facilitate memcmp
 */
class KeyEncoder {

  private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
  private final Schema schema;
  private int lastIndex = -1;

  KeyEncoder(Schema schema) {
    this.schema = schema;
  }

  /**
   * Utility method for slow paths like table pre-splitting
   * @param value Key component to add
   * @param columnIndex Which column we're adding it to
   */
  void addKey(byte[] value, int columnIndex) {
    addKey(value, 0, value.length, schema.getColumnByIndex(columnIndex), columnIndex);
  }

  void addKey(byte[] bytes, int offset, int size, ColumnSchema column, int columnIndex) {
    assert columnIndex == this.lastIndex + 1
        && columnIndex < this.schema.getPrimaryKeyColumnCount();
    switch (column.getType()) {
      case BOOL:
        assert size == 1;
        buf.write(bytes[0]);
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case TIMESTAMP:
        // picking the first byte because big endian
        byte lastByte = bytes[offset + (size - 1)];
        lastByte = Bytes.xorLeftMostBit(lastByte);
        buf.write(lastByte);
        if (size > 1) {
          for (int i = size - 2; i >= 0; i--) {
            buf.write(bytes[offset + i]);
          }
        }
        break;
      case BINARY:
      case STRING:
        // if this is the last component, just add
        if (columnIndex + 1 == this.schema.getPrimaryKeyColumnCount()) {
          buf.write(bytes, offset, size);
        } else {
          // If we're a middle component of a composite key, we need to add a \x00
          // at the end in order to separate this component from the next one. However,
          // if we just did that, we'd have issues where a key that actually has
          // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
          // encode \x00 as \x00\x01. -- key_encoder.h
          for (int b = offset; b < (offset + size); b++) {
            buf.write(bytes[b]);
            if (bytes[b] == 0x00) {
              buf.write(0x01);
            }
          }
          buf.write(0x00);
          buf.write(0x00);
        }
        break;
      default:
        throw new IllegalArgumentException("The column " + column.getName() + " is of type " +
            column.getType() + " which is not valid as part of a table key");
    }
    this.lastIndex++;
  }

  byte[] extractByteArray() {
    byte[] bytes = buf.toByteArray();
    buf.reset();
    lastIndex = -1;
    return bytes;
  }

}
