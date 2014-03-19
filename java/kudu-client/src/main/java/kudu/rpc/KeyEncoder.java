// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import kudu.ColumnSchema;
import kudu.Schema;

import java.io.ByteArrayOutputStream;

/**
 * Utility class used to encode row keys in a format that is mainly used for tablet lookups.
 */
class KeyEncoder {

  private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
  private final Schema schema;
  private int lastIndex = -1;

  KeyEncoder(Schema schema) {
    this.schema = schema;
  }

  void addKey(byte[] value, int offset, int size, ColumnSchema column, int columnIndex) {
    assert columnIndex == this.lastIndex + 1 && columnIndex < this.schema.getKeysCount();
    switch (column.getType()) {
      case UINT8:
      case UINT16:
      case UINT32:
      case UINT64:
        buf.write(value, offset, size);
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        // picking the last byte because little endian
        byte lastByte = value[offset + (size - 1)];
        lastByte = Bytes.xorLeftMostBit(lastByte);
        if (size > 1) {
          buf.write(value, offset, size - 1);
        }
        buf.write(lastByte);
        break;
      case STRING:
        // if this is the last component, just add
        if (columnIndex + 1 == this.schema.getKeysCount()) {
          buf.write(value, offset, size);
        } else {
          // If we're a middle component of a composite key, we need to add a \x00
          // at the end in order to separate this component from the next one. However,
          // if we just did that, we'd have issues where a key that actually has
          // \x00 in it would compare wrong, so we have to instead add \x00\x00, and
          // encode \x00 as \x00\x01. -- key_encoder.h
          for (int b = offset; b < (offset + size); b++) {
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
        throw new IllegalArgumentException("The column " + column.getName() + " is of type " +
            column.getType() + " which is unknown");
    }
    this.lastIndex++;
  }

  byte[] toByteArray() {
    return buf.toByteArray();
  }

}
