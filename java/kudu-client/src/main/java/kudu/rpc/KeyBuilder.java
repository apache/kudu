// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import kudu.Schema;

import java.math.BigInteger;

/**
 * Builder class to get encoded keys out of passed key components.
 * Key components must be added in order.
 * extractByteArray() resets the state so that the builder can be used again for another key.
 */
public class KeyBuilder {

  private final Schema schema;
  private final KeyEncoder encoder;
  private int currentColumn;

  public KeyBuilder(Schema schema) {
    this.schema = schema;
    this.encoder = new KeyEncoder(schema);
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addUnsignedByte(short val) {
    byte[] bytes = Bytes.fromUnsignedByte(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addUnsignedShort(int val) {
    byte[] bytes = Bytes.fromUnsignedShort(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addUnsignedInt(long val) {
    byte[] bytes = Bytes.fromUnsignedInt(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addUnsignedLong(BigInteger val) {
    byte[] bytes = Bytes.fromUnsignedLong(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addByte(byte val) {
    addBytes(new byte[] {val});
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addShort(short val) {
    byte[] bytes = Bytes.fromShort(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addInt(int val) {
    byte[] bytes = Bytes.fromInt(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addLong(long val) {
    byte[] bytes = Bytes.fromLong(val);
    addBytes(bytes);
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addString(String val) {
    addBytes(val.getBytes());
    return this;
  }

  /**
   * Add a new key component
   * @param val component to add
   * @throws IllegalArgumentException all the key components were already added
   */
  public KeyBuilder addBoolean(boolean val) {
    addByte((byte) (val ? 1 : 0));
    return this;
  }

  /**
   * Private method that keeps track of the current columns and actually adds the bytes to the
   * encoder.
   * @param bytes Key component to add
   */
  private void addBytes(byte[] bytes) {
    if (currentColumn == schema.getKeysCount()) {
      throw new IllegalArgumentException("Cannot add another key component, " +
          currentColumn + " were already added");
    }
    encoder.addKey(bytes, currentColumn);
    currentColumn++;
  }

  /**
   * Get the byte array that represents all the key components that were added. Also resets the
   * builder's state to be used again.
   * @return Encoded key
   */
  byte[] extractByteArray() {
    byte[] key = encoder.extractByteArray();
    currentColumn = 0;
    return key;
  }
}
