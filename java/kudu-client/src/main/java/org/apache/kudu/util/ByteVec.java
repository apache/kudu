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

package org.apache.kudu.util;

import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A vector of primitive bytes.
 *
 * The vector is backed by a contiguous array, and offers efficient random
 * access.
 */
@InterfaceAudience.Private
@NotThreadSafe
public final class ByteVec {

  /** Default initial capacity for new vectors. */
  @InterfaceAudience.LimitedPrivate("Test")
  static final int DEFAULT_CAPACITY = 32;

  /** data backing the vector. */
  private byte[] data;

  /** offset of first unused element in data. */
  private int len;

  private ByteVec(int capacity) {
    data = new byte[capacity];
    len = 0;
  }

  private ByteVec(byte[] data) {
    this.data = data;
    this.len = data.length;
  }

  /**
   * Creates a new vector.
   * @return the new vector.
   */
  public static ByteVec create() {
    return new ByteVec(DEFAULT_CAPACITY);
  }

  /**
   * Creates a new vector with the specified capacity.
   * @param capacity the initial capacity of the vector
   * @return a new vector with the specified capacity
   */
  public static ByteVec withCapacity(int capacity) {
    return new ByteVec(capacity);
  }

  /**
   * Wrap an existing array with a vector.
   * The array should not be modified after this call.
   * @param data the initial data for the vector
   * @return a vector wrapping the data
   */
  public static ByteVec wrap(byte[] data) {
    return new ByteVec(data);
  }

  /** Returns the number of elements the vector can hold without reallocating. */
  public int capacity() {
    return data.length;
  }

  /** Returns the primitive array backing the vector. The caller should not modify the array. */
  public byte[] data() {
    return data;
  }

  /** Returns the number of elements in the vector. */
  public int len() {
    return len;
  }

  /** Returns {@code true} if the vector is empty. */
  public boolean isEmpty() {
    return len == 0;
  }

  /**
   * Reserves capacity for at least {@code additional} more elements to be
   * inserted into the vector.
   *
   * The vector may reserve more space to avoid frequent reallocations. If the
   * vector already has sufficient capacity, no reallocation will happen.
   *
   * @param additional capacity to reserve
   */
  public void reserveAdditional(int additional) {
    Preconditions.checkArgument(additional >= 0, "negative additional");
    if (data.length - len >= additional) {
      return;
    }
    // Use a 1.5x growth factor. According to
    // https://stackoverflow.com/questions/1100311/what-is-the-ideal-growth-rate-for-a-dynamically-allocated-array
    // this is close to the ideal ratio, although it isn't clear if that holds
    // for managed languages.
    data = Arrays.copyOf(data, Math.max(len + additional,
                                        data.length + data.length / 2));
  }

  /**
   * Reserves capacity for exactly {@code additional} more elements to be
   * inserted into the vector.
   *
   * If the vector already has sufficient capacity, no reallocation will happen.
   *
   * @param additional capacity to reserve
   */
  public void reserveExact(int additional) {
    Preconditions.checkArgument(additional >= 0, "negative additional");
    if (data.length - len >= additional) {
      return;
    }
    data = Arrays.copyOf(data, len + additional);
  }

  /**
   * Shrink the capacity of the vector to match the length.
   */
  public void shrinkToFit() {
    if (len < data.length) {
      data = Arrays.copyOf(data, len);
    }
  }

  /**
   * Shorten the vector to be {@code len} elements long.
   * If {@code len} is greater than the vector's current length,
   * this has no effect.
   * @param len the new length of the vector
   */
  public void truncate(int len) {
    Preconditions.checkArgument(len >= 0, "negative len");
    this.len = Math.min(this.len, len);
  }

  /**
   * Removes all elements from the vector.
   * No reallocation will be performed.
   */
  public void clear() {
    truncate(0);
  }

  /**
   * Appends an element to the vector.
   * @param element the element to append
   */
  public void push(byte element) {
    reserveAdditional(1);
    data[len++] = element;
  }

  /**
   * Sets the element at {@code index} to the provided value.
   * @param index of the element to set
   * @param value to set the element to
   * @throws IndexOutOfBoundsException if {@code index} is not valid
   */
  public void set(int index, byte value) {
    if (index >= len) {
      throw new IndexOutOfBoundsException(String.format("index: %s, len: %s", index, len));
    }
    data[index] = value;
  }

  /**
   * Appends the bytes from another byte array to this vec.
   * @param values the values to append
   * @param offset the offset into {@code values} to append from
   * @param len the number of bytes from {@code values} to append
   */
  public void append(byte[] values, int offset, int len) {
    reserveAdditional(len);
    System.arraycopy(values, offset, this.data, this.len, len);
    this.len += len;
  }

  /**
   * Appends all of the bytes from another byte array to this vec.
   * @param values the values to append
   */
  public void append(byte[] values) {
    append(values, 0, values.length);
  }

  /**
   * Concatenates another vector onto the end of this one.
   * @param other the other vector to concatenate onto this one
   */
  public void append(ByteVec other) {
    append(other.data, 0, other.len);
  }

  /**
   * Returns the element at the specified position.
   * @param index of the element to return
   * @return the element at the specified position
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  public byte get(int index) {
    if (index >= len) {
      throw new IndexOutOfBoundsException(String.format("index: %s, len: %s", index, len));
    }
    return data[index];
  }

  /**
   * Returns a list view of the vector.
   * The vector should not be concurrently modified while the list is in use.
   * @return a list view of the vector
   */
  public List<Byte> asList() {
    List<Byte> list = Bytes.asList(data);
    if (len < data.length) {
      return list.subList(0, len);
    }
    return list;
  }

  /**
   * @return a copy of the vector as a byte[].
   */
  public byte[] toArray() {
    return Arrays.copyOf(data, len);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (len == 0) {
      return "[]";
    }

    StringBuilder builder = new StringBuilder(4 + len * 2);
    builder.append("[0x");
    builder.append(BaseEncoding.base16().encode(data, 0, len));
    builder.append(']');
    return builder.toString();
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteVec other = (ByteVec) o;
    if (len != other.len) {
      return false;
    }
    for (int i = 0; i < len; i++) {
      if (data[i] != other.data[i]) {
        return false;
      }
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    int result = len;
    for (int i = 0; i < len; i++) {
      result = 31 * result + data[i];
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public ByteVec clone() {
    ByteVec clone = ByteVec.withCapacity(data.length);
    clone.append(this);
    return clone;
  }
}
