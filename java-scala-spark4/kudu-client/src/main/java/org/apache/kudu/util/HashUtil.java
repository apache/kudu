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

/**
 * Hash utility functions.
 */
public class HashUtil {
  // Constants imported from Apache Impala used to compute hash values for special cases.
  // They are arbitrary constant obtained by taking lower bytes of generated UUID. Helps
  // distinguish NULL values and zero-length objects like empty strings.
  // Impala uses the direct BlockBloomFilter C++ API and inserts hash value directly using
  // its own implementation of the Fast hash. Hence the value must match with Impala.
  // Though Impala will use C++ API, keeping the implementation of the Fast hash algorithm
  // consistent across C++ and Java.
  private static final int HASH_VAL_NULL = 0x58081667;
  private static final byte[] HASH_VAL_NULL_BYTE_BUF = new byte[4];

  private static final int HASH_VAL_EMPTY = 0x7dca7eee;
  private static final byte[] HASH_VAL_EMPTY_BYTE_BUF = new byte[4];

  static {
    HASH_VAL_NULL_BYTE_BUF[0] = (byte) (HASH_VAL_NULL >>> 0);
    HASH_VAL_NULL_BYTE_BUF[1] = (byte) (HASH_VAL_NULL >>> 8);
    HASH_VAL_NULL_BYTE_BUF[2] = (byte) (HASH_VAL_NULL >>> 16);
    HASH_VAL_NULL_BYTE_BUF[3] = (byte) (HASH_VAL_NULL >>> 24);

    HASH_VAL_EMPTY_BYTE_BUF[0] = (byte) (HASH_VAL_EMPTY >>> 0);
    HASH_VAL_EMPTY_BYTE_BUF[1] = (byte) (HASH_VAL_EMPTY >>> 8);
    HASH_VAL_EMPTY_BYTE_BUF[2] = (byte) (HASH_VAL_EMPTY >>> 16);
    HASH_VAL_EMPTY_BYTE_BUF[3] = (byte) (HASH_VAL_EMPTY >>> 24);
  }

  /** Non-constructable utility class. */
  private HashUtil() {
  }

  /**
   * Compute 64-bit FastHash of the supplied data backed by byte array.
   *
   * FastHash is simple, robust, and efficient general-purpose hash function from Google.
   * Implementation is adapted from https://code.google.com/archive/p/fast-hash/
   *
   * Adds special handling for null input.
   *
   * @param buf the data to hash
   * @param seed seed to compute the hash
   * @return computed 64-bit hash value
   */
  public static long fastHash64(byte[] buf, long seed) {
    // Special handling for null input with possible non-zero length as could be the
    // case with nullable column values.
    if (buf == null) {
      buf = HASH_VAL_NULL_BYTE_BUF;
    } else if (buf.length == 0) {
      buf = HASH_VAL_EMPTY_BYTE_BUF;
    }
    final int len = buf.length;
    final long m = 0x880355f21e6d1965L;
    long h = seed ^ (len * m);
    long v;

    int len8 = len / 8;
    for (int i = 0; i < len8; ++i) {
      int pos = i * 8;
      v = (buf[pos] & 0xFF) |
          ((long)(buf[pos + 1] & 0xFF) << 8) |  ((long)(buf[pos + 2] & 0xFF) << 16) |
          ((long)(buf[pos + 3] & 0xFF) << 24) | ((long)(buf[pos + 4] & 0xFF) << 32) |
          ((long)(buf[pos + 5] & 0xFF) << 40) | ((long)(buf[pos + 6] & 0xFF) << 48) |
          ((long)(buf[pos + 7] & 0xFF) << 56);
      h ^= fastHashMix(v);
      h *= m;
    }

    v = 0;
    int pos2 = len8 * 8;
    //CHECKSTYLE:OFF
    switch (len & 7) {
      case 7:
        v ^= (long)(buf[pos2 + 6] & 0xFF) << 48;
      // fall through
      case 6:
        v ^= (long)(buf[pos2 + 5] & 0xFF) << 40;
      // fall through
      case 5:
        v ^= (long)(buf[pos2 + 4] & 0xFF) << 32;
      // fall through
      case 4:
        v ^= (long)(buf[pos2 + 3] & 0xFF) << 24;
      // fall through
      case 3:
        v ^= (long)(buf[pos2 + 2] & 0xFF) << 16;
      // fall through
      case 2:
        v ^= (long)(buf[pos2 + 1] & 0xFF) << 8;
      // fall through
      case 1:
        v ^= (buf[pos2] & 0xFF);
        h ^= fastHashMix(v);
        h *= m;
    }
    //CHECKSTYLE:ON

    return fastHashMix(h);
  }


  /**
   * Compute 32-bit FastHash of the supplied data backed by byte array.
   *
   * FastHash is simple, robust, and efficient general-purpose hash function from Google.
   * Implementation is adapted from https://code.google.com/archive/p/fast-hash/
   *
   * @param buf the data to compute the hash
   * @param seed seed to compute the hash
   * @return computed 32-bit hash value
   */
  public static int fastHash32(byte[] buf, int seed) {
    // the following trick converts the 64-bit hashcode to Fermat
    // residue, which shall retain information from both the higher
    // and lower parts of hashcode.
    long h = fastHash64(buf, seed);
    return (int)(h - (h >>> 32));
  }

  // Compression function for Merkle-Damgard construction.
  private static long fastHashMix(long h) {
    h ^= h >>> 23;
    h *= 0x2127599bf4325c37L;
    h ^= h >>> 47;
    return h;
  }
}