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
  /**
   * Compute 64-bit FastHash of the supplied data backed by byte array.
   *
   * FastHash is simple, robust, and efficient general-purpose hash function from Google.
   * Implementation is adapted from https://code.google.com/archive/p/fast-hash/
   *
   * @param buf the data to hash
   * @param len length of the supplied data
   * @param seed seed to compute the hash
   * @return computed 64-bit hash value
   */
  public static long fastHash64(final byte[] buf, int len, long seed) {
    final long m = 0x880355f21e6d1965L;
    long h = seed ^ (len * m);
    long v;

    int len8 = len / 8;
    for (int i = 0; i < len8; ++i) {
      int pos = i * 8;
      v = buf[pos] +
          ((long)buf[pos + 1] << 8) +  ((long)buf[pos + 2] << 16) +
          ((long)buf[pos + 3] << 24) + ((long)buf[pos + 4] << 32) +
          ((long)buf[pos + 5] << 40) + ((long)buf[pos + 6] << 48) +
          ((long)buf[pos + 7] << 56);
      h ^= fastHashMix(v);
      h *= m;
    }

    v = 0;
    int pos2 = len8 * 8;
    //CHECKSTYLE:OFF
    switch (len & 7) {
      case 7:
        v ^= (long)buf[pos2 + 6] << 48;
      // fall through
      case 6:
        v ^= (long)buf[pos2 + 5] << 40;
      // fall through
      case 5:
        v ^= (long)buf[pos2 + 4] << 32;
      // fall through
      case 4:
        v ^= (long)buf[pos2 + 3] << 24;
      // fall through
      case 3:
        v ^= (long)buf[pos2 + 2] << 16;
      // fall through
      case 2:
        v ^= (long)buf[pos2 + 1] << 8;
      // fall through
      case 1:
        v ^= buf[pos2];
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
   * @param len length of the supplied data
   * @param seed seed to compute the hash
   * @return computed 32-bit hash value
   */
  public static int fastHash32(final byte[] buf, int len, int seed) {
    // the following trick converts the 64-bit hashcode to Fermat
    // residue, which shall retain information from both the higher
    // and lower parts of hashcode.
    long h = fastHash64(buf, len, seed);
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
