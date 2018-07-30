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

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.sangupta.murmur.Murmur2;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * An space-efficient filter which offers an approximate containment check.
 *
 * <p>It can be used to filter all the records which are wanted, but doesn't guarantee to filter out
 * all the records which are <i>not</i> wanted.
 *
 * <p>Please check this <a
 * href="https://en.wikipedia.org/wiki/Bloom_filter">wiki</a> for more details.
 *
 * <p>The {@code BloomFilter} here is a scanning filter and used to constrain the number of records
 * returned from TServer. It provides different types of {@code put} methods. When you {@code put} a
 * record into {@code BloomFilter}, it means you expect the TServer to return records with
 * the same value in a scan.
 *
 * <p>Here is an example for use:
 * <pre>
 * {@code
 *   BloomFilter bf = BloomFilter.BySizeAndFPRate(nBytes);
 *   bf.put(1);
 *   bf.put(3);
 *   bf.put(4);
 *   byte[] bitSet = bf.getBitSet();
 *   byte[] nHashes = bf.getNHashes();
 *   String hashFunctionName = bf.getHashFunctionName();
 *   // TODO: implement the interface for serializing and sending
 *   // (bitSet, nHashes, hashFunctionName) to TServer.
 * }
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public class BloomFilter {

  private final BitSet bitSet;
  private final int nHashes;
  private final byte[] byteBuffer;
  private final HashFunction hashFunction;
  private static final double DEFAULT_FP_RATE = 0.01;

  private BloomFilter(BitSet bitSet, int nHashes, HashFunction hashFunction) {
    Preconditions.checkArgument(bitSet.size() >= 8, "Number of bits in " +
      "bitset should be at least 8, but found %s.", bitSet.size());
    this.bitSet = bitSet;
    this.nHashes = nHashes;
    this.hashFunction = hashFunction;
    byteBuffer = new byte[8];
  }

  /**
   * Generate bloom filter, default hashing is {@code Murmur2} and false positive rate is 0.01.
   * @param nBytes size of bloom filter in bytes
   */
  public static BloomFilter bySize(int nBytes) {
    return bySizeAndFPRate(nBytes, DEFAULT_FP_RATE);
  }

  /**
   * Generate bloom filter, default hashing is {@code Murmur2}.
   * @param nBytes size of bloom filter in bytes
   * @param fpRate the probability that TServer will erroneously return a record that has not
   *               ever been {@code put} into the {@code BloomFilter}.
   */
  public static BloomFilter bySizeAndFPRate(int nBytes, double fpRate) {
    return bySizeAndFPRate(nBytes, fpRate, HashFunctions.MURMUR2);
  }

  /**
   * Generate bloom filter.
   * @param nBytes size of bloom filter in bytes
   * @param fpRate the probability that TServer will erroneously return a record that has not
   *               ever been {@code put} into the {@code BloomFilter}.
   * @param hashFunction hashing used when updating or checking containment, user should pick
   *                     the hashing function from {@code HashFunctions}
   */
  public static BloomFilter bySizeAndFPRate(int nBytes, double fpRate, HashFunction hashFunction) {
    int nBits = nBytes * 8;
    int nHashes = computeOptimalHashCount(nBits, optimalExpectedCount(nBytes, fpRate));
    return new BloomFilter(new BitSet(nBits), nHashes, hashFunction);
  }

  /**
   * Generate bloom filter, default hashing is {@code Murmur2} and false positive rate is 0.01.
   * @param expectedCount The expected number of elements, targeted by this bloom filter.
   *                      It is used to size the bloom filter.
   */
  public static BloomFilter byCount(int expectedCount) {
    return byCountAndFPRate(expectedCount, DEFAULT_FP_RATE);
  }

  /**
   * Generate bloom filter, default hashing is {@code Murmur2}.
   * @param expectedCount The expected number of elements, targeted by this bloom filter.
   *                      It is used to size the bloom filter.
   * @param fpRate the probability that TServer will erroneously return a record that has not
   *               ever been {@code put} into the {@code BloomFilter}.
   */
  public static BloomFilter byCountAndFPRate(int expectedCount, double fpRate) {
    return byCountAndFPRate(expectedCount, fpRate, HashFunctions.MURMUR2);
  }

  /**
   * Generate bloom filter.
   * @param expectedCount The expected number of elements, targeted by this bloom filter.
   *                      It is used to size the bloom filter.
   * @param fpRate the probability that TServer will erroneously return a record that has not
   *               ever been {@code put} into the {@code BloomFilter}.
   * @param hashFunction hashing used when updating or checking containment, user should pick
   *                     the hashing function from {@code HashFunctions}
   */
  public static BloomFilter byCountAndFPRate(
      int expectedCount, double fpRate, HashFunction hashFunction) {
    int nBytes = optimalNumOfBytes(expectedCount, fpRate);
    int nBits = nBytes * 8;
    int nHashes = computeOptimalHashCount(nBits, expectedCount);
    return new BloomFilter(new BitSet(nBits), nHashes, hashFunction);
  }

  /**
   * Update bloom filter with a {@code byte[]}.
   */
  public void put(byte[] data) {
    updateBitset(data, data.length);
  }

  /**
   * Update bloom filter with a {@code boolean}.
   */
  public void put(boolean data) {
    byteBuffer[0] = (byte)(data ? 1 : 0);
    updateBitset(byteBuffer, 1);
  }

  /**
   * Update bloom filter with a {@code byte}.
   */
  public void put(byte data) {
    byteBuffer[0] = data;
    updateBitset(byteBuffer, 1);
  }

  /**
   * Update bloom filter with a {@code short}.
   */
  public void put(short data) {
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    updateBitset(byteBuffer, 2);
  }

  /**
   * Update bloom filter with a {@code int}.
   */
  public void put(int data) {
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    byteBuffer[2] = (byte) (data >>> 16);
    byteBuffer[3] = (byte) (data >>> 24);
    updateBitset(byteBuffer, 4);
  }

  /**
   * Update bloom filter with a {@code long}.
   */
  public void put(long data) {
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    byteBuffer[2] = (byte) (data >>> 16);
    byteBuffer[3] = (byte) (data >>> 24);
    byteBuffer[4] = (byte) (data >>> 32);
    byteBuffer[5] = (byte) (data >>> 40);
    byteBuffer[6] = (byte) (data >>> 48);
    byteBuffer[7] = (byte) (data >>> 56);
    updateBitset(byteBuffer, 8);
  }

  /**
   * Update bloom filter with a {@code float}.
   */
  public void put(float data) {
    put(Float.floatToIntBits(data));
  }

  /**
   * Update bloom filter with a {@code double}.
   */
  public void put(double data) {
    put(Double.doubleToLongBits(data));
  }

  /**
   * Update bloom filter with a {@code String}.
   */
  public void put(String data) {
    put(data.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Get the internal bit set in bytes.
   */
  public byte[] getBitSet() {
    return bitSet.toByteArray();
  }

  /**
   * Get the number of hashing times when updating or checking containment.
   */
  public int getNHashes() {
    return nHashes;
  }

  /**
   * Get the name of hashing used when updating or checking containment.
   */
  public String getHashFunctionName() {
    return hashFunction.toString();
  }

  // Mark it `private` and user can only use the `HashFunction` specified in the
  // enumeration below. Thus user cannot send TServer a self defined `HashFunction`,
  // which might not be identified by TServer.
  private interface HashFunction {
    long hash(byte[] data, int length, long seed);
  }

  /**
   * Hashing functions used when updating or checking containment for a bloom filter.
   * Currently the only choice is {@code Murmur2}, but we can consider to add more hashing
   * functions in the future.
   */
  public enum HashFunctions implements HashFunction {
    MURMUR2() {
      @Override
      public long hash(byte[] data, int length, long seed) {
        return Murmur2.hash(data, length, seed);
      }

      @Override
      public String toString() {
        return "Murmur2";
      }
    }
  }

  private void updateBitset(byte[] byteBuffer, int length) {
    Preconditions.checkArgument(byteBuffer.length >= length);
    long h = Murmur2.hash64(byteBuffer, length, 0);
    long h1 = (0xFFFFFFFFL & h);
    long h2 = (h >>> 32);
    long tmp = h1;
    for (int i = 0; i < nHashes; i++) {
      long bitPos = tmp % bitSet.size();
      bitSet.set((int)bitPos);
      tmp += h2;
    }
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(byte[] data) {
    return checkIfContains(data);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(boolean data) {
    byte[] byteBuffer = new byte[1];
    if (data) {
      byteBuffer[0] = 1;
    } else {
      byteBuffer[0] = 0;
    }
    return checkIfContains(byteBuffer);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(byte data) {
    byte[] byteBuffer = new byte[1];
    byteBuffer[0] = data;
    return checkIfContains(byteBuffer);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(short data) {
    byte[] byteBuffer = new byte[2];
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    return checkIfContains(byteBuffer);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(int data) {
    byte[] byteBuffer = new byte[4];
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    byteBuffer[2] = (byte) (data >>> 16);
    byteBuffer[3] = (byte) (data >>> 24);
    return checkIfContains(byteBuffer);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(long data) {
    byte[] byteBuffer = new byte[8];
    byteBuffer[0] = (byte) (data >>> 0);
    byteBuffer[1] = (byte) (data >>> 8);
    byteBuffer[2] = (byte) (data >>> 16);
    byteBuffer[3] = (byte) (data >>> 24);
    byteBuffer[4] = (byte) (data >>> 32);
    byteBuffer[5] = (byte) (data >>> 40);
    byteBuffer[6] = (byte) (data >>> 48);
    byteBuffer[7] = (byte) (data >>> 56);
    return checkIfContains(byteBuffer);
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(float data) {
    return mayContain(Float.floatToIntBits(data));
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(double data) {
    return mayContain(Double.doubleToLongBits(data));
  }

  @InterfaceAudience.LimitedPrivate("Test")
  public boolean mayContain(String data) {
    return mayContain(data.getBytes(StandardCharsets.UTF_8));
  }

  private boolean checkIfContains(byte[] bytes) {
    long h = Murmur2.hash64(bytes, bytes.length, 0);

    long h1 = (0xFFFFFFFFL & h);
    long h2 = (h >>> 32);
    long tmp = h1;
    int remHashes = nHashes;
    while (remHashes != 0) {
      long bitPos = tmp % bitSet.size();
      if (!bitSet.get((int)bitPos)) {
        return false;
      }
      tmp += h2;
      remHashes--;
    }
    return true;
  }

  private static double kNaturalLog2 = 0.69314;

  private static int optimalNumOfBytes(int expectedCount, double fpRate) {
    if (fpRate == 0) {
      fpRate = Double.MIN_VALUE;
    }
    return (int) Math.ceil(-expectedCount * Math.log(fpRate) / (Math.log(2) * Math.log(2) * 8));
  }

  private static int optimalExpectedCount(int nBytes, double fpRate) {
    int nBits = nBytes * 8;
    return (int) (Math.ceil(-nBits * kNaturalLog2 * kNaturalLog2 / Math.log(fpRate)));
  }

  private static int computeOptimalHashCount(int nBits, int elems) {
    int nHashes = (int)(nBits * kNaturalLog2 / elems);
    if (nHashes < 1) nHashes = 1;
    return nHashes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BloomFilter(nBits=");
    sb.append(bitSet.size());
    sb.append(", nHashes=");
    sb.append(nHashes);
    sb.append(", hashing=");
    sb.append(hashFunction);
    sb.append(")");
    return sb.toString();
  }
}
