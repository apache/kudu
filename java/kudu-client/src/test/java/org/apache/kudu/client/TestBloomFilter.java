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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;
import org.apache.kudu.util.BloomFilter;

public class TestBloomFilter {

  private int numBytes = 32 * 1024;
  private long randomSeed = System.currentTimeMillis();
  private int numKeys = 2000;

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testNumberOfHashes() {
    assertEquals(BloomFilter.byCountAndFPRate(10, 0.1).getNumHashes(), 3);
    assertEquals(BloomFilter.byCountAndFPRate(100, 0.2).getNumHashes(), 2);
    assertEquals(BloomFilter.byCountAndFPRate(1000, 0.05).getNumHashes(),  4);
    assertEquals(BloomFilter.byCountAndFPRate(10000, 0.01).getNumHashes(), 6);
    assertEquals(BloomFilter.bySizeAndFPRate(10, 0.1).getNumHashes(), 3);
    assertEquals(BloomFilter.bySizeAndFPRate(1000, 0.2).getNumHashes(), 2);
    assertEquals(BloomFilter.bySizeAndFPRate(100000, 0.05).getNumHashes(), 4);
    assertEquals(BloomFilter.bySizeAndFPRate(10000000, 0.01).getNumHashes(), 6);
  }

  @Test
  public void testIntGenBFBySize() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put integers into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextInt());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextInt()));
    }
  }

  @Test
  public void testIntGenBFByCount() {
    final BloomFilter bf = BloomFilter.byCount(numKeys);
    // Put integers into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextInt());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextInt()));
    }
  }

  @Test
  public void testBytes() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put byte arrays into bloomfilter by random
    Random rand = new Random(randomSeed);
    byte[] bytes = new byte[64];
    for (int i = 0; i < numKeys; i++) {
      rand.nextBytes(bytes);
      bf.put(bytes);
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      rand.nextBytes(bytes);
      assertTrue(bf.mayContain(bytes));
    }
  }

  @Test
  public void testBoolean() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put booleans into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextBoolean());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextBoolean()));
    }
  }

  @Test
  public void testShort() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put shorts into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put((short)rand.nextInt());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain((short)rand.nextInt()));
    }
  }

  @Test
  public void testLong() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put longs into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextLong());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextLong()));
    }
  }

  @Test
  public void testFloat() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put floats into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextFloat());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextFloat()));
    }
  }

  @Test
  public void testDouble() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put doubles into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextDouble());
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextDouble()));
    }
  }

  @Test
  public void testString() {
    final BloomFilter bf = BloomFilter.bySize(numBytes);
    // Put strings into bloomfilter by random
    Random rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      bf.put(rand.nextInt() + "");
    }
    // Reset the rand and check existence of the keys.
    rand = new Random(randomSeed);
    for (int i = 0; i < numKeys; i++) {
      assertTrue(bf.mayContain(rand.nextInt() + ""));
    }
  }
}
