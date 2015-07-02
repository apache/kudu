// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.util;

import com.google.common.primitives.UnsignedLongs;
import com.sangupta.murmur.Murmur2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test Murmur2 Hash64 returns the expected values for inputs.
 *
 * These tests are duplicated on the C++ side to ensure that hash computations
 * are stable across both platforms.
 */
public class TestMurmurHash {

    @Test
    public void testMurmur2Hash64() throws Exception {
      long hash;

      hash = Murmur2.hash64("ab".getBytes("UTF-8"), 2, 0);
      assertEquals(UnsignedLongs.parseUnsignedLong("7115271465109541368"), hash);

      hash = Murmur2.hash64("abcdefg".getBytes("UTF-8"), 7, 0);
      assertEquals(UnsignedLongs.parseUnsignedLong("2601573339036254301"), hash);

      hash = Murmur2.hash64("quick brown fox".getBytes("UTF-8"), 15, 42);
      assertEquals(UnsignedLongs.parseUnsignedLong("3575930248840144026"), hash);
    }
}
