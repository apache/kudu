// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.BitSet;

public class TestBitSet {

  /**
   * Test out BitSet-related operations
   */
  @Test
  public void test() {
    int colCount = 1;
    BitSet bs = new BitSet(colCount);
    bs.set(0);
    int size = Bytes.getBitSetSize(colCount);
    byte[] result =  Bytes.fromBitSet(bs, colCount);
    assertEquals(size, result.length);
    BitSet newBs = Bytes.toBitSet(result, 0, colCount);
    assertTrue(newBs.get(0));

    colCount = 7;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    size = Bytes.getBitSetSize(colCount);
    result =  Bytes.fromBitSet(bs, colCount);
    assertEquals(size, result.length);
    newBs = Bytes.toBitSet(result, 0, colCount);
    assertTrue(newBs.get(0));
    assertFalse(newBs.get(1));
    assertFalse(newBs.get(2));
    assertFalse(newBs.get(3));
    assertFalse(newBs.get(4));
    assertTrue(newBs.get(5));
    assertFalse(newBs.get(6));

    colCount = 8;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    bs.set(7);
    size = Bytes.getBitSetSize(colCount);
    result =  Bytes.fromBitSet(bs, colCount);
    assertEquals(size, result.length);
    newBs = Bytes.toBitSet(result, 0, colCount);
    assertTrue(newBs.get(0));
    assertFalse(newBs.get(1));
    assertFalse(newBs.get(2));
    assertFalse(newBs.get(3));
    assertFalse(newBs.get(4));
    assertTrue(newBs.get(5));
    assertFalse(newBs.get(6));
    assertTrue(newBs.get(7));

    colCount = 11;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    bs.set(7);
    bs.set(9);
    size = Bytes.getBitSetSize(colCount);
    result =  Bytes.fromBitSet(bs, colCount);
    assertEquals(size, result.length);
    newBs = Bytes.toBitSet(result, 0, colCount);
    assertTrue(newBs.get(0));
    assertFalse(newBs.get(1));
    assertFalse(newBs.get(2));
    assertFalse(newBs.get(3));
    assertFalse(newBs.get(4));
    assertTrue(newBs.get(5));
    assertFalse(newBs.get(6));
    assertTrue(newBs.get(7));
    assertFalse(newBs.get(8));
    assertTrue(newBs.get(9));
    assertFalse(newBs.get(10));
  }
}
