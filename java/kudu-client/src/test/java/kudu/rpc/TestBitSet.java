// Copyright (c) 2014, Cloudera, inc.
package kudu.rpc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    int size = Operation.getSizeOfColumnBitSet(colCount);
    assertEquals(size, Operation.toByteArray(bs, colCount).length);

    colCount = 7;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    size = Operation.getSizeOfColumnBitSet(colCount);
    assertEquals(size, Operation.toByteArray(bs, colCount).length);

    colCount = 8;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    bs.set(7);
    size = Operation.getSizeOfColumnBitSet(colCount);
    assertEquals(size, Operation.toByteArray(bs, colCount).length);

    colCount = 11;
    bs = new BitSet(colCount);
    bs.set(0);
    bs.set(5);
    bs.set(7);
    size = Operation.getSizeOfColumnBitSet(colCount);
    assertEquals(size, Operation.toByteArray(bs, colCount).length);
  }
}
