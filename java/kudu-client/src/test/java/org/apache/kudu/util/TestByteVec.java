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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

public class TestByteVec {
  private static final Random RAND = new Random();

  @Rule
  public RetryRule retryRule = new RetryRule();

  private void assertBytesEqual(byte a, byte b) {
    if (a != b) {
      throw new AssertionError(String.format("%s != %s", a, b));
    }
  }

  private List<Byte> random() {
    return random(RAND.nextInt(1024));
  }

  private List<Byte> random(int len) {
    List<Byte> list = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      list.add((byte) RAND.nextInt(i + 1));
    }
    return Collections.unmodifiableList(list);
  }

  private void checkByteVec(List<Byte> vals) {
    ByteVec vec = ByteVec.create();
    assertEquals(0, vec.len());

    // push
    for (byte i : vals) {
      vec.push(i);
    }
    assertEquals(vals, vec.asList());

    // withCapacity
    assertEquals(0, ByteVec.withCapacity(0).capacity());
    assertEquals(13, ByteVec.withCapacity(13).capacity());

    // wrap
    assertEquals(vec, ByteVec.wrap(vec.toArray()));

    // clone, equals
    ByteVec copy = vec.clone();
    assertEquals(copy, vec);

    // truncate
    copy.truncate(vec.len() + 1);
    assertEquals(vals, copy.asList());
    vec.truncate(copy.len());
    assertEquals(vals, copy.asList());
    copy.truncate(vals.size() / 2);
    assertEquals(vals.subList(0, vals.size() / 2), copy.asList());
    if (vals.size() > 0) {
      assertNotEquals(vals, copy.asList());
    }

    // reserveAdditional
    int unused = copy.capacity() - copy.len();

    copy.reserveAdditional(unused);
    assertEquals(vec.capacity(), copy.capacity());

    copy.reserveAdditional(unused + 1);
    assertTrue(copy.capacity() > vec.capacity());

    // reserveExact
    unused = copy.capacity() - copy.len();
    copy.reserveExact(unused + 3);
    assertEquals(copy.capacity() - copy.len(), unused + 3);

    copy.truncate(0);
    assertEquals(0, copy.len());

    // shrinkToFit
    copy.shrinkToFit();
    assertEquals(0, copy.capacity());
    vec.shrinkToFit();
    assertEquals(vec.len(), vec.capacity());

    // get
    for (int i = 0; i < vals.size(); i++) {
      assertBytesEqual(vals.get(i), vec.get(i));
    }

    // set
    if (vec.len() > 0) {
      copy = vec.clone();
      int index = RAND.nextInt(vec.len());
      copy.set(index, (byte) index);
      List<Byte> intsCopy = new ArrayList<>(vals);
      intsCopy.set(index, (byte) index);
      assertEquals(intsCopy, copy.asList());
    }
  }

  @Test
  public void testByteVec() throws Exception {
    checkByteVec(random(0));
    checkByteVec(random(1));
    checkByteVec(random(2));
    checkByteVec(random(3));
    checkByteVec(random(ByteVec.DEFAULT_CAPACITY - 2));
    checkByteVec(random(ByteVec.DEFAULT_CAPACITY - 1));
    checkByteVec(random(ByteVec.DEFAULT_CAPACITY));
    checkByteVec(random(ByteVec.DEFAULT_CAPACITY + 1));
    checkByteVec(random(ByteVec.DEFAULT_CAPACITY + 2));

    for (int i = 0; i < 100; i++) {
      checkByteVec(random());
    }
  }
}
