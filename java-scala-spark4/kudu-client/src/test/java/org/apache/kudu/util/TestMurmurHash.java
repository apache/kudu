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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import com.google.common.primitives.UnsignedLongs;
import com.sangupta.murmur.Murmur2;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

/**
 * Test Murmur2 Hash64 returns the expected values for inputs.
 *
 * These tests are duplicated on the C++ side to ensure that hash computations
 * are stable across both platforms.
 */
public class TestMurmurHash {

  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testMurmur2Hash64() throws Exception {
    long hash;

    hash = Murmur2.hash64("ab".getBytes(UTF_8), 2, 0);
    assertEquals(UnsignedLongs.parseUnsignedLong("7115271465109541368"), hash);

    hash = Murmur2.hash64("abcdefg".getBytes(UTF_8), 7, 0);
    assertEquals(UnsignedLongs.parseUnsignedLong("2601573339036254301"), hash);

    hash = Murmur2.hash64("quick brown fox".getBytes(UTF_8), 15, 42);
    assertEquals(UnsignedLongs.parseUnsignedLong("3575930248840144026"), hash);
  }
}
