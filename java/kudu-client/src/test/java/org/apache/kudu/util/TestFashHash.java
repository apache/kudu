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

import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.test.junit.RetryRule;

/**
 * Test FastHash64/32 returns the expected values for inputs.
 *
 * These tests are duplicated on the C++ side to ensure that hash computations
 * are stable across both platforms.
 */
public class TestFashHash {
  @Rule
  public RetryRule retryRule = new RetryRule();

  @Test
  public void testFastHash64() {
    long hash;

    hash = HashUtil.fastHash64("ab".getBytes(UTF_8), 0);
    assertEquals(Long.parseUnsignedLong("17293172613997361769"), hash);

    hash = HashUtil.fastHash64("abcdefg".getBytes(UTF_8), 0);
    assertEquals(Long.parseUnsignedLong("10206404559164245992"), hash);

    hash = HashUtil.fastHash64("quick brown fox".getBytes(UTF_8), 42);
    assertEquals(Long.parseUnsignedLong("3757424404558187042"), hash);

    hash = HashUtil.fastHash64(null, 0);
    assertEquals(Long.parseUnsignedLong("12680076593665652444"), hash);

    hash = HashUtil.fastHash64("".getBytes(UTF_8), 0);
    assertEquals(0, hash);
  }

  @Test
  public void testFastHash32() {
    int hash;

    hash = HashUtil.fastHash32("ab".getBytes(UTF_8), 0);
    assertEquals(Integer.parseUnsignedInt("2564147595"), hash);

    hash = HashUtil.fastHash32("abcdefg".getBytes(UTF_8), 0);
    assertEquals(Integer.parseUnsignedInt("1497700618"), hash);

    hash = HashUtil.fastHash32("quick brown fox".getBytes(UTF_8), 42);
    assertEquals(Integer.parseUnsignedInt("1676541068"), hash);

    hash = HashUtil.fastHash32(null, 0);
    assertEquals(Integer.parseUnsignedInt("842467426"), hash);

    hash = HashUtil.fastHash32("".getBytes(UTF_8), 0);
    assertEquals(0, hash);
  }
}
