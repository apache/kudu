// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <boost/utility.hpp>
#include <gtest/gtest.h>
#include <math.h>

#include "util/rle-encoding.h"
#include "util/bit-stream-utils.h"
#include "util/test_util.h"

using namespace std;

namespace kudu {

TEST(BitArray, TestBool) {
  const int len = 8;
  faststring buffer(len);

  BitWriter writer(&buffer);

  // Write alternating 0's and 1's
  for (int i = 0; i < 8; ++i) {
    writer.PutBool(i % 2);
  }
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));

  // Write 00110011
  for (int i = 0; i < 8; ++i) {
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        writer.PutBool(false);
        break;
      default:
        writer.PutBool(true);
        break;
    }
  }

  // Validate the exact bit value
  EXPECT_EQ((int)buffer[0], BOOST_BINARY(1 0 1 0 1 0 1 0));
  EXPECT_EQ((int)buffer[1], BOOST_BINARY(1 1 0 0 1 1 0 0));

  // Use the reader and validate
  BitReader reader(buffer.data(), len);
  for (int i = 0; i < 8; ++i) {
    bool val = false;
    bool result = reader.GetBool(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(val, i % 2);
  }

  for (int i = 0; i < 8; ++i) {
    bool val;
    bool result = reader.GetBool(&val);
    EXPECT_TRUE(result);
    switch (i) {
      case 0:
      case 1:
      case 4:
      case 5:
        EXPECT_EQ(val, false);
        break;
      default:
        EXPECT_EQ(val, true);
        break;
    }
  }
}

// Validates encoding of values by encoding and decoding them.  If
// expected_encoding != NULL, also validates that the encoded buffer is
// exactly 'expected_encoding'.
// if expected_len is not -1, it will validate the encoded size is correct.
void ValidateRle(const vector<bool>& values,
    uint8_t* expected_encoding, int expected_len) {
  faststring buffer;
  RleEncoder encoder(&buffer);
  for (int i = 0; i < values.size(); ++i) {
    encoder.Put(values[i]);
  }
  int encoded_len = encoder.Flush();

  if (expected_len != -1) {
    EXPECT_EQ(encoded_len, expected_len);
  }
  if (expected_encoding != NULL) {
    EXPECT_TRUE(memcmp(buffer.data(), expected_encoding, expected_len) == 0);
  }

  // Verify read
  RleDecoder decoder(buffer.data(), encoded_len);
  for (int i = 0; i < values.size(); ++i) {
    bool val;
    bool result = decoder.Get(&val);
    EXPECT_TRUE(result);
    EXPECT_EQ(values[i], val);
  }
}

TEST(Rle, SpecificSequences) {
  const int len = 1024;
  uint8_t expected_buffer[len];
  vector<bool> values;

  // Test 50 0' followed by 50 1's
  values.resize(100);
  for (int i = 0; i < 50; ++i) {
    values[i] = false;
  }
  for (int i = 50; i < 100; ++i) {
    values[i] = true;
  }
  expected_buffer[0] = (50 << 1);
  expected_buffer[1] = 0;
  expected_buffer[2] = (50 << 1);
  expected_buffer[3] = 1;
  ValidateRle(values, expected_buffer, 4);

  // Test 100 0's and 1's alternating
  for (int i = 0; i < 100; ++i) {
    values[i] = i % 2;
  }
  int num_groups = BitmapSize(100);
  expected_buffer[0] = (num_groups << 1) | 1;
  for (int i = 0; i < 100/8; ++i) {
    expected_buffer[i + 1] = BOOST_BINARY(1 0 1 0 1 0 1 0);
  }
  // Values for the last 4 0 and 1's
  expected_buffer[1 + 100/8] = BOOST_BINARY(0 0 0 0 1 0 1 0);
  ValidateRle(values, expected_buffer, 1 + num_groups);
}

// Tests alternating true/false values.
TEST(Rle, AlternateTest) {
  const int len = 2048;
  vector<bool> values;
  for (int i = 0; i < len; ++i) {
    values.push_back(i % 2);
  }
  ValidateRle(values, NULL, -1);
}

class BitRle : public KuduTest {
};

// Tests all true/false values
TEST_F(BitRle, AllSame) {
  const int len = 1024;
  vector<bool> values;

  for (int v = 0; v < 2; ++v) {
    values.clear();
    for (int i = 0; i < len; ++i) {
      values.push_back(v);
    }

    ValidateRle(values, NULL, 3);
  }
}

// Test that writes out a repeated group and then a literal
// group but flush before finishing.
TEST_F(BitRle, Flush) {
  vector<bool> values;
  for (int i = 0; i < 16; ++i) values.push_back(1);
  values.push_back(0);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
  values.push_back(1);
  ValidateRle(values, NULL, -1);
}

// Test some random sequences.
TEST_F(BitRle, Random) {
  int iters = 0;
  const int n_iters = AllowSlowTests() ? 1000 : 20;
  while (iters < n_iters) {
    srand(iters++);
    if (iters % 10000 == 0) LOG(ERROR) << "Seed: " << iters;
    vector<bool> values;
    bool parity = 0;
    for (int i = 0; i < 1000; ++i) {
      int group_size = rand() % 20 + 1;
      if (group_size > 16) {
        group_size = 1;
      }
      for (int i = 0; i < group_size; ++i) {
        values.push_back(parity);
      }
      parity = !parity;
    }
    ValidateRle(values, NULL, -1);
  }
}

// Test a sequence of 1 0's, 2 1's, 3 0's. etc
// e.g. 011000111100000
TEST_F(BitRle, RepeatedPattern) {
  vector<bool> values;
  const int min_run = 1;
  const int max_run = 32;

  for (int i = min_run; i <= max_run; ++i) {
    bool v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  // And go back down again
  for (int i = max_run; i >= min_run; --i) {
    int v = i % 2;
    for (int j = 0; j < i; ++j) {
      values.push_back(v);
    }
  }

  ValidateRle(values, NULL, -1);
}

TEST(TestRle, TestBulkPut) {
  size_t run_length;
  bool val;

  faststring buffer(1);
  RleEncoder encoder(&buffer);
  encoder.Put(true, 10);
  encoder.Put(false, 7);
  encoder.Put(true, 5);
  encoder.Put(true, 15);
  encoder.Flush();

  RleDecoder decoder(buffer.data(), encoder.len());
  decoder.GetNextRun(&val, &run_length);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, run_length);

  decoder.GetNextRun(&val, &run_length);
  ASSERT_FALSE(val);
  ASSERT_EQ(7, run_length);

  decoder.GetNextRun(&val, &run_length);
  ASSERT_TRUE(val);
  ASSERT_EQ(20, run_length);

  ASSERT_FALSE(decoder.GetNextRun(&val, &run_length));
}

TEST(TestRle, TestGetNextRun) {
  // Repeat the test with different number of items
  for (int num_items = 7; num_items < 200; num_items += 13) {
    // Test different block patterns
    //    1: 01010101 01010101
    //    2: 00110011 00110011
    //    3: 00011100 01110001
    //    ...
    for (int block = 1; block <= 20; ++block) {
      faststring buffer(1);
      RleEncoder encoder(&buffer);
      for (int j = 0; j < num_items; ++j) {
        encoder.Put(!!(j & 1), block);
      }
      encoder.Flush();

      RleDecoder decoder(buffer.data(), encoder.len());
      size_t count = num_items * block;
      for (int j = 0; j < num_items; ++j) {
        size_t run_length;
        bool val;
        DCHECK_GT(count, 0);
        decoder.GetNextRun(&val, &run_length);
        run_length = std::min(run_length, count);

        ASSERT_EQ(!!(j & 1), val);
        ASSERT_EQ(block, run_length);
        count -= run_length;
      }
      DCHECK_EQ(count, 0);
    }
  }
}

TEST(TestRle, TestSkip) {
  faststring buffer(1);
  RleEncoder encoder(&buffer);

  // 0101010[1] 01010101 01
  for (int j = 0; j < 18; ++j) {
    encoder.Put(!!(j & 1));
  }

  // 0011[00] 11001100 11001100 11001100 11001100
  for (int j = 0; j < 19; ++j) {
    encoder.Put(!!(j & 1), 2);
  }

  // 000000000000 11[1111111111] 000000000000 111111111111
  // 000000000000 111111111111 0[00000000000] 111111111111
  // 000000000000 111111111111 000000000000 111111111111
  for (int j = 0; j < 12; ++j) {
    encoder.Put(!!(j & 1), 12);
  }
  encoder.Flush();

  bool val;
  size_t run_length;
  RleDecoder decoder(buffer.data(), encoder.len());

  decoder.Skip(7);
  decoder.GetNextRun(&val, &run_length);
  ASSERT_TRUE(val);
  ASSERT_EQ(1, run_length);

  decoder.Skip(14);
  decoder.GetNextRun(&val, &run_length);
  ASSERT_FALSE(val);
  ASSERT_EQ(2, run_length);

  decoder.Skip(46);
  decoder.GetNextRun(&val, &run_length);
  ASSERT_TRUE(val);
  ASSERT_EQ(10, run_length);

  decoder.Skip(49);
  decoder.GetNextRun(&val, &run_length);
  ASSERT_FALSE(val);
  ASSERT_EQ(11, run_length);

  encoder.Flush();
}

}
