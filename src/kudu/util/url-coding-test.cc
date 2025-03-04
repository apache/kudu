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

#include "kudu/util/url-coding.h"

#include <cstring>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

using std::ostringstream;
using std::string;
using std::vector;

namespace kudu {

// Tests encoding/decoding of input.  If expected_encoded is non-empty, the
// encoded string is validated against it.
void TestUrl(const string& input, const string& expected_encoded, bool hive_compat) {
  string intermediate;
  UrlEncode(input, &intermediate, hive_compat);
  string output;
  if (!expected_encoded.empty()) {
    EXPECT_EQ(expected_encoded, intermediate);
  }
  EXPECT_TRUE(UrlDecode(intermediate, &output, hive_compat));
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> input_vector(input.size());
  if (!input.empty()) {
    memcpy(&input_vector[0], input.c_str(), input.size());
  }
  string intermediate2;
  UrlEncode(input_vector, &intermediate2, hive_compat);
  EXPECT_EQ(intermediate, intermediate2);
}

void TestBase64(const string& input, const string& expected_encoded) {
  string intermediate;
  Base64Encode(input, &intermediate);
  string output;
  if (!expected_encoded.empty()) {
    EXPECT_EQ(intermediate, expected_encoded);
  }
  EXPECT_TRUE(Base64Decode(intermediate, &output));
  EXPECT_EQ(input, output);

  // Convert string to vector and try that also
  vector<uint8_t> input_vector(input.size());
  if (!input_vector.empty()) {
    memcpy(&input_vector[0], input.c_str(), input.size());
  }
  string intermediate2;
  Base64Encode(input_vector, &intermediate2);
  EXPECT_EQ(intermediate, intermediate2);
}

// Test URL encoding. Check that the values that are put in are the
// same that come out.
TEST(UrlCodingTest, Basic) {
  string input = "ABCDEFGHIJKLMNOPQRSTUWXYZ1234567890~!@#$%^&*()<>?,./:\";'{}|[]\\_+-=";
  TestUrl(input, "", false);
  TestUrl(input, "", true);
}

TEST(UrlCodingTest, HiveExceptions) {
  TestUrl(" +", " +", true);
}

TEST(UrlCodingTest, BlankString) {
  TestUrl("", "", false);
  TestUrl("", "", true);
}

TEST(UrlCodingTest, PathSeparators) {
  TestUrl("/home/impala/directory/", "%2Fhome%2Fimpala%2Fdirectory%2F", false);
  TestUrl("/home/impala/directory/", "%2Fhome%2Fimpala%2Fdirectory%2F", true);
}

TEST(Base64Test, Basic) {
  TestBase64("", "");
  TestBase64("a", "YQ==");
  TestBase64(string("a\0", 2), "YQA=");
  TestBase64(string("\0a", 2), "AGE=");
  TestBase64(string("a\0\0", 3), "YQAA");
  TestBase64(string("\0a\0", 3), "AGEA");
  TestBase64(string("\0 a \0", 5), "ACBhIAA=");
  TestBase64(string(" \0a\0 ", 5), "IABhACA=");
  TestBase64("ab", "YWI=");
  TestBase64(string("ab\0", 3), "YWIA");
  TestBase64("a a", "YSBh");
  TestBase64("abc", "YWJj");
  TestBase64("a a ", "YSBhIA==");
  TestBase64(string("a a \0", 5), "YSBhIAA=");
  TestBase64(string("a a \0\0", 6), "YSBhIAAA");
  TestBase64(string("\0a\0 \0a\0 \0\0", 10), "AGEAIABhACAAAA==");
  TestBase64("abcd", "YWJjZA==");
  TestBase64(string("abcd\0", 5), "YWJjZAA=");
  TestBase64("abcde", "YWJjZGU=");
  TestBase64(string("abcde\0", 6), "YWJjZGUA");
  TestBase64("abcdef", "YWJjZGVm");
  TestBase64(string("a\0b", 3), "YQBi");
  TestBase64(string("a\0b\0", 4), "YQBiAA==");
  TestBase64(string("a\0b\0\0", 5), "YQBiAAA=");
  TestBase64(string("a\0b\0\0\0", 6), "YQBiAAAA");
}

TEST(Base64DecodeTest, InvalidInput) {
  // These should fail to decode.
  string out;
  ASSERT_FALSE(Base64Decode("A", &out));
  ASSERT_FALSE(Base64Decode("=", &out));
  ASSERT_FALSE(Base64Decode("A=", &out));
  ASSERT_FALSE(Base64Decode("==", &out));
  ASSERT_FALSE(Base64Decode("A==", &out));
  ASSERT_FALSE(Base64Decode("=A=", &out));
  ASSERT_FALSE(Base64Decode("===", &out));
  ASSERT_FALSE(Base64Decode("A===", &out));
  ASSERT_FALSE(Base64Decode("====", &out));
  ASSERT_FALSE(Base64Decode("A====", &out));
  ASSERT_FALSE(Base64Decode("==A==", &out));
  ASSERT_FALSE(Base64Decode("YQ=", &out));
  ASSERT_FALSE(Base64Decode("=W=", &out));
  ASSERT_FALSE(Base64Decode("YQW==", &out));
  ASSERT_FALSE(Base64Decode("=YQW=", &out));
  ASSERT_FALSE(Base64Decode("=Y=W=", &out));
  ASSERT_FALSE(Base64Decode("ADCD=", &out));
  ASSERT_FALSE(Base64Decode("ADCD==", &out));
  ASSERT_FALSE(Base64Decode("ADCD===", &out));
}

TEST(Base64DecodeTest, InvalidInputMisplacedPaddingSymbols) {
#if !DCHECK_IS_ON()
  GTEST_SKIP() << "this test is only for DCHECK_IS_ON()";
#endif
  string out;
  ASSERT_DEATH(({ Base64Decode("=a",        &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("==a",       &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("===a",      &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("=A==",      &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("==A=",      &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("YQA=YQA=",  &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("YQA=ABCD",  &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("YQA==ABCD", &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("=QA=ABCD",  &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("YQ==YQ==",  &out); }), "non-trailing '='");
  ASSERT_DEATH(({ Base64Decode("YQ==YQA=",  &out); }), "non-trailing '='");
}

TEST(HtmlEscapingTest, Basic) {
  string before = "<html><body>&amp";
  ostringstream after;
  EscapeForHtml(before, &after);
  EXPECT_EQ(after.str(), "&lt;html&gt;&lt;body&gt;&amp;amp");
}

} // namespace kudu
