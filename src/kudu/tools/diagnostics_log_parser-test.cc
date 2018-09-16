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

#include "kudu/tools/diagnostics_log_parser.h"

#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::stringstream;

namespace kudu {
namespace tools {

TEST(DiagLogParserTest, TestParseLine) {
  const auto parse_line = [&] (const string& line_str) {
    ParsedLine pl(line_str);
    return pl.Parse();
  };
  // Lines have the following format:
  // I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  // Lines begin with 'I' and detect missing fields.
  Status s = parse_line("");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "lines must start with 'I'");

  s = parse_line("X0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "lines must start with 'I'");

  s = parse_line("I0220 17:38:09.950546 metrics 1519177089950546");
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid JSON payload");
  {
    ParsedLine pl("I0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
  }
  {
    // The date and time should be parsed successfully.
    ParsedLine pl("I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    ASSERT_EQ("0220 17:38:09.950546", pl.date_time());
  }
  {
    // The line parser recognizes "stacks" and "symbols" categories.
    // "metrics" isn't recognized yet.
    ParsedLine pl("I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    ASSERT_EQ(RecordType::kStacks, pl.type());
  }
  {
    ParsedLine pl("I0220 17:38:09.950546 symbols 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    ASSERT_EQ(RecordType::kSymbols, pl.type());
  }
  {
    ParsedLine pl("I0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    ASSERT_EQ(RecordType::kMetrics, pl.type());
  }
  {
    ParsedLine pl("I0220 17:38:09.950546 foo 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    ASSERT_EQ(RecordType::kUnknown, pl.type());
  }
  {
    // The json blob must be valid json.
    ParsedLine pl("I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}");
    ASSERT_OK(pl.Parse());
    string val = (*pl.json())["foo"].GetString();
    ASSERT_EQ(val, "bar");
  }
  // The timestamp must be a number.
  s = parse_line("I0220 17:38:09.950546 stacks 1234foo567890000 {\"foo\" : \"bar\"}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "invalid timestamp");

  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : }");
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

class TestSymbolsLogVisitor : public LogVisitor {
 public:
  Status ParseRecord(const ParsedLine& pl) override {
    if (pl.type() == RecordType::kSymbols) {
      SymbolsRecord sr;
      RETURN_NOT_OK(sr.FromParsedLine(pl));
      addr_ = sr.addr_to_symbol.begin()->first;
      symbol_ = sr.addr_to_symbol.begin()->second;
    }
    return Status::OK();
  }

  string addr_;
  string symbol_;
};

TEST(DiagLogParserTest, TestParseSymbols) {
  TestSymbolsLogVisitor lv;
  const auto parse_line = [&] (const string& line_str) {
    ParsedLine pl(line_str);
    pl.Parse();
    return lv.ParseRecord(pl);
  };

  Status s = parse_line("I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : 99}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected symbol values to be strings");

  s = parse_line("I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : "
                "{ \"bar\" : \"baaz\" } }");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected symbol values to be strings");

  ASSERT_OK(parse_line("I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : \"symbol\"}"));
  ASSERT_EQ("addr", lv.addr_);
  ASSERT_EQ("symbol", lv.symbol_);
}

class TestStacksLogVisitor : public LogVisitor {
 public:
  Status ParseRecord(const ParsedLine& pl) override {
    StacksRecord sr;
    return sr.FromParsedLine(pl);
  }
};

// For parsing stacks, we'll check for success or error only. The parse_stacks
// tool's tests serve as a check that the right information is parsed.
TEST(DiagLogParserTest, TestParseStacks) {
  TestStacksLogVisitor lv;
  const auto parse_line = [&] (const string& line_str) {
    ParsedLine pl(line_str);
    pl.Parse();
    return lv.ParseRecord(pl);
  };

  // The "reason" field must be a string, if present.
  Status s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
                        "{\"reason\" : 1.2, \"groups\" : []}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks 'reason' to be a string");

  // The "groups" field must present
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 {}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "no 'groups' field in stacks object");

  // The "groups" field must an array.
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 {\"groups\" : \"foo\"}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "'groups' field should be an array");

  // A member of the groups array (a group) must be an object.
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 {\"groups\" : [\"foo\"]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to be JSON objects");

  // A group must have "tids" and stack fields.
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"stack\" : []}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to have frames and tids");

  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : 5}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to have frames and tids");

  // A group must have a "tids" field, with value a numeric array
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : 5, \"stack\" : []}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'tids' to be an array");

  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [false], \"stack\" : []}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'tids' elements to be numeric");

  // A group must have a "stack" field, with value an array of strings.
  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [], \"stack\" : 5}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'stack' to be an array");

  s = parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [], \"stack\" : [5]}]}");
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'stack' elements to be strings");

  // Happy cases with and without a "reason" field.
  ASSERT_OK(parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
      "{\"reason\" : \"test\", \"groups\" : [{\"tids\" : [0], \"stack\" : [\"stack\"]}]}"));

  ASSERT_OK(parse_line("I0220 17:38:09.950546 stacks 1519177089950546 "
      "{\"groups\" : [{\"tids\" : [0], \"stack\" : [\"stack\"]}]}"));
}

} // namespace tools
} // namespace kudu
