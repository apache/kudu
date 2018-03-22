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

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

namespace kudu {
namespace tools {

using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

TEST(DiagLogParserTest, TestParseLine) {
  // Lines have the following format:
  // I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  {
    // Lines begin with 'I' and detect missing fields.
    ParsedLine pl;
    string line = "";
    Status s = pl.Parse(line);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "lines must start with 'I'");

    line = "X0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}";
    s = pl.Parse(line);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "lines must start with 'I'");

    line = "I0220 17:38:09.950546 metrics 1519177089950546";
    s = pl.Parse(line);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_STR_CONTAINS(s.ToString(), "invalid JSON payload");

    line = "I0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
  }

  {
    // The date and time should be parsed successfully.
    ParsedLine pl;
    string line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    ASSERT_EQ("0220 17:38:09.950546", pl.date_time());
  }

  {
    // The line parser recognizes "stacks" and "symbols" categories.
    // "metrics" isn't recognized yet.
    ParsedLine pl;
    string line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    ASSERT_EQ(RecordType::kStacks, pl.type());

    line = "I0220 17:38:09.950546 symbols 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    ASSERT_EQ(RecordType::kSymbols, pl.type());

    line = "I0220 17:38:09.950546 metrics 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    ASSERT_EQ(RecordType::kUnknown, pl.type());

    line = "I0220 17:38:09.950546 foo 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    ASSERT_EQ(RecordType::kUnknown, pl.type());
  }

  {
    // The timestamp must be a number.
    ParsedLine pl;
    string line = "I0220 17:38:09.950546 stacks 1234foo567890000 {\"foo\" : \"bar\"}";
    Status s = pl.Parse(line);
    ASSERT_TRUE(s.IsInvalidArgument());
    ASSERT_STR_CONTAINS(s.ToString(), "invalid timestamp");
  }

  {
    // The json blob must be valid json.
    ParsedLine pl;
    string line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : \"bar\"}";
    ASSERT_OK(pl.Parse(line));
    string val = (*pl.json())["foo"].GetString();
    ASSERT_EQ(val, "bar");

    line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"foo\" : }";
    Status s = pl.Parse(line);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

class TestSymbolsLogVisitor : public LogVisitor {
 public:
  void VisitSymbol(const string& addr, const string& symbol) override {
    addr_ = addr;
    symbol_ = symbol;
  }

  void VisitStacksRecord(const StacksRecord& /*sr*/) override {}

  string addr_;
  string symbol_;
};

TEST(DiagLogParserTest, TestParseSymbols) {
  TestSymbolsLogVisitor lv;
  LogParser lp(&lv);

  string line = "I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : 99}";
  Status s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected symbol values to be strings");

  line = "I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : { \"bar\" : \"baaz\" } }";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected symbol values to be strings");

  line = "I0220 17:38:09.950546 symbols 1519177089950546 {\"addr\" : \"symbol\"}";
  ASSERT_OK(lp.ParseLine(line));
  ASSERT_EQ("addr", lv.addr_);
  ASSERT_EQ("symbol", lv.symbol_);
}

class NoopLogVisitor : public LogVisitor {
 public:
  void VisitSymbol(const string& /*addr*/, const string& /*symbol*/) override {}
  void VisitStacksRecord(const StacksRecord& /*sr*/) override {}
};

// For parsing stacks, we'll check for success or error only. The parse_stacks
// tool's tests serve as a check that the right information is parsed.
TEST(DiagLogParserTest, TestParseStacks) {
  NoopLogVisitor lv;
  LogParser lp(&lv);

  // The "reason" field must be a string, if present.
  string line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"reason\" : 1.2, \"groups\" : []}";
  Status s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks 'reason' to be a string");

  // The "groups" field must present
  line = "I0220 17:38:09.950546 stacks 1519177089950546 {}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "no 'groups' field in stacks object");

  // The "groups" field must an array.
  line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"groups\" : \"foo\"}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "'groups' field should be an array");

  // A member of the groups array (a group) must be an object.
  line = "I0220 17:38:09.950546 stacks 1519177089950546 {\"groups\" : [\"foo\"]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to be JSON objects");

  // A group must have "tids" and stack fields.
  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"stack\" : []}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to have frames and tids");

  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : 5}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected stacks groups to have frames and tids");

  // A group must have a "tids" field, with value a numeric array
  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : 5, \"stack\" : []}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'tids' to be an array");

  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [false], \"stack\" : []}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'tids' elements to be numeric");

  // A group must have a "stack" field, with value an array of strings.
  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [], \"stack\" : 5}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'stack' to be an array");

  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [], \"stack\" : [5]}]}";
  s = lp.ParseLine(line);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STR_CONTAINS(s.ToString(), "expected 'stack' elements to be strings");

  // Happy cases with and without a "reason" field.
  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"reason\" : \"test\", \"groups\" : [{\"tids\" : [0], \"stack\" : [\"stack\"]}]}";
  ASSERT_OK(lp.ParseLine(line));

  line = "I0220 17:38:09.950546 stacks 1519177089950546 "
         "{\"groups\" : [{\"tids\" : [0], \"stack\" : [\"stack\"]}]}";
  ASSERT_OK(lp.ParseLine(line));
}

} // namespace tools
} // namespace kudu
