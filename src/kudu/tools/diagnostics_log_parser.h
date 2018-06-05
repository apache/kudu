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

#pragma once

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// One of the record types from the log.
// TODO(KUDU-2353) support metrics records.
enum class RecordType {
  kSymbols,
  kStacks,
  kUnknown
};

const char* RecordTypeToString(RecordType r);

std::ostream& operator<<(std::ostream& o, RecordType r);

// A stack sample from the log.
struct StacksRecord {
  // A group of threads which share the same stack trace.
  struct Group {
    // The thread IDs in this group.
    std::vector<int> tids;
    // The non-symbolized addresses forming the stack trace.
    std::vector<std::string> frame_addrs;
  };

  // The time the stack traces were collected.
  std::string date_time;

  // The reason for stack trace collection.
  std::string reason;

  // The grouped threads with their stack traces.
  std::vector<Group> groups;
};

// Interface for consuming the parsed records from a diagnostics log.
class LogVisitor {
 public:
  virtual ~LogVisitor() {}
  virtual void VisitSymbol(const std::string& addr, const std::string& symbol) = 0;
  virtual void VisitStacksRecord(const StacksRecord& sr) = 0;
};

// LogVisitor implementation which dumps the parsed stack records to cout.
class StackDumpingLogVisitor : public LogVisitor {
 public:
  void VisitSymbol(const std::string& addr, const std::string& symbol) override;

  void VisitStacksRecord(const StacksRecord& sr) override;

 private:
  // True when we have not yet output any data.
  bool first_ = true;
  // Map from symbols to name.
  std::unordered_map<std::string, std::string> symbols_;

  const std::string kUnknownSymbol = "<unknown>";
};

// A parsed line from the diagnostics log.
//
// Each line contains a timestamp, a record type, and some JSON data.
class ParsedLine {
 public:
  // Parse a line from the diagnostics log.
  Status Parse(std::string line);

  RecordType type() const { return type_; }

  const rapidjson::Value* json() const {
    CHECK(json_);
    return json_->root();
  }

  std::string date_time() const;

 private:
  std::string line_;
  RecordType type_;

  // date_ and time_ point to substrings of line_.
  StringPiece date_;
  StringPiece time_;

  // A JsonReader initialized from the most recent line.
  // This will be 'none' before any lines have been read.
  boost::optional<JsonReader> json_;
};

// Parser for a metrics log.
//
// Each line should be fed to LogParser::ParseLine().
//
// This instance follows a 'SAX' model. As records are available, the appropriate
// functions are invoked on the visitor provided in the constructor.
class LogParser {
 public:
  explicit LogParser(LogVisitor* visitor);

  // Parse the next line of the log. This function may invoke the appropriate
  // visitor functions zero or more times.
  Status ParseLine(std::string line);

 private:
  Status ParseSymbols(const ParsedLine& lf);

  static Status ParseStackGroup(const rapidjson::Value& group_json,
                                StacksRecord::Group* group);

  Status ParseStacks(const ParsedLine& lf);

  LogVisitor* visitor_;
};

} // namespace tools
} // namespace kudu
