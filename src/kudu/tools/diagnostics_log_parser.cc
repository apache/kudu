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

#include <array>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

using std::array;
using std::cout;
using std::endl;
using std::ifstream;
using std::string;
using strings::Substitute;

namespace kudu {
namespace tools {

const char* RecordTypeToString(RecordType r) {
  switch (r) {
    case RecordType::kStacks: return "stacks"; break;
    case RecordType::kSymbols: return "symbols"; break;
    case RecordType::kUnknown: return "<unknown>"; break;
  }
  return "<unreachable>";
}

std::ostream& operator<<(std::ostream& o, RecordType r) {
  return o << RecordTypeToString(r);
}

void StackDumpingLogVisitor::VisitSymbol(const string& addr, const string& symbol) {
  InsertIfNotPresent(&symbols_, addr, symbol);
}

void StackDumpingLogVisitor::VisitStacksRecord(const StacksRecord& sr) {
  if (!first_) {
    cout << endl << endl;
  }
  first_ = false;
  cout << "Stacks at " << sr.date_time << " (" << sr.reason << "):" << endl;
  for (const auto& group : sr.groups) {
    cout << "  tids=["
          << JoinMapped(group.tids, [](int t) { return std::to_string(t); }, ",")
          << "]" << endl;
    for (const auto& addr : group.frame_addrs) {
      // NOTE: passing 'kUnknownSymbols' as the default instead of a "foo"
      // literal is important to avoid capturing a reference to a temporary.
      // See the FindWithDefault() docs for details.
      const auto& sym = FindWithDefault(symbols_, addr, kUnknownSymbol);
      cout << std::setw(20) << addr << " " << sym << endl;
    }
  }
}

Status ParsedLine::Parse(string line) {
  // Take ownership of the line to avoid copying substrings.
  line_ = std::move(line);

  // Lines have the following format:
  //
  //     I0220 17:38:09.950546 metrics 1519177089950546 <json blob>...
  //
  if (line_.empty() || line_[0] != 'I') {
    return Status::InvalidArgument("lines must start with 'I'");
  }

  array<StringPiece, 5> fields = strings::Split(
      line_, strings::delimiter::Limit(" ", 4));
  fields[0].remove_prefix(1); // Remove the 'I'.
  // Sanity check the microsecond timestamp.
  // Eventually, it should be used when processing metrics records.
  int64_t time_us;
  if (!safe_strto64(fields[3].data(), fields[3].size(), &time_us)) {
    return Status::InvalidArgument("invalid timestamp", fields[3]);
  }
  // TODO(todd) JsonReader should be able to parse from a StringPiece
  // directly instead of making the copy here.
  json_.emplace(fields[4].ToString());
  Status s = json_->Init();
  if (!s.ok()) {
    json_ = boost::none;
    return s.CloneAndPrepend("invalid JSON payload");
  }
  date_ = fields[0];
  time_ = fields[1];
  if (fields[2] == "symbols") {
    type_ = RecordType::kSymbols;
  } else if (fields[2] == "stacks") {
    type_ = RecordType::kStacks;
  } else {
    type_ = RecordType::kUnknown;
  }
  return Status::OK();
}

string ParsedLine::date_time() const {
  return Substitute("$0 $1", date_, time_);
}

LogParser::LogParser(LogVisitor* visitor)
    : visitor_(CHECK_NOTNULL(visitor)) {
}

Status LogParser::ParseLine(string line) {
  ParsedLine pl;
  RETURN_NOT_OK(pl.Parse(std::move(line)));
  switch (pl.type()) {
    case RecordType::kSymbols:
      RETURN_NOT_OK(ParseSymbols(pl));
      break;
    case RecordType::kStacks: {
      RETURN_NOT_OK(ParseStacks(pl));
      break;
    }
    default:
      break;
  }
  return Status::OK();
}

Status LogParser::ParseSymbols(const ParsedLine& pl) {
  CHECK_EQ(RecordType::kSymbols, pl.type());
  if (!pl.json()->IsObject()) {
    return Status::InvalidArgument("expected symbols data to be a JSON object");
  }
  for (auto it = pl.json()->MemberBegin();
       it != pl.json()->MemberEnd();
       ++it) {
    if (PREDICT_FALSE(!it->value.IsString())) {
      return Status::InvalidArgument("expected symbol values to be strings");
    }
    visitor_->VisitSymbol(it->name.GetString(), it->value.GetString());
  }

  return Status::OK();
}

Status LogParser::ParseStackGroup(const rapidjson::Value& group_json,
                                  StacksRecord::Group* group) {
  DCHECK(group);
  StacksRecord::Group ret;
  if (PREDICT_FALSE(!group_json.IsObject())) {
    return Status::InvalidArgument("expected stacks groups to be JSON objects");
  }
  if (!group_json.HasMember("tids") || !group_json.HasMember("stack")) {
    return Status::InvalidArgument("expected stacks groups to have frames and tids");
  }

  // Parse the tids.
  const auto& tids = group_json["tids"];
  if (PREDICT_FALSE(!tids.IsArray())) {
    return Status::InvalidArgument("expected 'tids' to be an array");
  }
  ret.tids.reserve(tids.Size());
  for (const auto* tid = tids.Begin();
       tid != tids.End();
       ++tid) {
    if (PREDICT_FALSE(!tid->IsNumber())) {
      return Status::InvalidArgument("expected 'tids' elements to be numeric");
    }
    ret.tids.push_back(tid->GetInt64());
  }

  // Parse and symbolize the stack trace itself.
  const auto& stack = group_json["stack"];
  if (PREDICT_FALSE(!stack.IsArray())) {
    return Status::InvalidArgument("expected 'stack' to be an array");
  }
  for (const auto* frame = stack.Begin();
       frame != stack.End();
       ++frame) {
    if (PREDICT_FALSE(!frame->IsString())) {
      return Status::InvalidArgument("expected 'stack' elements to be strings");
    }
    ret.frame_addrs.emplace_back(frame->GetString());
  }
  *group = std::move(ret);
  return Status::OK();
}

Status LogParser::ParseStacks(const ParsedLine& pl) {
  StacksRecord sr;
  sr.date_time = pl.date_time();

  const rapidjson::Value& json = *pl.json();
  if (!json.IsObject()) {
    return Status::InvalidArgument("expected stacks data to be a JSON object");
  }

  // Parse reason if present. If not, we'll just leave it empty.
  if (json.HasMember("reason")) {
    if (!json["reason"].IsString()) {
      return Status::InvalidArgument("expected stacks 'reason' to be a string");
    }
    sr.reason = json["reason"].GetString();
  }

  // Parse groups.
  if (PREDICT_FALSE(!json.HasMember("groups"))) {
    return Status::InvalidArgument("no 'groups' field in stacks object");
  }
  const auto& groups = json["groups"];
  if (!groups.IsArray()) {
    return Status::InvalidArgument("'groups' field should be an array");
  }

  for (const rapidjson::Value* group = groups.Begin();
       group != groups.End();
       ++group) {
    StacksRecord::Group g;
    RETURN_NOT_OK(ParseStackGroup(*group, &g));
    sr.groups.emplace_back(std::move(g));
  }
  visitor_->VisitStacksRecord(std::move(sr));
  return Status::OK();
}

} // namespace tools
} // namespace kudu

