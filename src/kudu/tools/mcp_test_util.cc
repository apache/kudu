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

#include "kudu/tools/mcp_test_util.h"

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"

using std::string;
using std::vector;

namespace kudu {
namespace tools {

string MakeToolsCallRequest(const string& tool_name,
                            const string& arguments_json) {
  return strings::Substitute(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
      "\"params\":{\"name\":\"$0\",\"arguments\":$1}}\n",
      tool_name, arguments_json);
}

vector<string> SplitResponseLines(const string& out) {
  vector<string> lines;
  std::istringstream iss(out);
  string line;
  while (std::getline(iss, line)) {
    if (!line.empty()) {
      lines.emplace_back(std::move(line));
    }
  }
  return lines;
}

vector<string> ToolNames(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  CHECK_OK(reader.ExtractObjectArray(result, "tools", &tools));
  vector<string> names;
  names.reserve(tools.size());
  for (const auto* t : tools) {
    string n;
    CHECK_OK(reader.ExtractString(t, "name", &n));
    names.emplace_back(std::move(n));
  }
  return names;
}

int ErrorCode(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  int32_t code = 0;
  CHECK_OK(reader.ExtractInt32(err, "code", &code));
  return code;
}

string ErrorMessage(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  CHECK_OK(reader.ExtractString(err, "message", &message));
  return message;
}

ToolResult ParseToolResult(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  ToolResult tr;
  tr.is_error = false;
  // isError is present on tools/call results (false on success, true on error).
  if (result->HasMember("isError")) {
    CHECK_OK(reader.ExtractBool(result, "isError", &tr.is_error));
  }
  vector<const rapidjson::Value*> content;
  CHECK_OK(reader.ExtractObjectArray(result, "content", &content));
  CHECK(!content.empty()) << "tool result has empty content: " << line;
  string type;
  CHECK_OK(reader.ExtractString(content[0], "type", &type));
  CHECK_EQ("text", type) << line;
  CHECK_OK(reader.ExtractString(content[0], "text", &tr.text));
  return tr;
}

bool Contains(const vector<string>& v, const string& s) {
  return std::find(v.begin(), v.end(), s) != v.end();
}

} // namespace tools
} // namespace kudu
