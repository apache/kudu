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

// Client-side JSON-RPC helpers shared by the MCP server's unit test
// (tool_action_mcp-test.cc) and integration test (tool_action_mcp-itest.cc).
//
// These build requests and parse responses using JsonReader only -- they share
// no code with the server's own response writer (tool_action_mcp.cc), so the
// tests remain an independent oracle of the wire format: a bug in the server's
// framing cannot be masked by a helper that mirrors it.

#include <string>
#include <vector>

namespace kudu {
namespace tools {

// The parsed MCP tool result from a 'tools/call' JSON-RPC response. This
// deliberately mirrors (rather than reuses) the server's ToolOutcome in
// tool_action_mcp.cc: that is the value before JSON serialization, this is the
// value parsed back off the wire. Keeping the test's parse-side type
// independent of the server's type is what makes these helpers an independent
// oracle of the wire format (see the file comment above), so a framing bug in
// the server cannot be masked by a helper that reuses the server's own type.
struct ToolResult {
  bool is_error;
  std::string text;  // content[0].text
};

// Builds a newline-terminated 'tools/call' JSON-RPC request line. 'arguments_json'
// is the raw JSON object for params.arguments (e.g. R"({"table_name":"t"})").
std::string MakeToolsCallRequest(const std::string& tool_name,
                                 const std::string& arguments_json);

// Splits newline-delimited server output into individual JSON-RPC lines,
// dropping empty lines (the server emits exactly one response line per request
// and nothing for notifications, so interior blanks never carry content).
std::vector<std::string> SplitResponseLines(const std::string& out);

// Collects the "name" of every tool in a 'tools/list' response 'line'.
std::vector<std::string> ToolNames(const std::string& line);

// Extracts the integer error code from a JSON-RPC error response 'line'.
// CHECK-fails if the line is not an error envelope.
int ErrorCode(const std::string& line);

// Extracts the error 'message' string from a JSON-RPC error response 'line'.
std::string ErrorMessage(const std::string& line);

// Parses a 'tools/call' success response 'line' into its MCP tool result. Fails
// (via CHECK) if the line is not a well-formed result carrying a text
// content[0]; this doubles as the assertion that the protocol line is valid JSON.
ToolResult ParseToolResult(const std::string& line);

// True if 'v' contains 's'.
bool Contains(const std::vector<std::string>& v, const std::string& s);

} // namespace tools
} // namespace kudu
