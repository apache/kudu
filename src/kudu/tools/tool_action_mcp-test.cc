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

#include "kudu/tools/tool_action_mcp.h"

#include <stdlib.h>
#include <sys/wait.h>

#include <algorithm>
#include <csignal>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <istream>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/mcp_test_util.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_mcp-internal.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

// The write and data-read gates, defined in tool_action_mcp.cc. Toggled
// in-process to exercise the gated tool listings.
DECLARE_bool(mcp_allow_writes);
DECLARE_bool(mcp_allow_data_reads);
// Client-side connection flag forwarded from 'serve' to tool children.
DECLARE_string(sasl_protocol_name);

// The serve-level master addresses, injected into tools/call dispatch. Set
// in-process to exercise the injection path (and its "not configured" error).
DECLARE_string(master_addresses);

using std::istringstream;
using std::pair;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// Runs the serve loop over 'input' and returns the response lines. Passes an
// empty 'kudu_binary_path': these tests exercise the handshake, tools/list,
// argument validation, dry-run, and gated rejection -- none of which spawn a
// child -- so no real 'kudu' binary is needed (the tests that actually execute
// an action live in tool_action_mcp-itest.cc).
vector<string> RunAndCollect(const string& input) {
  istringstream in(input);
  ostringstream out;
  CHECK(RunMcpServeLoop(in, out, /*kudu_binary_path=*/"").ok());
  return SplitResponseLines(out.str());
}

// Asserts that 'line' is a well-formed JSON-RPC 2.0 envelope: it parses, is an
// object, carries "jsonrpc":"2.0", and does not contain both 'result' and
// 'error'. On success, sets '*reader' up so callers can inspect it further.
void AssertValidEnvelope(const string& line, JsonReader* reader) {
  ASSERT_OK(reader->Init());
  const rapidjson::Value* root = reader->root();
  ASSERT_TRUE(root->IsObject()) << "response is not a JSON object: " << line;

  string jsonrpc;
  ASSERT_OK(reader->ExtractString(root, "jsonrpc", &jsonrpc));
  ASSERT_EQ("2.0", jsonrpc);

  const bool has_result = root->HasMember("result");
  const bool has_error = root->HasMember("error");
  ASSERT_FALSE(has_result && has_error)
      << "response carries both result and error: " << line;
  ASSERT_TRUE(has_result || has_error)
      << "response carries neither result nor error: " << line;
}

// Runs a single 'tools/list' request through the loop and returns the one
// response line. 'allow_writes' / 'allow_data_reads' toggle the gates around
// the call (saved and restored) to exercise them in-process.
string RunToolsList(bool allow_writes, bool allow_data_reads = false) {
  const bool saved_writes = FLAGS_mcp_allow_writes;
  const bool saved_data_reads = FLAGS_mcp_allow_data_reads;
  FLAGS_mcp_allow_writes = allow_writes;
  FLAGS_mcp_allow_data_reads = allow_data_reads;
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n");
  FLAGS_mcp_allow_writes = saved_writes;
  FLAGS_mcp_allow_data_reads = saved_data_reads;
  CHECK_EQ(1, lines.size());
  return lines[0];
}

// Runs a single 'tools/call' request through the loop and returns the one
// response line. 'arguments_json' is the raw JSON object for params.arguments.
string RunToolsCall(const string& tool_name, const string& arguments_json) {
  vector<string> lines = RunAndCollect(MakeToolsCallRequest(tool_name, arguments_json));
  CHECK_EQ(1, lines.size()) << "expected exactly one response line";
  return lines[0];
}

// RAII helper: sets FLAGS_master_addresses for the duration of a test and
// restores the prior value on destruction, so injection-path tests do not leak
// state into other tests.
class ScopedMasterAddresses {
 public:
  explicit ScopedMasterAddresses(const string& value) : saved_(FLAGS_master_addresses) {
    FLAGS_master_addresses = value;
  }
  ~ScopedMasterAddresses() { FLAGS_master_addresses = saved_; }
 private:
  const string saved_;
};

// RAII helper: toggles FLAGS_mcp_allow_writes for the duration of a test (so
// the serve loop surfaces GATED tools and the dispatch path admits them) and
// restores the prior value on destruction.
class ScopedAllowWrites {
 public:
  explicit ScopedAllowWrites(bool value) : saved_(FLAGS_mcp_allow_writes) {
    FLAGS_mcp_allow_writes = value;
  }
  ~ScopedAllowWrites() { FLAGS_mcp_allow_writes = saved_; }
 private:
  const bool saved_;
};

// One action of the real tree, as the registry builder sees it.
struct VisitedAction {
  string tool_name;
  const Action* action;
  DispositionInfo info;
};

// Collects every action of the tree rooted at 'root' via the shared
// VisitActions() walk -- the same traversal the registry builder uses -- so the
// test sees exactly the action set the server classifies.
vector<VisitedAction> CollectActions(const Mode* root) {
  vector<VisitedAction> out;
  CHECK_OK(VisitActions(root, [&](const vector<Mode*>& chain,
                                  const Action* action) {
    string name = DispositionCommandPath(chain, action);
    std::replace(name.begin(), name.end(), ' ', '_');
    out.push_back({ std::move(name), action, DispositionFor(action) });
    return Status::OK();
  }));
  return out;
}

// Returns the 'error.message' of a JSON-RPC error response line.
string ErrorMessageOf(const string& line) {
  JsonReader reader(line);
  CHECK_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  CHECK_OK(reader.ExtractString(err, "message", &message));
  return message;
}

// Returns the inputSchema of tool 'tool_name' from a tools/list response line
// owned by 'reader', or nullptr if the tool is absent.
const rapidjson::Value* FindToolSchema(const JsonReader& reader,
                                       const string& tool_name) {
  const rapidjson::Value* result = nullptr;
  CHECK_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  CHECK_OK(reader.ExtractObjectArray(result, "tools", &tools));
  for (const auto* t : tools) {
    string n;
    CHECK_OK(reader.ExtractString(t, "name", &n));
    if (n == tool_name) {
      const rapidjson::Value* schema = nullptr;
      CHECK_OK(reader.ExtractObject(t, "inputSchema", &schema));
      return schema;
    }
  }
  return nullptr;
}

} // anonymous namespace

// Happy path: 'initialize' returns protocolVersion, serverInfo, and a tools
// capability, and echoes the request id.
TEST(ToolActionMcpTest, InitializeReturnsHandshake) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"2025-06-18\"}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* root = reader.root();

  // id echoed back as the number it was sent as.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(root, "id", &id));
  ASSERT_EQ(1, id);

  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(root, "result", &result));

  string protocol_version;
  ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
  ASSERT_EQ("2025-06-18", protocol_version);

  // serverInfo.name / serverInfo.version present.
  const rapidjson::Value* server_info = nullptr;
  ASSERT_OK(reader.ExtractObject(result, "serverInfo", &server_info));
  string name;
  ASSERT_OK(reader.ExtractString(server_info, "name", &name));
  ASSERT_EQ("kudu-mcp", name);
  string version;
  ASSERT_OK(reader.ExtractString(server_info, "version", &version));
  ASSERT_FALSE(version.empty());

  // capabilities.tools present (an object).
  const rapidjson::Value* capabilities = nullptr;
  ASSERT_OK(reader.ExtractObject(result, "capabilities", &capabilities));
  const rapidjson::Value* tools = nullptr;
  ASSERT_OK(reader.ExtractObject(capabilities, "tools", &tools));
  // v1 declares no resources capability.
  ASSERT_FALSE(capabilities->HasMember("resources"));
}

// A client requesting a supported non-default revision has it echoed back
// verbatim. Guards the supported set in IsSupportedProtocolVersion against
// accidental removal of a revision we advertise.
TEST(ToolActionMcpTest, InitializeEchoesSupportedNonDefaultVersion) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"2024-11-05\"}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  string protocol_version;
  ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
  ASSERT_EQ("2024-11-05", protocol_version);
}

// When the client requests an unsupported protocolVersion, the server replies
// with its own default supported version rather than echoing the request.
TEST(ToolActionMcpTest, InitializeFallsBackToServerVersionWhenUnsupported) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"initialize\","
      "\"params\":{\"protocolVersion\":\"1999-01-01\"}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  string protocol_version;
  ASSERT_OK(reader.ExtractString(result, "protocolVersion", &protocol_version));
  ASSERT_NE("1999-01-01", protocol_version);
  ASSERT_FALSE(protocol_version.empty());
}

// A string id is echoed back exactly as a string.
TEST(ToolActionMcpTest, StringIdIsEchoedExactly) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":\"abc-123\",\"method\":\"initialize\","
      "\"params\":{}}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  string id;
  ASSERT_OK(reader.ExtractString(reader.root(), "id", &id));
  ASSERT_EQ("abc-123", id);
}

// A numeric id is echoed back as a number (not stringified).
TEST(ToolActionMcpTest, NumericIdIsEchoedAsNumber) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  // Must extract as an int, proving it was written as a JSON number.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
  ASSERT_EQ(42, id);
}

// An explicitly-null id still counts as present per JSON-RPC, so the line is a
// request (not a notification) and is answered with the id echoed back as JSON
// null. This exercises the null path through the shared response envelope.
TEST(ToolActionMcpTest, NullIdIsEchoedAsNullAndAnswered) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* root = reader.root();
  ASSERT_TRUE(root->HasMember("id"));
  ASSERT_TRUE((*root)["id"].IsNull()) << lines[0];
}

// Happy path: 'tools/list' reflects the action tree into MCP tool
// objects, filtered by the disposition table. Without --mcp_allow_writes the list
// is non-empty and SURFACE-only. Every tool is well-formed: a non-empty name
// and an inputSchema object whose type is "object".
TEST(ToolActionMcpTest, ToolsListReflectsSurfaceToolsByDefault) {
  const string line = RunToolsList(/*allow_writes=*/false);

  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
  ASSERT_FALSE(tools.empty());

  for (const auto* t : tools) {
    string name;
    ASSERT_OK(reader.ExtractString(t, "name", &name));
    ASSERT_FALSE(name.empty()) << "tool with empty name in: " << line;
    const rapidjson::Value* schema = nullptr;
    ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
    string type;
    ASSERT_OK(reader.ExtractString(schema, "type", &type));
    ASSERT_EQ("object", type) << "tool '" << name << "' inputSchema.type";
  }

  // A representative SURFACE read is present; a GATED write is not (no gate).
  const vector<string> names = ToolNames(line);
  ASSERT_TRUE(Contains(names, "table_describe")) << line;
  ASSERT_TRUE(Contains(names, "master_status")) << line;
  ASSERT_FALSE(Contains(names, "table_delete")) << line;
}

// Happy path: a known SURFACE read action emits a valid JSON Schema:
// - inputSchema.type == "object";
// - its required positional arg is a string property listed in "required";
// - an optional bool flag maps to a "boolean" property NOT in "required".
TEST(ToolActionMcpTest, SurfaceToolInputSchemaIsWellFormed) {
  const string line = RunToolsList(/*allow_writes=*/false);

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* describe = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_describe") {
      describe = t;
      break;
    }
  }
  ASSERT_NE(nullptr, describe) << "table_describe not surfaced: " << line;

  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(describe, "inputSchema", &schema));
  string type;
  ASSERT_OK(reader.ExtractString(schema, "type", &type));
  ASSERT_EQ("object", type);

  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));

  // Required arg "table_name" is a string property.
  ASSERT_TRUE(props->HasMember("table_name")) << line;
  const rapidjson::Value* table_name = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "table_name", &table_name));
  string table_name_type;
  ASSERT_OK(reader.ExtractString(table_name, "type", &table_name_type));
  ASSERT_EQ("string", table_name_type);

  // "table_name" is listed in required[].
  vector<const rapidjson::Value*> required;
  ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
  bool found_required = false;
  for (const auto* r : required) {
    ASSERT_TRUE(r->IsString());
    if (string(r->GetString()) == "table_name") {
      found_required = true;
    }
  }
  ASSERT_TRUE(found_required) << "table_name not in required[]: " << line;

  // Optional bool flag "show_attributes" maps to a boolean property and is NOT
  // required.
  ASSERT_TRUE(props->HasMember("show_attributes")) << line;
  const rapidjson::Value* show_attributes = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "show_attributes", &show_attributes));
  string show_attributes_type;
  ASSERT_OK(reader.ExtractString(show_attributes, "type", &show_attributes_type));
  ASSERT_EQ("boolean", show_attributes_type);
  for (const auto* r : required) {
    ASSERT_NE("show_attributes", string(r->GetString()));
  }
}

// Happy path: an optional integer gflag maps to an "integer" JSON Schema
// type (proving the gflag-type -> JSON-type mapping beyond bool). 'table scan'
// is SURFACE (behind the data-read gate) and declares the int32 flag
// 'scan_batch_size'.
TEST(ToolActionMcpTest, OptionalIntegerFlagMapsToIntegerType) {
  const string line =
      RunToolsList(/*allow_writes=*/false, /*allow_data_reads=*/true);

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* scan = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_scan") {
      scan = t;
      break;
    }
  }
  ASSERT_NE(nullptr, scan) << "table_scan not surfaced: " << line;

  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(scan, "inputSchema", &schema));
  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
  ASSERT_TRUE(props->HasMember("scan_batch_size")) << line;
  const rapidjson::Value* batch = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "scan_batch_size", &batch));
  string batch_type;
  ASSERT_OK(reader.ExtractString(batch, "type", &batch_type));
  ASSERT_EQ("integer", batch_type);
}

// Happy path: flipping FLAGS_mcp_allow_writes surfaces GATED tools, and a
// GATED tool carries the not-read-only / destructive annotations.
TEST(ToolActionMcpTest, GatedToolsAppearOnlyWithAllowWritesAndAreTagged) {
  // Absent when the gate is closed.
  ASSERT_FALSE(Contains(ToolNames(RunToolsList(false)), "table_delete"));

  // Present when the gate is open.
  const string line = RunToolsList(/*allow_writes=*/true);
  const vector<string> names = ToolNames(line);
  ASSERT_TRUE(Contains(names, "table_delete")) << line;
  // SURFACE tools are still present alongside the GATED ones.
  ASSERT_TRUE(Contains(names, "table_describe")) << line;

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* del = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_delete") {
      del = t;
      break;
    }
  }
  ASSERT_NE(nullptr, del);

  const rapidjson::Value* annotations = nullptr;
  ASSERT_OK(reader.ExtractObject(del, "annotations", &annotations));
  bool read_only = true;
  ASSERT_OK(reader.ExtractBool(annotations, "readOnlyHint", &read_only));
  ASSERT_FALSE(read_only) << "GATED tool must not be read-only: " << line;
  bool destructive = false;
  ASSERT_OK(reader.ExtractBool(annotations, "destructiveHint", &destructive));
  ASSERT_TRUE(destructive) << "GATED tool must be destructive-hinted: " << line;
}

// Bad path: REJECT (cluster_rebalance) and EXCLUDE (pbc_edit) tools NEVER
// appear, in either the default mode or with --mcp_allow_writes.
TEST(ToolActionMcpTest, RejectAndExcludeToolsNeverAppear) {
  for (bool allow_writes : {false, true}) {
    const vector<string> names = ToolNames(RunToolsList(allow_writes));
    ASSERT_FALSE(Contains(names, "cluster_rebalance"))
        << "REJECT tool leaked (allow_writes=" << allow_writes << ")";
    ASSERT_FALSE(Contains(names, "pbc_edit"))
        << "EXCLUDE tool leaked (allow_writes=" << allow_writes << ")";
    // The MCP server's own blocking 'serve' action (REJECT) must not appear.
    ASSERT_FALSE(Contains(names, "mcp_serve"))
        << "mcp_serve leaked (allow_writes=" << allow_writes << ")";
  }
}

// Bad path: the server-level control flags (mcp_*: gates, timeouts, output
// limit) must never leak into any tool's inputSchema.properties, in any mode.
// Setting one per-call would let the model open a gate itself.
TEST(ToolActionMcpTest, ControlFlagsNeverLeakIntoAnyInputSchema) {
  for (bool allow_writes : {false, true}) {
    const string line = RunToolsList(allow_writes, /*allow_data_reads=*/true);
    JsonReader reader(line);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* result = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
    vector<const rapidjson::Value*> tools;
    ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
    for (const auto* t : tools) {
      string name;
      ASSERT_OK(reader.ExtractString(t, "name", &name));
      const rapidjson::Value* schema = nullptr;
      ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
      const rapidjson::Value* props = nullptr;
      ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
      for (const auto& m : props->GetObject()) {
        ASSERT_NE(0, string(m.name.GetString()).rfind("mcp_", 0))
            << "tool '" << name << "' leaks control flag '"
            << m.name.GetString() << "' (allow_writes=" << allow_writes << ")";
      }
    }
  }
}

// Bad path: an unknown method yields a JSON-RPC -32601 (method not found).
TEST(ToolActionMcpTest, UnknownMethodReturnsMethodNotFound) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"no/such/method\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(kJsonRpcMethodNotFound, ErrorCode(lines[0]));
  // The id is still echoed on the error response.
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
  ASSERT_EQ(3, id);
}

// Bad path: a malformed JSON line yields a -32700 parse error, and the loop
// continues to serve the subsequent valid line.
TEST(ToolActionMcpTest, MalformedJsonReturnsParseErrorAndLoopContinues) {
  vector<string> lines = RunAndCollect(
      "not json at all\n"
      "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(2, lines.size());

  // First line: parse error with a null id.
  JsonReader err(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &err));
  ASSERT_EQ(kJsonRpcParseError, ErrorCode(lines[0]));

  // Second line: the following valid request is still served.
  JsonReader ok(lines[1]);
  NO_FATALS(AssertValidEnvelope(lines[1], &ok));
  ASSERT_TRUE(ok.root()->HasMember("result"));
}

// Bad path: a 'notifications/initialized' line produces NO output line.
TEST(ToolActionMcpTest, InitializedNotificationProducesNoOutput) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n");
  ASSERT_TRUE(lines.empty());
}

// A JSON-RPC notification (a request without an id) produces no response even
// for a non-"notifications/*" method.
TEST(ToolActionMcpTest, RequestWithoutIdProducesNoOutput) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"method\":\"tools/list\"}\n");
  ASSERT_TRUE(lines.empty());
}

// Bad path: a request that carries an id but no 'method' -> -32600 (invalid
// request).
TEST(ToolActionMcpTest, RequestWithoutMethodReturnsInvalidRequest) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":5}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(kJsonRpcInvalidRequest, ErrorCode(lines[0]));
}

// Blank lines are ignored while surrounding requests are answered in order.
TEST(ToolActionMcpTest, BlankLinesIgnoredAndRequestsAnsweredInOrder) {
  vector<string> lines = RunAndCollect(
      "\n"
      "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n"
      "\n"
      "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}\n");
  ASSERT_EQ(2, lines.size());

  JsonReader r0(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &r0));
  int32_t id0 = 0;
  ASSERT_OK(r0.ExtractInt32(r0.root(), "id", &id0));
  ASSERT_EQ(1, id0);

  JsonReader r1(lines[1]);
  NO_FATALS(AssertValidEnvelope(lines[1], &r1));
  int32_t id1 = 0;
  ASSERT_OK(r1.ExtractInt32(r1.root(), "id", &id1));
  ASSERT_EQ(2, id1);
}

// Bad path: valid JSON that is not an object (a bare array/number) -> -32600.
TEST(ToolActionMcpTest, NonObjectJsonReturnsInvalidRequest) {
  vector<string> lines = RunAndCollect("[1,2,3]\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(kJsonRpcInvalidRequest, ErrorCode(lines[0]));
}

// Bad path: an 'id' whose type is not string/number/null (here an object) is an
// Invalid Request -> -32600. It must be rejected rather than processed, since a
// success response would have to null the id and break request/response
// correlation. Per JSON-RPC, the error itself carries a null id.
TEST(ToolActionMcpTest, NonScalarIdReturnsInvalidRequestWithNullId) {
  vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":{\"k\":1},\"method\":\"tools/list\"}\n");
  ASSERT_EQ(1, lines.size());

  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  ASSERT_EQ(kJsonRpcInvalidRequest, ErrorCode(lines[0]));
  const rapidjson::Value* root = reader.root();
  ASSERT_TRUE(root->HasMember("id"));
  ASSERT_TRUE((*root)["id"].IsNull()) << lines[0];
}

// Empty stdin: the loop returns OK and writes nothing.
TEST(ToolActionMcpTest, EmptyInputProducesNoResponseAndExitsCleanly) {
  vector<string> lines = RunAndCollect("");
  ASSERT_TRUE(lines.empty());
}

// ---------------------------------------------------------------------------
// tools/call
// ---------------------------------------------------------------------------

// Bad path: calling an unknown tool name yields a JSON-RPC -32602 (invalid
// params), not a tool result.
TEST(ToolActionMcpTest, ToolsCallUnknownToolReturnsInvalidParams) {
  const string line = RunToolsCall("no_such_tool", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("error")) << line;
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line));
}

// Bad path: a call missing a non-injected required argument yields -32602
// naming the missing argument. 'tserver_status' requires 'tserver_address',
// which is model-supplied (not injected).
TEST(ToolActionMcpTest, ToolsCallMissingRequiredArgReturnsInvalidParams) {
  const string line = RunToolsCall("tserver_status", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line));

  const rapidjson::Value* err = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  ASSERT_OK(reader.ExtractString(err, "message", &message));
  ASSERT_NE(string::npos, message.find("tserver_address"))
      << "error must name the missing argument: " << message;
}

// Schema scrub: the plural 'master_addresses' must NEVER appear in any tool's
// inputSchema (as a property or in required[]), in either write mode. The model
// must not need to supply (nor be able to redirect) the cluster address; the
// singular 'master_address' is a separate, required property restricted to the
// configured masters (see MasterAddressIsRequiredAndRestrictedToConfiguredMasters).
TEST(ToolActionMcpTest, MasterAddressesNeverInAnyInputSchema) {
  for (bool allow_writes : {false, true}) {
    const string line = RunToolsList(allow_writes);
    JsonReader reader(line);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* result = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
    vector<const rapidjson::Value*> tools;
    ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));
    for (const auto* t : tools) {
      string name;
      ASSERT_OK(reader.ExtractString(t, "name", &name));
      const rapidjson::Value* schema = nullptr;
      ASSERT_OK(reader.ExtractObject(t, "inputSchema", &schema));
      const rapidjson::Value* props = nullptr;
      ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
      ASSERT_FALSE(props->HasMember("master_addresses"))
          << "tool '" << name << "' leaks master_addresses";
      vector<const rapidjson::Value*> required;
      ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
      for (const auto* r : required) {
        ASSERT_TRUE(r->IsString());
        const string rname = r->GetString();
        ASSERT_NE("master_addresses", rname) << name;
      }
    }
  }
}

// Injection error path: omitting master_addresses when FLAGS_master_addresses
// is unset yields a tool result (isError=true) telling the operator to launch
// with --master_addresses -- NOT a JSON-RPC protocol error.
TEST(ToolActionMcpTest, ToolsCallWithoutConfiguredMasterAddressesReturnsToolError) {
  ScopedMasterAddresses guard("");
  const string line = RunToolsCall("master_status", "{}");
  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("result")) << line;

  const ToolResult tr = ParseToolResult(line);
  ASSERT_TRUE(tr.is_error) << line;
  ASSERT_NE(string::npos, tr.text.find("--master_addresses"))
      << "message must tell the operator to launch with --master_addresses: "
      << tr.text;
}

// NOTE: The tests that actually EXECUTE an action -- exercising the child-process
// spawn path (Status->isError mapping, per-call flags not leaking into a later
// call, and action output staying inside the tool result content) -- live in
// tool_action_mcp-itest.cc. They now require this server's own 'kudu' binary at
// runtime (each tools/call forks a fresh 'kudu <action>'), which the unit-test
// target does not guarantee, and the itest additionally has a live cluster.

// ---------------------------------------------------------------------------
// Write gating (dry-run + server-side gated rejection)
// ---------------------------------------------------------------------------

// Schema: a GATED tool's inputSchema carries the synthetic 'dry_run'
// boolean property, and it is NOT listed in required[]. A SURFACE tool's schema
// carries no 'dry_run' property at all (dry-run is meaningful only for writes).
TEST(ToolActionMcpTest, GatedToolSchemaCarriesDryRunPropertyNotRequired) {
  const string line = RunToolsList(/*allow_writes=*/true);
  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  vector<const rapidjson::Value*> tools;
  ASSERT_OK(reader.ExtractObjectArray(result, "tools", &tools));

  const rapidjson::Value* add_range = nullptr;
  const rapidjson::Value* describe = nullptr;
  for (const auto* t : tools) {
    string n;
    ASSERT_OK(reader.ExtractString(t, "name", &n));
    if (n == "table_add_range_partition") {
      add_range = t;
    } else if (n == "table_describe") {
      describe = t;
    }
  }
  ASSERT_NE(nullptr, add_range) << "GATED table_add_range_partition missing: " << line;
  ASSERT_NE(nullptr, describe) << "SURFACE table_describe missing: " << line;

  // GATED tool: dry_run is a boolean property.
  const rapidjson::Value* schema = nullptr;
  ASSERT_OK(reader.ExtractObject(add_range, "inputSchema", &schema));
  const rapidjson::Value* props = nullptr;
  ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
  ASSERT_TRUE(props->HasMember("dry_run")) << line;
  const rapidjson::Value* dry_run = nullptr;
  ASSERT_OK(reader.ExtractObject(props, "dry_run", &dry_run));
  string dr_type;
  ASSERT_OK(reader.ExtractString(dry_run, "type", &dr_type));
  ASSERT_EQ("boolean", dr_type);

  // ...but never a required argument.
  vector<const rapidjson::Value*> required;
  ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
  for (const auto* r : required) {
    ASSERT_TRUE(r->IsString());
    ASSERT_NE("dry_run", string(r->GetString())) << "dry_run must not be required: " << line;
  }

  // SURFACE tool: no dry_run property at all.
  const rapidjson::Value* sschema = nullptr;
  ASSERT_OK(reader.ExtractObject(describe, "inputSchema", &sschema));
  const rapidjson::Value* sprops = nullptr;
  ASSERT_OK(reader.ExtractObject(sschema, "properties", &sprops));
  ASSERT_FALSE(sprops->HasMember("dry_run"))
      << "SURFACE tool must not carry dry_run: " << line;
}

// Dry-run happy path: a GATED tools/call with dry_run:true and valid args
// returns isError:false and content text that is EXACTLY the literal command
// that would run -- including the injected master addresses at their positional
// slot, positionals in declared order, and shell-quoting of bracketed bounds.
// No cluster is contacted (Run() is never called).
TEST(ToolActionMcpTest, DryRunGatedCallReturnsExactLiteralCommand) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\",\"dry_run\":true}");

  JsonReader reader(line);
  NO_FATALS(AssertValidEnvelope(line, &reader));
  ASSERT_TRUE(reader.root()->HasMember("result")) << line;
  const ToolResult tr = ParseToolResult(line);
  ASSERT_FALSE(tr.is_error) << tr.text;
  ASSERT_EQ(
      "kudu table add_range_partition master-1:7051 my_range_tbl '[0]' '[100]'",
      tr.text);
}

// Dry-run bad path: args are validated even in dry-run, so a missing
// required argument still yields -32602 naming it (never a silent success).
TEST(ToolActionMcpTest, DryRunWithMissingRequiredArgStillReturnsInvalidParams) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  // Missing table_range_upper_bound.
  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"dry_run\":true}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;

  JsonReader reader(line);
  ASSERT_OK(reader.Init());
  const rapidjson::Value* err = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
  string message;
  ASSERT_OK(reader.ExtractString(err, "message", &message));
  ASSERT_NE(string::npos, message.find("table_range_upper_bound"))
      << "error must name the missing argument: " << message;
}

// Gating bad path: without --mcp_allow_writes a GATED tool is absent from
// tools/list AND calling it is rejected server-side with a DISTINCT message
// that names --mcp_allow_writes -- clearly different from the "unknown tool"
// message a genuinely unknown name still gets.
TEST(ToolActionMcpTest, GatedToolWithoutAllowWritesRejectedWithDistinctMessage) {
  ASSERT_FALSE(FLAGS_mcp_allow_writes);

  // Absent from the listing.
  ASSERT_FALSE(Contains(ToolNames(RunToolsList(/*allow_writes=*/false)),
                        "table_add_range_partition"));

  // Calling it anyway: rejected, and the message names the write gate.
  const string gated = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\"}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(gated)) << gated;
  {
    JsonReader reader(gated);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* err = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
    string message;
    ASSERT_OK(reader.ExtractString(err, "message", &message));
    ASSERT_NE(string::npos, message.find("--mcp_allow_writes"))
        << "gated rejection must name --mcp_allow_writes: " << message;
  }

  // A genuinely unknown tool still gets the "unknown tool" message, not the
  // gated one.
  const string unknown = RunToolsCall("no_such_tool", "{}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(unknown)) << unknown;
  {
    JsonReader reader(unknown);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* err = nullptr;
    ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
    string message;
    ASSERT_OK(reader.ExtractString(err, "message", &message));
    ASSERT_NE(string::npos, message.find("unknown tool"))
        << "unknown-tool rejection must say 'unknown tool': " << message;
    ASSERT_EQ(string::npos, message.find("--mcp_allow_writes"))
        << "unknown-tool rejection must NOT mention --mcp_allow_writes: " << message;
  }
}

// A dry-run call renders any supplied optional flag in the
// reconstructed command but does NOT apply it to process-global gflag state --
// dry-run must leak no flag state (nor call Run()).
TEST(ToolActionMcpTest, DryRunLeavesOptionalFlagStateUnchanged) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");

  string prior_lower_bound_type;
  ASSERT_TRUE(
      google::GetCommandLineOption("lower_bound_type", &prior_lower_bound_type));

  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"my_range_tbl\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\","
      "\"lower_bound_type\":\"EXCLUSIVE_BOUND\",\"dry_run\":true}");

  const ToolResult tr = ParseToolResult(line);
  ASSERT_FALSE(tr.is_error) << tr.text;
  // The optional flag is echoed into the reconstructed command...
  ASSERT_NE(string::npos, tr.text.find("--lower_bound_type=EXCLUSIVE_BOUND"))
      << "dry-run command missing the supplied optional flag: " << tr.text;

  // ...but the process-global gflag value is untouched.
  string after_lower_bound_type;
  ASSERT_TRUE(
      google::GetCommandLineOption("lower_bound_type", &after_lower_bound_type));
  ASSERT_EQ(prior_lower_bound_type, after_lower_bound_type)
      << "dry-run must not mutate gflag state";
}

// ---------------------------------------------------------------------------
// No REJECT / EXCLUDE action is ever exposed (exhaustive tree walk)
// ---------------------------------------------------------------------------

// The RejectAndExcludeToolsNeverAppear test above checks three hand-picked
// names. This is the exhaustive form: walk the REAL action tree, and for EVERY
// action classified REJECT or EXCLUDE assert its MCP tool name is absent from
// tools/list in BOTH write modes. A newly added blocking or interactive action
// that is mistakenly surfaced fails here without needing a bespoke per-name
// assertion.
TEST(ToolActionMcpTest, NoRejectOrExcludeActionIsEverExposed) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  // The set of names the server actually surfaces, in each write mode.
  const unordered_set<string> surfaced_closed = [&] {
    const vector<string> n = ToolNames(RunToolsList(/*allow_writes=*/false));
    return unordered_set<string>(n.begin(), n.end());
  }();
  const unordered_set<string> surfaced_open = [&] {
    const vector<string> n = ToolNames(
        RunToolsList(/*allow_writes=*/true, /*allow_data_reads=*/true));
    return unordered_set<string>(n.begin(), n.end());
  }();

  int reject_or_exclude_seen = 0;
  for (const auto& v : CollectActions(root.get())) {
    const DispositionInfo& info = v.info;
    ASSERT_TRUE(info.classified)
        << "unclassified action leaked into the tree: " << v.tool_name;
    if (info.disposition != Disposition::REJECT &&
        info.disposition != Disposition::EXCLUDE) {
      continue;
    }
    ++reject_or_exclude_seen;
    const string& tool = v.tool_name;
    ASSERT_EQ(0, surfaced_closed.count(tool))
        << DispositionToString(info.disposition) << " action '" << tool
        << "' is surfaced without --mcp_allow_writes";
    ASSERT_EQ(0, surfaced_open.count(tool))
        << DispositionToString(info.disposition) << " action '" << tool
        << "' is surfaced with --mcp_allow_writes";
  }

  // Sanity: the tree really did contain REJECT/EXCLUDE actions, so the walk
  // above was not vacuously green.
  ASSERT_GT(reject_or_exclude_seen, 0)
      << "expected the action tree to contain REJECT/EXCLUDE actions";
}

// Dispatch side: calling a REJECT (cluster_rebalance) or an EXCLUDE
// (pbc_edit) tool by name is refused server-side -- it is not in the registry
// in either write mode, so tools/call returns -32602 (unknown tool), never a
// tool result that would run it.
TEST(ToolActionMcpTest, ToolsCallRejectOrExcludeToolIsRefused) {
  for (bool allow_writes : {false, true}) {
    ScopedAllowWrites allow(allow_writes);
    for (const char* tool : {"cluster_rebalance", "pbc_edit"}) {
      const string line = RunToolsCall(tool, "{}");
      ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line))
          << tool << " (allow_writes=" << allow_writes << "): " << line;
      JsonReader reader(line);
      ASSERT_OK(reader.Init());
      const rapidjson::Value* err = nullptr;
      ASSERT_OK(reader.ExtractObject(reader.root(), "error", &err));
      string message;
      ASSERT_OK(reader.ExtractString(err, "message", &message));
      // A never-surfaced tool is rejected as unknown, NOT with the write-gate
      // message (which is reserved for GATED tools hidden by the closed gate).
      ASSERT_NE(string::npos, message.find("unknown tool"))
          << tool << ": " << message;
      ASSERT_EQ(string::npos, message.find("--mcp_allow_writes"))
          << tool << " must not be treated as a gated tool: " << message;
    }
  }
}

// ---------------------------------------------------------------------------
// No surfaced tool exposes an unsafe/experimental optional parameter
// ---------------------------------------------------------------------------

// Action::Run() calls kudu::ValidateFlags(), which can exit(1) if an unsafe- or
// experimental-tagged gflag is set without the matching --unlock flag. A
// surfaced tool is safe from that path ONLY if none of its model-settable
// optional parameters map to such a flag. This walks every SURFACE/GATED action
// and asserts none of its optional parameters is tagged 'unsafe' or
// 'experimental'. If this ever fails, a newly surfaced tool could let model
// input trip a process-fatal flag check -- fix the disposition (or the tool),
// do not weaken this assertion.
TEST(ToolActionMcpTest, NoSurfacedToolExposesUnsafeOrExperimentalFlag) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  int checked = 0;
  for (const auto& v : CollectActions(root.get())) {
    const DispositionInfo& info = v.info;
    ASSERT_TRUE(info.classified);
    if (info.disposition != Disposition::SURFACE &&
        info.disposition != Disposition::GATED) {
      continue;
    }
    ++checked;
    const string& tool = v.tool_name;
    for (const auto& flag : v.action->args().optional) {
      unordered_set<string> tags;
      GetFlagTags(flag.name, &tags);
      ASSERT_EQ(0, tags.count("unsafe"))
          << "surfaced tool '" << tool << "' exposes unsafe flag '"
          << flag.name << "'";
      ASSERT_EQ(0, tags.count("experimental"))
          << "surfaced tool '" << tool << "' exposes experimental flag '"
          << flag.name << "'";
    }
  }

  ASSERT_GT(checked, 0) << "expected surfaced actions to audit";
}

// ---------------------------------------------------------------------------
// Argument validation
// ---------------------------------------------------------------------------

// An argument key the tool's schema does not advertise is rejected, so a typo
// fails loudly rather than the tool silently running without it.
TEST(ToolActionMcpTest, UnknownArgumentIsRejected) {
  ScopedMasterAddresses addrs("master-1:7051");
  const string line = RunToolsCall("table_describe",
                                   "{\"table_name\":\"t\",\"no_such_flag\":1}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
  ASSERT_STR_CONTAINS(ErrorMessageOf(line), "unknown argument 'no_such_flag'");
}

// The same holds for GATED tools: a misspelled 'dry_run' must not run the
// mutation for real.
TEST(ToolActionMcpTest, MisspelledDryRunIsRejected) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");
  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\",\"dryrun\":true}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
  ASSERT_STR_CONTAINS(ErrorMessageOf(line), "unknown argument 'dryrun'");
}

// 'dry_run' is only part of a GATED tool's schema; on any other tool it is an
// unknown argument.
TEST(ToolActionMcpTest, DryRunOnSurfaceToolIsRejected) {
  ScopedMasterAddresses addrs("master-1:7051");
  const string line = RunToolsCall("table_describe",
                                   "{\"table_name\":\"t\",\"dry_run\":true}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
  ASSERT_STR_CONTAINS(ErrorMessageOf(line), "unknown argument 'dry_run'");
}

// A present-but-non-boolean 'dry_run' is rejected instead of being ignored
// (ignoring it would execute the mutation the caller meant to preview).
TEST(ToolActionMcpTest, NonBooleanDryRunIsRejected) {
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");
  for (const char* value : { "\"true\"", "1", "null" }) {
    SCOPED_TRACE(value);
    const string line = RunToolsCall(
        "table_add_range_partition",
        Substitute("{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
                   "\"table_range_upper_bound\":\"[100]\",\"dry_run\":$0}",
                   value));
    ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
    ASSERT_STR_CONTAINS(ErrorMessageOf(line), "'dry_run' must be a boolean");
  }
}

// The cluster a tool talks to is fixed at launch: a per-call
// 'master_addresses' is rejected rather than overriding the configured value.
TEST(ToolActionMcpTest, PerCallMasterAddressesOverrideIsRejected) {
  for (const char* configured : { "master-1:7051", "" }) {
    SCOPED_TRACE(configured);
    ScopedMasterAddresses addrs(configured);
    const string line = RunToolsCall(
        "table_list", "{\"master_addresses\":\"other-cluster:7051\"}");
    ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
    ASSERT_STR_CONTAINS(ErrorMessageOf(line), "cannot be overridden");
  }
}

// The singular 'master_address' (single-master actions) is a required property
// listing the configured masters, with no default: the targeted master matters
// (e.g. 'master_remove' removes it), so it is never picked implicitly. Omitting
// it or naming a master outside the list is rejected; a listed one is accepted.
TEST(ToolActionMcpTest, MasterAddressIsRequiredAndRestrictedToConfiguredMasters) {
  ScopedMasterAddresses addrs("master-1:7051, master-2:7051");

  {
    const string line = RunToolsList(/*allow_writes=*/false);
    JsonReader reader(line);
    ASSERT_OK(reader.Init());
    const rapidjson::Value* schema = FindToolSchema(reader, "master_status");
    ASSERT_NE(nullptr, schema) << line;
    const rapidjson::Value* props = nullptr;
    ASSERT_OK(reader.ExtractObject(schema, "properties", &props));
    const rapidjson::Value* ma = nullptr;
    ASSERT_OK(reader.ExtractObject(props, "master_address", &ma));
    ASSERT_TRUE(ma->HasMember("enum") && (*ma)["enum"].IsArray()) << line;
    vector<string> allowed;
    for (const auto& e : (*ma)["enum"].GetArray()) {
      ASSERT_TRUE(e.IsString()) << line;
      allowed.emplace_back(e.GetString());
    }
    ASSERT_EQ(vector<string>({ "master-1:7051", "master-2:7051" }), allowed);
    ASSERT_FALSE(ma->HasMember("default")) << line;
    vector<const rapidjson::Value*> required;
    ASSERT_OK(reader.ExtractObjectArray(schema, "required", &required));
    ASSERT_TRUE(std::any_of(required.begin(), required.end(),
                            [](const rapidjson::Value* r) {
                              return r->IsString() &&
                                  string(r->GetString()) == "master_address";
                            })) << line;
  }

  // Omitted: rejected, with the choices listed. Applies to read-only and
  // gated tools alike; there is no implicit "first master".
  for (const auto& tool : { "master_status", "master_remove" }) {
    SCOPED_TRACE(tool);
    ScopedAllowWrites allow(true);
    const string line = RunToolsCall(tool, "{}");
    ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
    const string message = ErrorMessageOf(line);
    ASSERT_STR_CONTAINS(message, "missing required argument 'master_address'");
    ASSERT_STR_CONTAINS(message, "master-1:7051,master-2:7051");
  }

  // Not a configured master: rejected.
  {
    const string line = RunToolsCall("master_status",
                                     "{\"master_address\":\"evil:7051\"}");
    ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
    ASSERT_STR_CONTAINS(ErrorMessageOf(line), "configured masters");
  }
  // A configured (non-first) master passes validation. With no 'kudu' binary
  // in this test the call then stops at the execution step, which proves the
  // arguments were accepted.
  {
    const string line = RunToolsCall("master_status",
                                     "{\"master_address\":\"master-2:7051\"}");
    const ToolResult tr = ParseToolResult(line);
    ASSERT_TRUE(tr.is_error) << line;
    ASSERT_STR_CONTAINS(tr.text, "could not locate its own 'kudu' binary");
  }
}

// The scalar-type error wording is the same for every kind of argument.
TEST(ToolActionMcpTest, NonScalarArgumentErrorsAreConsistent) {
  ScopedMasterAddresses addrs("master-1:7051");
  for (const auto& c : vector<pair<string, string>>{
         { "table_describe", "{\"table_name\":{}}" },
         { "table_describe", "{\"table_name\":\"t\",\"show_attributes\":[]}" },
         { "master_status", "{\"master_address\":[]}" } }) {
    SCOPED_TRACE(c.second);
    const string line = RunToolsCall(c.first, c.second);
    ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
    ASSERT_STR_MATCHES(ErrorMessageOf(line),
                       "^Invalid params: argument '[a-z_]+' must be a scalar$");
  }
}

// Connection/security flags set on 'serve' (here --sasl_protocol_name) are
// forwarded to the tool child; the dry-run command shows exactly that.
TEST(ToolActionMcpTest, ServeConnectionFlagsAreForwardedToTools) {
  gflags::FlagSaver saver;
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");
  ASSERT_NE("", google::SetCommandLineOption("sasl_protocol_name", "custom"));

  const string line = RunToolsCall(
      "table_add_range_partition",
      "{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\",\"dry_run\":true}");
  const ToolResult tr = ParseToolResult(line);
  ASSERT_FALSE(tr.is_error) << tr.text;
  ASSERT_STR_CONTAINS(tr.text, "--sasl_protocol_name=custom");
}

// A forwarded flag explicitly set to its default value is still forwarded:
// "explicitly set" follows gflags' is_default (was the flag assigned?), not a
// comparison of the current value against the default. A flag never set is
// not forwarded.
TEST(ToolActionMcpTest, ServeConnectionFlagExplicitlySetToDefaultIsForwarded) {
  gflags::FlagSaver saver;
  ScopedAllowWrites allow(true);
  ScopedMasterAddresses addrs("master-1:7051");
  const string kCall =
      "{\"table_name\":\"t\",\"table_range_lower_bound\":\"[0]\","
      "\"table_range_upper_bound\":\"[100]\",\"dry_run\":true}";

  google::CommandLineFlagInfo info;
  ASSERT_TRUE(google::GetCommandLineFlagInfo("sasl_protocol_name", &info));
  ASSERT_TRUE(info.is_default);
  {
    const ToolResult tr =
        ParseToolResult(RunToolsCall("table_add_range_partition", kCall));
    ASSERT_FALSE(tr.is_error) << tr.text;
    ASSERT_STR_NOT_CONTAINS(tr.text, "--sasl_protocol_name");
  }

  ASSERT_NE("", google::SetCommandLineOption("sasl_protocol_name",
                                             info.default_value.c_str()));
  {
    const ToolResult tr =
        ParseToolResult(RunToolsCall("table_add_range_partition", kCall));
    ASSERT_FALSE(tr.is_error) << tr.text;
    ASSERT_STR_CONTAINS(tr.text,
                        "--sasl_protocol_name=" + info.default_value);
  }
}

// ---------------------------------------------------------------------------
// Data-read gate
// ---------------------------------------------------------------------------

// Tools returning row-level user data are hidden unless --mcp_allow_data_reads
// is set, and calling one while hidden names that flag.
TEST(ToolActionMcpTest, UserDataToolsRequireAllowDataReads) {
  ASSERT_FALSE(FLAGS_mcp_allow_data_reads);
  ASSERT_FALSE(Contains(ToolNames(RunToolsList(/*allow_writes=*/true)),
                        "table_scan"));
  const vector<string> names = ToolNames(
      RunToolsList(/*allow_writes=*/false, /*allow_data_reads=*/true));
  ASSERT_TRUE(Contains(names, "table_scan"));
  ASSERT_TRUE(Contains(names, "wal_dump"));
  // Metadata reads do not depend on the data gate.
  ASSERT_TRUE(Contains(ToolNames(RunToolsList(/*allow_writes=*/false)),
                       "table_describe"));

  ScopedMasterAddresses addrs("master-1:7051");
  const string line = RunToolsCall("table_scan", "{\"table_name\":\"t\"}");
  ASSERT_EQ(kJsonRpcInvalidParams, ErrorCode(line)) << line;
  const string message = ErrorMessageOf(line);
  ASSERT_STR_CONTAINS(message, "--mcp_allow_data_reads");
  ASSERT_STR_NOT_CONTAINS(message, "--mcp_allow_writes");
}

// ---------------------------------------------------------------------------
// Protocol methods and serve options
// ---------------------------------------------------------------------------

// 'ping' is answered with an empty result, as the MCP spec requires.
TEST(ToolActionMcpTest, PingReturnsEmptyResult) {
  const vector<string> lines = RunAndCollect(
      "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"ping\"}\n");
  ASSERT_EQ(1, lines.size());
  JsonReader reader(lines[0]);
  NO_FATALS(AssertValidEnvelope(lines[0], &reader));
  const rapidjson::Value* result = nullptr;
  ASSERT_OK(reader.ExtractObject(reader.root(), "result", &result));
  ASSERT_TRUE(result->ObjectEmpty()) << lines[0];
  int32_t id = 0;
  ASSERT_OK(reader.ExtractInt32(reader.root(), "id", &id));
  ASSERT_EQ(7, id);
}

// Every mcp_* server flag is registered on 'kudu mcp serve', so it shows up in
// its --help.
TEST(ToolActionMcpTest, ServeRegistersAllServerFlags) {
  unique_ptr<Mode> mode = BuildMcpMode();
  ASSERT_EQ(1, mode->actions().size());
  unordered_set<string> optional;
  for (const auto& o : mode->actions()[0]->args().optional) {
    optional.insert(o.name);
  }
  vector<google::CommandLineFlagInfo> all;
  google::GetAllFlags(&all);
  int mcp_flags = 0;
  for (const auto& f : all) {
    if (f.name.rfind("mcp_", 0) == 0) {
      ++mcp_flags;
      ASSERT_EQ(1, optional.count(f.name)) << f.name << " missing from serve";
    }
  }
  ASSERT_GT(mcp_flags, 0);
}

// ---------------------------------------------------------------------------
// Child-outcome mapping (deterministic, no child process needed).
// ---------------------------------------------------------------------------

// InterpretChildOutcome is the sole place a finished child tool process becomes
// an MCP tool result, so it is the crash boundary's decision function. Cover its
// four cases directly -- a clean exit, a non-zero exit, death by signal (the
// CHECK / LOG(FATAL) abort class that used to take the whole server down in
// process), and a timeout -- with hand-built wait statuses, so the signal and
// timeout paths are exercised without needing a binary that actually crashes.
TEST(ToolActionMcpTest, InterpretChildOutcomeMapsExitSignalAndTimeout) {
  // Traditional Unix wait-status layout (Linux and macOS): the low 7 bits hold
  // the terminating signal (0 for a normal exit), bit 0x80 marks a core dump,
  // and bits 8..15 hold the exit code. Each case asserts the W* macro
  // precondition first, so a platform whose layout differed would fail loudly
  // here rather than silently mis-test the mapping.
  auto make_exited = [](int code) { return (code & 0xff) << 8; };
  auto make_signaled = [](int sig, bool core) {
    return (sig & 0x7f) | (core ? 0x80 : 0);
  };

  // Clean exit (code 0): not an error; the child's stdout IS the result text,
  // and stderr is ignored.
  {
    const int status = make_exited(0);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*mutating=*/false, /*out=*/"the tool output",
        /*err=*/"some progress on stderr");
    ASSERT_FALSE(o.is_error);
    ASSERT_EQ("the tool output", o.text);
  }

  // Non-zero exit: an error whose text names the exit code and carries the
  // child's stderr (the action's own diagnostics).
  {
    const int status = make_exited(1);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(1, WEXITSTATUS(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*mutating=*/false, /*out=*/"", /*err=*/"table not found");
    ASSERT_TRUE(o.is_error);
    ASSERT_NE(string::npos, o.text.find("code 1")) << o.text;
    ASSERT_NE(string::npos, o.text.find("table not found")) << o.text;
  }

  // Non-zero exit with empty stderr: the error text falls back to stdout.
  {
    const int status = make_exited(2);
    ASSERT_TRUE(WIFEXITED(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*mutating=*/false, /*out=*/"printed to stdout before failing",
        /*err=*/"");
    ASSERT_TRUE(o.is_error);
    ASSERT_NE(string::npos, o.text.find("printed to stdout before failing"))
        << o.text;
  }

  // Death by signal (SIGABRT: the CHECK / LOG(FATAL) / abort class): an error
  // that names the signal. This is exactly the outcome that, in process, would
  // have terminated the long-lived server.
  {
    const int status = make_signaled(SIGABRT, /*core=*/false);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGABRT, WTERMSIG(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*mutating=*/false, /*out=*/"", /*err=*/"Check failed: ...");
    ASSERT_TRUE(o.is_error);
    ASSERT_NE(string::npos, o.text.find(Substitute("signal $0", SIGABRT)))
        << o.text;
  }

  // Death by signal with a core dump: the text notes the core dump.
  {
    const int status = make_signaled(SIGSEGV, /*core=*/true);
    ASSERT_TRUE(WIFSIGNALED(status));
    ASSERT_EQ(SIGSEGV, WTERMSIG(status));
    ASSERT_TRUE(WCOREDUMP(status));
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/false, /*timeout_seconds=*/60,
        /*mutating=*/false, /*out=*/"", /*err=*/"");
    ASSERT_TRUE(o.is_error);
    ASSERT_NE(string::npos, o.text.find("core dumped")) << o.text;
  }

  // Timeout: an error naming the deadline; 'timed_out' wins over whatever the
  // (post-SIGKILL) wait status happens to be.
  {
    const int status = make_signaled(SIGKILL, /*core=*/false);
    const ToolOutcome o = InterpretChildOutcome(
        status, /*timed_out=*/true, /*timeout_seconds=*/42,
        /*mutating=*/false, /*out=*/"partial", /*err=*/"");
    ASSERT_TRUE(o.is_error);
    ASSERT_NE(string::npos, o.text.find("42 seconds")) << o.text;
    ASSERT_EQ(string::npos, o.text.find("partially")) << o.text;
  }

  // A mutating tool killed on timeout or by a signal: the text warns that the
  // change may be partially applied. A clean failure (non-zero exit) does not.
  {
    const ToolOutcome timed_out = InterpretChildOutcome(
        make_signaled(SIGKILL, false), /*timed_out=*/true,
        /*timeout_seconds=*/300, /*mutating=*/true, /*out=*/"", /*err=*/"");
    ASSERT_TRUE(timed_out.is_error);
    ASSERT_STR_CONTAINS(timed_out.text, "partially or fully applied");
    ASSERT_STR_CONTAINS(timed_out.text, "Check the cluster state");

    const ToolOutcome signaled = InterpretChildOutcome(
        make_signaled(SIGABRT, false), /*timed_out=*/false,
        /*timeout_seconds=*/300, /*mutating=*/true, /*out=*/"", /*err=*/"");
    ASSERT_STR_CONTAINS(signaled.text, "partially or fully applied");

    const ToolOutcome exited = InterpretChildOutcome(
        make_exited(1), /*timed_out=*/false, /*timeout_seconds=*/300,
        /*mutating=*/true, /*out=*/"", /*err=*/"already exists");
    ASSERT_EQ(string::npos, exited.text.find("partially")) << exited.text;
  }
}

} // namespace tools
} // namespace kudu
