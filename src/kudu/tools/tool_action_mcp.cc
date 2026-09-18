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

#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <rapidjson/document.h>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/strings/util.h"
#include "kudu/tools/mcp_disposition.h"
#include "kudu/tools/tool_action.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/tools/tool_action_mcp-internal.h"
#include "kudu/util/jsonreader.h"
#include "kudu/util/jsonwriter.h"
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "kudu/util/subprocess.h"
#include "kudu/util/version_info.h"

// The mcp_* flags below are registered on the 'serve' action. The two gates are
// read once at startup to build the exposure registry (see RunMcpServeLoop).
DEFINE_bool(mcp_allow_writes, false,
            "Whether to expose mutating (write) tools over the MCP protocol. "
            "When false, only read-only tools are surfaced.");

DEFINE_bool(mcp_allow_data_reads, false,
            "Whether to expose tools that return row-level user data (e.g. "
            "'table scan', WAL and rowset dumps) over the MCP protocol. Such "
            "output is not redacted, so it reaches the model verbatim. When "
            "false, only tools returning metadata are surfaced.");

// Each tools/call runs its action in a fresh child 'kudu' process. These bound
// how long that child may run before the server SIGKILLs it and returns a tool
// error, so a hung action can never wedge the single-threaded serve loop.
// Mutating tools get a separate, longer limit: killing one midway may leave its
// change partially applied, so it should only happen to a truly stuck call.
DEFINE_int32(mcp_tool_timeout_sec, 60,
             "Maximum wall-clock seconds a single read-only MCP tool invocation "
             "(a child 'kudu' process) may run before it is killed and the "
             "call returns a tool error.");
DEFINE_validator(mcp_tool_timeout_sec,
                 [](const char* /*n*/, int32_t v) { return v > 0; });

DEFINE_int32(mcp_gated_tool_timeout_sec, 300,
             "Maximum wall-clock seconds a single mutating (gated) MCP tool "
             "invocation may run before it is killed. A killed mutating tool "
             "may have partially applied its change.");
DEFINE_validator(mcp_gated_tool_timeout_sec,
                 [](const char* /*n*/, int32_t v) { return v > 0; });

// Bounds the memory a single tools/call can consume and the size of the result
// handed to the model: a 'table scan' or 'ksck --checksum_scan' can otherwise
// produce gigabytes within the timeout.
DEFINE_int64(mcp_max_output_bytes, 1024 * 1024,
             "Maximum bytes of a tool's stdout (and, separately, stderr) "
             "returned in an MCP tool result. Output beyond the limit is "
             "discarded and the result is marked as truncated. 0 means no "
             "limit.");
DEFINE_validator(mcp_max_output_bytes,
                 [](const char* /*n*/, int64_t v) { return v >= 0; });

// The master addresses the serve session connects to. The operator registers
// the server once with 'kudu mcp serve --master_addresses=...', and tools/call
// injects this value for any action that needs it. A call can never point a
// tool at another cluster: per-call 'master_addresses' is rejected, and a
// single-master action's (required) 'master_address' must be one of these. Defined in the
// master library
// (master_options.cc) and linked into the 'kudu' binary; declared here so
// 'serve' can register it as an optional parameter and tools/call can read it.
DECLARE_string(master_addresses);

using std::function;
using std::istream;
using std::ostream;
using std::ostringstream;
using std::pair;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

// Interprets a finished child tool process into an MCP tool result; see the
// declaration in tool_action_mcp-internal.h for the full contract.
ToolOutcome InterpretChildOutcome(int wait_status, bool timed_out,
                                  int timeout_seconds, bool mutating,
                                  const string& out, const string& err) {
  // A mutating tool that was killed (or crashed) midway may have applied some
  // or all of its change: say so, so that the caller checks before retrying.
  auto maybe_partial = [&](string text) {
    if (mutating) {
      text += ". This is a mutating tool: its change may have been partially "
              "or fully applied. Check the cluster state before retrying";
    }
    return text;
  };
  auto with_detail = [&](string text) -> ToolOutcome {
    const string& detail = !err.empty() ? err : out;
    if (!detail.empty()) {
      text += "\n";
      text += detail;
    }
    return { std::move(text), true };
  };

  if (timed_out) {
    return with_detail(maybe_partial(Substitute(
        "tool timed out after $0 seconds and was killed", timeout_seconds)));
  }
  if (WIFEXITED(wait_status)) {
    const int code = WEXITSTATUS(wait_status);
    if (code == 0) {
      return { out, false };
    }
    return with_detail(Substitute("tool process exited with code $0", code));
  }
  if (WIFSIGNALED(wait_status)) {
    const int sig = WTERMSIG(wait_status);
    string text = Substitute("tool process was terminated by signal $0", sig);
#if defined(WCOREDUMP)
    if (WCOREDUMP(wait_status)) {
      text += " (core dumped)";
    }
#endif
    return with_detail(maybe_partial(std::move(text)));
  }
  return with_detail(
      Substitute("tool process ended abnormally (raw status $0)", wait_status));
}

namespace {

// ----------------------------------------------------------------------------
// MCP / JSON-RPC constants
// ----------------------------------------------------------------------------

// serverInfo.name reported in the initialize handshake.
const char* const kServerName = "kudu-mcp";

// The MCP protocol version this server implements by default. This is what we
// report when the client does not request a specific (supported) version. Uses
// the dated MCP revision string format. We advertise the newest revision in the
// supported set (see IsSupportedProtocolVersion): our implemented surface is
// identical across all supported revisions.
const char* const kDefaultProtocolVersion = "2025-11-25";

// Returns true if 'version' is an MCP protocol revision this server can speak.
// The client's requested version is echoed back only when it is in this set;
// otherwise the server replies with kDefaultProtocolVersion and the client is
// expected to adapt or disconnect.
//
// All four are initialize-handshake revisions, and this server implements only
// the subset unchanged across them: 'initialize' -> 'tools/list' -> 'tools/call'
// returning text content over stdio, advertising capabilities.tools only. Every
// breaking change between these revisions lands in an area we don't touch (HTTP
// transport, OAuth, JSON-RPC batching, elicitation/sampling, richer content
// types), so no per-version behavior is needed -- we just echo the negotiated
// string. The next revision (2026-07-28) is NOT handshake-based (it removes
// 'initialize', makes the protocol stateless, and mandates 'server/discover'),
// so it cannot simply be added here -- it requires a separate implementation.
//
// TODO(mgreber): support the latest revision (2026-07-28). It needs the
// stateless per-request path (protocol version carried in each request's _meta)
// plus a 'server/discover' RPC, so it is real work rather than another entry in
// this set. Not urgent: a 2026-07-28 client still interoperates with us today
// via the spec's backward-compatibility path -- it probes 'server/discover' on
// stdio, finds a handshake-only server, and falls back to negotiating down to a
// version in this set. So this is a capability gap, not a breakage.
bool IsSupportedProtocolVersion(const string& version) {
  return version == "2024-11-05" ||
         version == "2025-03-26" ||
         version == "2025-06-18" ||
         version == "2025-11-25";
}

// ----------------------------------------------------------------------------
// Response envelope construction (JsonWriter)
// ----------------------------------------------------------------------------

// Writes a JSON-RPC 'id' value. The spec permits String, Number, or Null, and
// requires the response id to equal the request id exactly, so we echo the
// value verbatim -- including a fractional Number (which the spec discourages
// clients from sending but does not forbid), since "MUST match" outranks that
// advice. All four integer widths are handled because rapidjson stores a number
// as the narrowest type that fits; a large-but-legal id lands in Int64/Uint64,
// and handling only Int would null it out. A null or absent id is written as
// JSON null ('id' may be nullptr). Invalid id types (bool/object/array) never
// reach here from a client request -- ClassifyRequest rejects them as Invalid
// Request -- so the final branch is a defensive DCHECK, not a normal path.
void WriteJsonRpcId(JsonWriter* jw, const rapidjson::Value* id) {
  if (id == nullptr || id->IsNull()) {
    jw->Null();
  } else if (id->IsInt()) {
    jw->Int(id->GetInt());
  } else if (id->IsInt64()) {
    jw->Int64(id->GetInt64());
  } else if (id->IsUint()) {
    jw->Uint(id->GetUint());
  } else if (id->IsUint64()) {
    jw->Uint64(id->GetUint64());
  } else if (id->IsDouble()) {
    jw->Double(id->GetDouble());
  } else if (id->IsString()) {
    jw->String(id->GetString());
  } else {
    // Bool/object/array are not valid JSON-RPC ids. Client requests carrying
    // such an id are rejected as Invalid Request at the transport boundary
    // (ClassifyRequest), so reaching here means an id of an invalid type was
    // constructed internally -- a bug. Flag it in debug builds; still write a
    // well-formed null in release rather than emitting a nonsensical id.
    DCHECK(false) << "WriteJsonRpcId: id has invalid JSON-RPC type "
                  << id->GetType();
    jw->Null();
  }
}

// Writes the envelope common to every JSON-RPC response
// ({"jsonrpc":"2.0","id":<id>, ...}) and invokes 'write_body' to emit exactly
// one of the "result" or "error" members. This is the single place the response
// shape -- and the "carries EITHER result OR error, never both" invariant -- is
// expressed; every builder below funnels through here.
string BuildResponse(const rapidjson::Value* id,
                     const function<void(JsonWriter*)>& write_body) {
  ostringstream ss;
  JsonWriter jw(&ss, JsonWriter::COMPACT);
  jw.StartObject();
  jw.String("jsonrpc");
  jw.String("2.0");
  jw.String("id");
  WriteJsonRpcId(&jw, id);
  write_body(&jw);
  jw.EndObject();
  return ss.str();
}

// Builds a JSON-RPC success response whose "result" is an object; the object's
// fields are written by 'write_result_fields'.
string BuildResultResponse(
    const rapidjson::Value* id,
    const function<void(JsonWriter*)>& write_result_fields) {
  return BuildResponse(id, [&](JsonWriter* jw) {
    jw->String("result");
    jw->StartObject();
    write_result_fields(jw);
    jw->EndObject();
  });
}

// Builds a JSON-RPC error response line:
//   {"jsonrpc":"2.0","id":<id>,"error":{"code":<code>,"message":<message>}}
string BuildErrorResponse(const rapidjson::Value* id,
                          int code,
                          const string& message) {
  return BuildResponse(id, [&](JsonWriter* jw) {
    jw->String("error");
    jw->StartObject();
    jw->String("code");
    jw->Int(code);
    jw->String("message");
    jw->String(message);
    jw->EndObject();
  });
}

// Builds the JSON-RPC response to an 'initialize' request. The result reports
// the negotiated protocol version, the server's declared capabilities (tools
// only -- no resources), and serverInfo.
string BuildInitializeResponse(const rapidjson::Value* id,
                               const string& protocol_version) {
  return BuildResultResponse(id, [&protocol_version](JsonWriter* jw) {
    jw->String("protocolVersion");
    jw->String(protocol_version);

    // Capabilities: declare only the 'tools' capability. Its value is an empty
    // object (no sub-capabilities such as listChanged in v1).
    jw->String("capabilities");
    jw->StartObject();
    jw->String("tools");
    jw->StartObject();
    jw->EndObject();
    jw->EndObject();

    jw->String("serverInfo");
    jw->StartObject();
    jw->String("name");
    jw->String(kServerName);
    jw->String("version");
    jw->String(VersionInfo::GetShortVersionInfo());
    jw->EndObject();
  });
}

// ----------------------------------------------------------------------------
// Tool exposure registry + reflection into MCP tool objects
// ----------------------------------------------------------------------------

// Control keys that must NEVER be surfaced as a tool input property: the model
// cannot be allowed to flip a gate, a limit, or the dry-run switch on a
// per-call basis. The 'mcp_*' names are the server-level gflags (gates,
// timeouts, output limit); 'dry_run' is the synthetic control key injected on
// GATED tools (see WriteMcpToolObject). An action flag matching any of these is
// scrubbed so it can never collide with these reserved keys.
//
// The gflags negated boolean spelling ('--nofoo' for '--foo=false') is reserved
// as well. A per-call argument key must exactly match an advertised property
// name and becomes a single '--name=value' argv element, so 'nomcp_*' cannot be
// smuggled in today; this keeps the check correct should a property ever be
// keyed by a negated name.
//
// The name is checked both as given and with a leading 'no' removed, so a
// 'no' that is just part of a name is handled correctly either way: a name
// that is reserved as spelled (even one starting with 'no') is still matched,
// and stripping 'no' from an unrelated name such as 'normal_xxx' only yields
// 'rmal_xxx', which is not reserved.
bool IsReservedName(StringPiece name) {
  return HasPrefixString(name, "mcp_") || name == "dry_run";
}

bool IsReservedControlFlag(const string& flag_name) {
  StringPiece name(flag_name);
  if (IsReservedName(name)) {
    return true;
  }
  if (HasPrefixString(name, "no")) {
    name.remove_prefix(2);
    return IsReservedName(name);
  }
  return false;
}

// Connection arguments that the server fills in from its serve-level
// FLAGS_master_addresses rather than requiring the model to supply per call.
// These are the master-address positionals: the plural 'master_addresses'
// (cluster / table actions; never overridable) and the singular
// 'master_address' (master actions; required, and restricted to the configured
// masters).
bool IsInjectedConnectionArg(const string& arg_name) {
  return arg_name == kMasterAddressesArg || arg_name == kMasterAddressArg;
}

// One exposed action and everything tools/list needs to describe it and
// tools/call needs to invoke it. Built by BuildMcpToolRegistry() by walking the
// action tree once; the tree must outlive the registry because 'chain' and
// 'action' point into it.
struct McpToolEntry {
  // The flattened tool name, e.g. "table_describe" (see McpToolName()).
  string tool_name;
  // The mode chain from the root to the action's parent (chain.front() is the
  // root), exactly as the disposition lookup and BuildHelpXML walk expect it.
  vector<Mode*> chain;
  const Action* action;
  DispositionInfo disposition;
};

// Computes the MCP tool name for an action: its full command path with the path
// separators (spaces) turned into underscores. E.g. a chain {root, "table"}
// with action "describe" -> "table_describe"; {root, "tserver", "quiesce"} with
// action "status" -> "tserver_quiesce_status". Mode and action names may
// themselves contain underscores (e.g. "set_limit", "authz_cache"), so the
// joined name is NOT reversible by splitting on '_'; callers resolve a name back
// to an action via the registry, never by string-splitting. This function is
// the single source of truth so tools/list and tools/call agree.
string McpToolName(const vector<Mode*>& chain, const Action* action) {
  string name = DispositionCommandPath(chain, action);
  std::replace(name.begin(), name.end(), ' ', '_');
  return name;
}

// The server-level gates that decide which classified actions are exposed.
struct McpGates {
  bool allow_writes = false;
  bool allow_data_reads = false;
};

// Exposure rule: SURFACE is exposed; GATED only when 'allow_writes'; a tool
// tagged user_data additionally needs 'allow_data_reads'. REJECT / EXCLUDE /
// unclassified are never exposed.
bool IsExposed(const DispositionInfo& info, const McpGates& gates) {
  if (!info.classified) {
    return false;
  }
  if (info.user_data && !gates.allow_data_reads) {
    return false;
  }
  switch (info.disposition) {
    case Disposition::SURFACE:
      return true;
    case Disposition::GATED:
      return gates.allow_writes;
    case Disposition::REJECT:
    case Disposition::EXCLUDE:
      return false;
  }
  return false;
}

// Builds the exposure registry for the tree rooted at 'root': one record per
// action that should be visible as an MCP tool given 'gates'. This is what
// tools/list iterates and what tools/call resolves a tool name against. The
// returned entries borrow from 'root', which must outlive them.
//
// Tool names are not reversible (see McpToolName()), so two different command
// paths could in principle map to the same name (e.g. "a_b c" and "a b_c").
// That would make tools/call ambiguous, so it is a CHECK failure.
vector<McpToolEntry> BuildMcpToolRegistry(const Mode* root,
                                          const McpGates& gates) {
  vector<McpToolEntry> tools;
  unordered_map<string, string> paths_by_name;
  CHECK_OK(VisitActions(root, [&](const vector<Mode*>& chain,
                                  const Action* action) {
    const DispositionInfo info = DispositionFor(action);
    // The startup coverage invariant guarantees every action is classified;
    // IsExposed() stays defensive and never surfaces an unclassified action.
    if (!IsExposed(info, gates)) {
      return Status::OK();
    }
    McpToolEntry entry;
    entry.tool_name = McpToolName(chain, action);
    entry.chain = chain;
    entry.action = action;
    entry.disposition = info;
    const string path = DispositionCommandPath(chain, action);
    const auto inserted = paths_by_name.emplace(entry.tool_name, path);
    CHECK(inserted.second) << Substitute(
        "MCP tool name '$0' is ambiguous: both '$1' and '$2' map to it",
        entry.tool_name, inserted.first->second, path);
    tools.emplace_back(std::move(entry));
    return Status::OK();
  }));
  return tools;
}

// Maps a gflag type string (as reported by google::CommandLineFlagInfo::type)
// to a JSON Schema type: "bool" -> boolean; the integer families -> integer;
// "double" -> number; everything else -> string (the safe fallback for any
// gflag type we do not explicitly recognize, e.g. "string" and "uint64" edge
// cases). Never returns null.
const char* JsonSchemaTypeForGflag(const string& gflag_type) {
  if (gflag_type == "bool") {
    return "boolean";
  }
  if (gflag_type == "int32" || gflag_type == "int64" ||
      gflag_type == "uint32" || gflag_type == "uint64") {
    return "integer";
  }
  if (gflag_type == "double") {
    return "number";
  }
  return "string";
}

// Writes a JSON Schema "default" value for an optional flag, typed to match the
// property's JSON type where possible. gflags reports every default as a
// string; this converts it to a JSON boolean/number when the flag type calls
// for one and the string parses cleanly, falling back to the raw string
// otherwise so the schema is always well-formed.
void WriteFlagDefault(JsonWriter* jw,
                      const string& gflag_type,
                      const string& default_value) {
  if (gflag_type == "bool") {
    jw->Bool(default_value == "true");
    return;
  }
  if (gflag_type == "int32" || gflag_type == "int64") {
    int64_t v = 0;
    if (safe_strto64(default_value, &v)) {
      jw->Int64(v);
      return;
    }
  } else if (gflag_type == "uint32" || gflag_type == "uint64") {
    // Parse as unsigned: a uint64 default above INT64_MAX would fail
    // safe_strto64() and fall back to a string, contradicting the integer type.
    uint64_t v = 0;
    if (safe_strtou64(default_value, &v)) {
      jw->Uint64(v);
      return;
    }
  } else if (gflag_type == "double") {
    double d = 0;
    if (safe_strtod(default_value.c_str(), &d)) {
      jw->Double(d);
      return;
    }
  }
  // Non-numeric flag type, or the default failed to parse as its numeric type:
  // emit it verbatim as a string so the schema is always well-formed.
  jw->String(default_value);
}

// Returns the master addresses configured at server launch
// (--master_addresses), split on ',' with surrounding whitespace removed and
// empty entries dropped. Empty if none were configured.
vector<string> ConfiguredMasterAddresses() {
  vector<string> addrs;
  for (const auto& a : strings::Split(FLAGS_master_addresses, ",",
                                      strings::SkipWhitespace())) {
    string addr(a.data(), a.size());
    StripWhiteSpace(&addr);
    addrs.emplace_back(std::move(addr));
  }
  return addrs;
}

// Whether 'name' is an optional flag the model may set per call (and so the
// schema advertises): neither a reserved control key nor a connection argument
// the server owns.
bool IsModelSettableOptional(const string& name) {
  return !IsReservedControlFlag(name) && !IsInjectedConnectionArg(name);
}

// Writes a single MCP tool object for 'entry' into the open array 'jw'. Mirrors
// Action::BuildHelpXML's argument handling: required args and the variadic arg
// become JSON Schema properties (and are listed in "required"), optional flags
// become typed properties keyed off the gflag type.
void WriteMcpToolObject(JsonWriter* jw, const McpToolEntry& entry) {
  const Action* action = entry.action;
  const ActionArgsDescriptor& args = action->args();

  jw->StartObject();

  jw->String("name");
  jw->String(entry.tool_name);

  // Description: the action description, plus its extra description if any, plus
  // a node-local locality note for tagged SURFACE tools.
  string description = action->description();
  if (action->extra_description()) {
    description += " ";
    description += *action->extra_description();
  }
  if (entry.disposition.disposition == Disposition::SURFACE &&
      entry.disposition.node_local) {
    description += " Note: this reflects only the local node this server runs "
                   "on, not the whole cluster.";
  }
  if (entry.disposition.user_data) {
    description += " Note: the output contains unredacted row-level user data.";
  }
  jw->String("description");
  jw->String(description);

  // inputSchema: a JSON Schema object.
  jw->String("inputSchema");
  jw->StartObject();
  {
    jw->String("type");
    jw->String("object");

    jw->String("properties");
    jw->StartObject();
    {
      // Required positional args -> string properties. The plural
      // 'master_addresses' is omitted: the server always fills it in from its
      // serve-level FLAGS_master_addresses and never lets a call override it.
      // The singular 'master_address' (single-master actions) stays REQUIRED
      // but is restricted to the configured masters (enumerated when known).
      // There is deliberately no default: the master these actions target is
      // significant (e.g. 'master_remove' removes it, and a leader-only
      // operation fails on a follower), so the caller must name it.
      for (const auto& r : args.required) {
        if (r.name == kMasterAddressArg) {
          const vector<string> masters = ConfiguredMasterAddresses();
          jw->String(r.name);
          jw->StartObject();
          jw->String("type");
          jw->String("string");
          jw->String("description");
          jw->String(Substitute("$0 Must be one of the masters the server was "
                                "configured with.", r.description));
          if (!masters.empty()) {
            jw->String("enum");
            jw->StartArray();
            for (const auto& m : masters) {
              jw->String(m);
            }
            jw->EndArray();
          }
          jw->EndObject();
          continue;
        }
        if (IsInjectedConnectionArg(r.name)) {
          continue;
        }
        jw->String(r.name);
        jw->StartObject();
        jw->String("type");
        jw->String("string");
        jw->String("description");
        jw->String(r.description);
        jw->EndObject();
      }
      // The variadic arg -> an array-of-strings property (required variadic).
      if (args.variadic) {
        const ActionArgsDescriptor::Arg& v = *args.variadic;
        jw->String(v.name);
        jw->StartObject();
        jw->String("type");
        jw->String("array");
        jw->String("items");
        jw->StartObject();
        jw->String("type");
        jw->String("string");
        jw->EndObject();
        jw->String("description");
        jw->String(v.description);
        jw->EndObject();
      }
      // Optional flags -> typed properties, type read from gflags exactly as
      // the XML walk reads it. Control flags are never surfaced.
      for (const auto& o : args.optional) {
        if (!IsModelSettableOptional(o.name)) {
          continue;
        }
        google::CommandLineFlagInfo gflag_info =
            google::GetCommandLineFlagInfoOrDie(o.name.c_str());
        // The action may override the gflag's description / default.
        const string description_str =
            o.description.value_or(gflag_info.description);
        const string default_str =
            o.default_value.value_or(gflag_info.default_value);

        jw->String(o.name);
        jw->StartObject();
        jw->String("type");
        jw->String(JsonSchemaTypeForGflag(gflag_info.type));
        jw->String("description");
        jw->String(description_str);
        jw->String("default");
        WriteFlagDefault(jw, gflag_info.type, default_str);
        jw->EndObject();
      }
      // GATED tools carry a synthetic 'dry_run' control property. It is an
      // MCP-level switch, NOT a gflag: when set true, tools/call reconstructs and
      // returns the exact command that would run instead of executing it. It is
      // surfaced only on GATED tools (a write is the only thing worth previewing)
      // and, being reserved by IsReservedControlFlag, can never collide with a
      // real action flag. It is intentionally absent from required[].
      if (entry.disposition.disposition == Disposition::GATED) {
        jw->String("dry_run");
        jw->StartObject();
        jw->String("type");
        jw->String("boolean");
        jw->String("description");
        jw->String("If true, do not execute; return the exact kudu command that "
                   "would run, making no changes.");
        jw->String("default");
        jw->Bool(false);
        jw->EndObject();
      }
    }
    jw->EndObject(); // properties

    // required[]: required args (except the server-injected plural
    // 'master_addresses') and the (required) variadic arg. Optional flags are
    // never required.
    jw->String("required");
    jw->StartArray();
    for (const auto& r : args.required) {
      if (r.name == kMasterAddressesArg) {
        continue;
      }
      jw->String(r.name);
    }
    if (args.variadic) {
      jw->String(args.variadic->name);
    }
    jw->EndArray();
  }
  jw->EndObject(); // inputSchema

  // annotations: hint the host about read-only vs destructive behavior so it
  // can decide whether to prompt. GATED tools are mutating (not read-only) and
  // potentially destructive; SURFACE tools are read-only.
  jw->String("annotations");
  jw->StartObject();
  if (entry.disposition.disposition == Disposition::GATED) {
    jw->String("readOnlyHint");
    jw->Bool(false);
    jw->String("destructiveHint");
    jw->Bool(true);
  } else {
    jw->String("readOnlyHint");
    jw->Bool(true);
  }
  jw->EndObject();

  jw->EndObject(); // tool
}

// Builds the JSON-RPC response to a 'tools/list' request by emitting one MCP
// tool object per entry in the pre-built exposure registry.
string BuildToolsListResponse(const rapidjson::Value* id,
                              const vector<McpToolEntry>& tools) {
  return BuildResultResponse(id, [&](JsonWriter* jw) {
    jw->String("tools");
    jw->StartArray();
    for (const auto& entry : tools) {
      WriteMcpToolObject(jw, entry);
    }
    jw->EndArray();
  });
}

// Builds the action tree the serve session reflects into MCP tools. Delegates
// to the shared BuildRootMode() (declared in tool_action.h, defined in
// tool_action_root.cc). The root mode's name is irrelevant to reflection: it is
// dropped from every command path (DispositionCommandPath) and tool name
// (McpToolName).
unique_ptr<Mode> BuildMcpRootMode() {
  return BuildRootMode("kudu");
}

// Determines the protocol version to report in the initialize response. If the
// client's params carry a supported protocolVersion, it is echoed back;
// otherwise the server's default supported version is returned.
string NegotiateProtocolVersion(const JsonReader& reader,
                                const rapidjson::Value* root) {
  const rapidjson::Value* params = nullptr;
  if (reader.ExtractObject(root, "params", &params).ok()) {
    string requested;
    if (reader.ExtractString(params, "protocolVersion", &requested).ok() &&
        !requested.empty() && IsSupportedProtocolVersion(requested)) {
      return requested;
    }
  }
  return kDefaultProtocolVersion;
}

// ----------------------------------------------------------------------------
// tools/call: dispatch a read-only action and capture its text output
// ----------------------------------------------------------------------------

// Converts a scalar JSON value to the string form the CLI's positional and
// gflag parsing expects. Strings pass through; numbers and booleans are
// stringified as gflags would render them. Returns false for a non-scalar
// (object / array / null), which cannot be a positional arg or a flag value.
bool JsonScalarToString(const rapidjson::Value& v, string* out) {
  if (v.IsString()) {
    *out = v.GetString();
    return true;
  }
  if (v.IsBool()) {
    *out = v.GetBool() ? "true" : "false";
    return true;
  }
  if (v.IsInt()) {
    *out = SimpleItoa(v.GetInt());
    return true;
  }
  if (v.IsInt64()) {
    *out = SimpleItoa(v.GetInt64());
    return true;
  }
  if (v.IsUint()) {
    *out = SimpleItoa(v.GetUint());
    return true;
  }
  if (v.IsUint64()) {
    *out = SimpleItoa(v.GetUint64());
    return true;
  }
  if (v.IsDouble()) {
    *out = SimpleDtoa(v.GetDouble());
    return true;
  }
  return false;
}

// Client-side connection and security flags that the child 'kudu' honors when
// it talks to the cluster (see BuildMessenger() / CreateKuduClient() in
// tool_action_common.cc). If the operator set any of them on 'kudu mcp serve',
// the same value is forwarded to every tool child so that, e.g., a custom
// --sasl_protocol_name applies to the tool calls as well. Other server-level
// flags are deliberately NOT forwarded.
const char* const kForwardedServeFlags[] = {
  "negotiation_timeout_ms",
  "rpc_max_message_size",
  "sasl_protocol_name",
  "timeout_ms",
  "tls_ciphers",
  "tls_ciphersuites",
  "tls_min_version",
};

// Returns --name=value pairs for every kForwardedServeFlags entry that was
// explicitly set on this server process, skipping any name in 'per_call' (a
// per-call value takes precedence). "Explicitly set" is gflags' notion of
// is_default == false: the flag was assigned on the command line or via
// SetCommandLineOption(), regardless of the value. A flag explicitly set to its
// default value is therefore still forwarded; only a flag never set is
// omitted, and then the child picks up the same compiled-in default anyway.
// Unlike CheckFlagsAndWarn() in util/flags.cc, there is deliberately no extra
// current_value != default_value filter: forwarding a default value is
// harmless (the child would resolve to the same value) and keeps the dry-run
// output an exact echo of what the operator put on the serve command line.
vector<pair<string, string>> ForwardedServeFlags(
    const vector<pair<string, string>>& per_call) {
  vector<pair<string, string>> flags;
  for (const char* name : kForwardedServeFlags) {
    google::CommandLineFlagInfo info;
    if (!google::GetCommandLineFlagInfo(name, &info) || info.is_default) {
      continue;
    }
    const bool overridden = std::any_of(
        per_call.begin(), per_call.end(),
        [&](const pair<string, string>& f) { return f.first == name; });
    if (!overridden) {
      flags.emplace_back(name, info.current_value);
    }
  }
  return flags;
}

// Builds a successful JSON-RPC 'tools/call' response carrying an MCP tool
// result: content is a single text block, and 'isError' distinguishes a normal
// result from a tool-execution error (a non-OK action Status). Note this is a
// SUCCESSFUL JSON-RPC result even when is_error is true -- the tool ran and
// reported a failure, which is not a protocol-level error.
string BuildToolResultResponse(const rapidjson::Value* id,
                               const string& text,
                               bool is_error) {
  return BuildResultResponse(id, [&](JsonWriter* jw) {
    jw->String("content");
    jw->StartArray();
    {
      jw->StartObject();
      jw->String("type");
      jw->String("text");
      jw->String("text");
      jw->String(text);
      jw->EndObject();
    }
    jw->EndArray();
    jw->String("isError");
    jw->Bool(is_error);
  });
}

// Finds a member of the 'arguments' object by name, or nullptr if 'arguments'
// is absent / not an object / lacks the member.
const rapidjson::Value* FindArgument(const rapidjson::Value* arguments,
                                     const string& key) {
  if (arguments == nullptr || !arguments->IsObject()) {
    return nullptr;
  }
  auto it = arguments->FindMember(key.c_str());
  if (it == arguments->MemberEnd()) {
    return nullptr;
  }
  return &it->value;
}

// Returns 'raw' unchanged if it is made up entirely of characters a POSIX shell
// treats literally; otherwise wraps it in single quotes (escaping any embedded
// single quote as the usual '\'' sequence) so the reconstructed dry-run command
// line is copy-pasteable into a shell without reinterpretation. An empty string
// becomes '' so it survives as a distinct (empty) argument.
string ShellQuote(const string& raw) {
  if (!raw.empty()) {
    bool safe = true;
    for (const char c : raw) {
      const bool ok = std::isalnum(static_cast<unsigned char>(c)) ||
                      c == '_' || c == '-' || c == '.' || c == '/' ||
                      c == ':' || c == ',' || c == '=' || c == '@' ||
                      c == '+' || c == '%';
      if (!ok) {
        safe = false;
        break;
      }
    }
    if (safe) {
      return raw;
    }
  }
  string out = "'";
  for (const char c : raw) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out += c;
    }
  }
  out += "'";
  return out;
}

// Reconstructs the faithful, copy-pasteable command line a GATED tools/call
// would execute, for the dry-run path: the model sees exactly what would run
// instead of it running. Format:
//   kudu <chain-names...> <action-name> <positionals-in-declared-order> \
//        [--opt=value ...]
// Positional order matches args().required declaration order (which is why the
// injected master-address value is emitted at its real slot -- it is part of the
// command), followed by any variadic values, then the optional flags that were
// supplied, in declaration order. Each argument is shell-quoted as needed.
string BuildDryRunCommand(const McpToolEntry& entry,
                          const unordered_map<string, string>& required_args,
                          const vector<string>& variadic_args,
                          const vector<pair<string, string>>& optional_args) {
  const ActionArgsDescriptor& args = entry.action->args();
  ostringstream cmd;
  // DispositionCommandPath drops the root and joins mode names + action with
  // spaces, e.g. "table add_range_partition".
  cmd << "kudu " << DispositionCommandPath(entry.chain, entry.action);
  for (const auto& r : args.required) {
    const string* value = FindOrNull(required_args, r.name);
    // Every required arg (injected ones included) was marshalled before this
    // point, so it is present; stay defensive against a future refactor.
    if (value != nullptr) {
      cmd << " " << ShellQuote(*value);
    }
  }
  for (const auto& v : variadic_args) {
    cmd << " " << ShellQuote(v);
  }
  for (const auto& o : optional_args) {
    cmd << " --" << o.first << "=" << ShellQuote(o.second);
  }
  return cmd.str();
}

// Builds the argv for the child 'kudu' process that executes a tools/call in
// isolation. argv[0] is the absolute path to this server's own 'kudu' binary.
//
// The child's main() runs gflags::ParseCommandLineNonHelpFlags(remove_flags=true),
// which strips any token it recognizes as a flag from ANY position in argv before
// the tool framework matches the remaining tokens to a mode/action/positionals.
// That means a positional value beginning with '-' (e.g. a table name of
// "--version" or "--flagfile=/etc/passwd") would otherwise be swallowed and
// interpreted as a child flag rather than treated as data -- flag injection. To
// prevent it, we emit every real flag (the "--name=value" optionals) FIRST, then a
// bare "--" end-of-flags sentinel, and only then the positional tokens (mode-chain
// names, action name, required positionals, variadic values). gflags stops flag
// processing at the "--" and leaves everything after it as literal positionals, so
// user-controlled values can no longer masquerade as flags. Flags are
// position-independent to gflags, so leading the optionals does not change which
// flags the child sees; the positional order still mirrors BuildDryRunCommand (the
// injected master-address positional stays in its declared slot).
vector<string> BuildToolArgv(const string& kudu_binary_path,
                             const McpToolEntry& entry,
                             const unordered_map<string, string>& required_args,
                             const vector<string>& variadic_args,
                             const vector<pair<string, string>>& optional_args) {
  const ActionArgsDescriptor& args = entry.action->args();
  vector<string> argv;
  argv.emplace_back(kudu_binary_path);
  // All real flags first, so gflags parses them before the "--" terminator.
  for (const auto& o : optional_args) {
    argv.emplace_back(Substitute("--$0=$1", o.first, o.second));
  }
  // End-of-flags sentinel: everything after this is a literal positional and can
  // never be reinterpreted as a child flag.
  argv.emplace_back("--");
  // The mode chain names (skipping the root at index 0), then the action name --
  // e.g. {"table", "describe"} -- each as its own argv element.
  for (size_t i = 1; i < entry.chain.size(); i++) {
    argv.emplace_back(entry.chain[i]->name());
  }
  argv.emplace_back(entry.action->name());
  for (const auto& r : args.required) {
    const string* value = FindOrNull(required_args, r.name);
    if (value != nullptr) {
      argv.emplace_back(*value);
    }
  }
  for (const auto& v : variadic_args) {
    argv.emplace_back(v);
  }
  return argv;
}

// Runs 'argv' as a child process to completion or until 'timeout_seconds'
// elapses, feeding it no stdin and capturing its stdout and stderr. On timeout
// the child is SIGKILLed and '*timed_out' is set. At most 'max_output_bytes' of
// each stream are kept (0 means no limit); '*truncated' reports whether
// anything was dropped.
// '*wait_status' receives the reaped waitpid() status. Returns non-OK only for a
// failure to spawn / read / reap (a server-side problem); a failed *tool* is a
// normal return with a non-zero '*wait_status' or '*timed_out'.
//
// The concurrent, deadline-bounded drain-and-reap lives in
// Subprocess::WaitAndCollect() so it can be shared and tested independently;
// this function just wires the child up the way tool actions expect (no stdin)
// and hands the outcome to InterpretChildOutcome().
Status RunToolChild(const vector<string>& argv, int timeout_seconds,
                    int64_t max_output_bytes, string* out, string* err,
                    int* wait_status, bool* timed_out, bool* truncated) {
  Subprocess p(argv);
  p.ShareParentStdin(false);
  p.ShareParentStdout(false);
  p.ShareParentStderr(false);
  RETURN_NOT_OK_PREPEND(p.Start(), "could not start tool subprocess");

  // Close the child's stdin so anything that reads it sees EOF at once; tool
  // actions consume no stdin.
  const int stdin_fd = p.ReleaseChildStdinFd();
  if (stdin_fd >= 0) {
    ::close(stdin_fd);
  }

  return p.WaitAndCollect(MonoDelta::FromSeconds(timeout_seconds),
                          out, err, timed_out, wait_status,
                          max_output_bytes, truncated);
}

// Handles a 'tools/call' request. Resolves the tool name against the pre-built
// (already gated) registry, marshals the JSON arguments into the
// required/variadic/optional shapes the CLI expects, then EXECUTES the action in
// a fresh child 'kudu' process (see RunToolChild) and returns the child's output
// as an MCP tool result. Running each call in its own process is the crash
// boundary: a CLI action written for the run-once world may CHECK / LOG(FATAL) /
// exit() on bad state, which in-process would kill the whole server; isolated in
// a child it becomes an isError result and the serve loop lives on.
// Protocol-level problems (bad params, unknown tool) return a JSON-RPC error; an
// action-level failure returns a successful result carrying isError=true.
//
// 'hidden_tools' maps the name of every tool that exists but is hidden by a
// closed gate to the reason, so such a call gets an honest rejection rather
// than "unknown tool".
string HandleToolsCall(const JsonReader& reader,
                       const rapidjson::Value* req,
                       const rapidjson::Value* id,
                       const vector<McpToolEntry>& tools,
                       const unordered_map<string, string>& hidden_tools,
                       const string& kudu_binary_path) {
  const rapidjson::Value* params = nullptr;
  if (!reader.ExtractObject(req, "params", &params).ok()) {
    return BuildErrorResponse(id, kJsonRpcInvalidParams,
                              "Invalid params: missing 'params' object");
  }

  string name;
  if (!reader.ExtractString(params, "name", &name).ok()) {
    return BuildErrorResponse(id, kJsonRpcInvalidParams,
                              "Invalid params: missing tool 'name'");
  }

  // Resolve the name against the active (gated) registry.
  const McpToolEntry* entry = nullptr;
  for (const auto& e : tools) {
    if (e.tool_name == name) {
      entry = &e;
      break;
    }
  }
  if (entry == nullptr) {
    // Defense in depth: a gated tool is not merely hidden while its gate is
    // closed, it is explicitly rejected here -- and with an honest message
    // distinguishing "the gate is closed" from "no such tool".
    const string* reason = FindOrNull(hidden_tools, name);
    if (reason != nullptr) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: tool '$0' $1", name, *reason));
    }
    return BuildErrorResponse(
        id, kJsonRpcInvalidParams,
        Substitute("Invalid params: unknown tool '$0'", name));
  }
  const bool gated = entry->disposition.disposition == Disposition::GATED;

  // 'arguments' is optional; absent (or an explicit null) means no arguments.
  const rapidjson::Value* arguments = nullptr;
  if (params->HasMember("arguments")) {
    const rapidjson::Value& a = (*params)["arguments"];
    if (a.IsObject()) {
      arguments = &a;
    } else if (!a.IsNull()) {
      return BuildErrorResponse(id, kJsonRpcInvalidParams,
                                "Invalid params: 'arguments' must be an object");
    }
  }

  const ActionArgsDescriptor& args = entry->action->args();

  // Reject any argument key the tool's inputSchema does not advertise, so a
  // typo (e.g. 'dryrun' or a misspelled flag) fails loudly instead of the tool
  // silently running without it. The accepted set mirrors WriteMcpToolObject.
  if (arguments != nullptr) {
    unordered_set<string> accepted;
    for (const auto& r : args.required) {
      if (!IsInjectedConnectionArg(r.name) || r.name == kMasterAddressArg) {
        accepted.insert(r.name);
      }
    }
    if (args.variadic) {
      accepted.insert(args.variadic->name);
    }
    for (const auto& o : args.optional) {
      if (IsModelSettableOptional(o.name)) {
        accepted.insert(o.name);
      }
    }
    if (gated) {
      accepted.insert("dry_run");
    }
    for (const auto& m : arguments->GetObject()) {
      const string key = m.name.GetString();
      if (ContainsKey(accepted, key)) {
        continue;
      }
      if (key == kMasterAddressesArg) {
        // The cluster a tool talks to is fixed by the operator at launch and
        // reached with the operator's credentials; the model may not redirect
        // it.
        return BuildErrorResponse(
            id, kJsonRpcInvalidParams,
            Substitute("Invalid params: argument '$0' is set by the server "
                       "(--master_addresses) and cannot be overridden per call",
                       key));
      }
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: unknown argument '$0' for tool '$1'",
                     key, name));
    }
  }

  // Dry-run is a synthetic MCP-level control, accepted ONLY for GATED tools
  // (for any other tool it is an unknown argument and was rejected above).
  // When set, we still validate/marshal every argument (so malformed args are
  // still reported as -32602, never a partial mutation) but return the
  // reconstructed command instead of running it. A present-but-non-boolean
  // value is rejected rather than ignored: ignoring '"dry_run":"true"' would
  // run the mutation the caller meant to preview.
  bool dry_run = false;
  if (gated) {
    const rapidjson::Value* dr = FindArgument(arguments, "dry_run");
    if (dr != nullptr) {
      if (!dr->IsBool()) {
        return BuildErrorResponse(
            id, kJsonRpcInvalidParams,
            "Invalid params: argument 'dry_run' must be a boolean");
      }
      dry_run = dr->GetBool();
    }
  }

  unordered_map<string, string> required_args;
  vector<string> variadic_args;

  // Required positional args, in declaration order (mirrors MarshalArgs).
  for (const auto& r : args.required) {
    const rapidjson::Value* v = FindArgument(arguments, r.name);
    if (IsInjectedConnectionArg(r.name)) {
      const vector<string> masters = ConfiguredMasterAddresses();
      if (masters.empty()) {
        return BuildToolResultResponse(
            id,
            "No master addresses are configured. Launch the server with "
            "'kudu mcp serve --master_addresses=<addr>[,<addr>...]'.",
            /*is_error=*/true);
      }
      string value;
      if (r.name == kMasterAddressArg) {
        // Single-master action: the caller must name one of the configured
        // masters. There is no default (see WriteMcpToolObject).
        if (v == nullptr) {
          return BuildErrorResponse(
              id, kJsonRpcInvalidParams,
              Substitute("Invalid params: missing required argument '$0' "
                         "(one of the configured masters: $1)",
                         r.name, JoinStrings(masters, ",")));
        }
        if (!JsonScalarToString(*v, &value)) {
          return BuildErrorResponse(
              id, kJsonRpcInvalidParams,
              Substitute("Invalid params: argument '$0' must be a scalar",
                         r.name));
        }
        if (std::find(masters.begin(), masters.end(), value) ==
            masters.end()) {
          return BuildErrorResponse(
              id, kJsonRpcInvalidParams,
              Substitute("Invalid params: argument '$0' must be one of the "
                         "configured masters ($1), got '$2'",
                         r.name, JoinStrings(masters, ","), value));
        }
      } else {
        // The plural master_addresses is always the configured value; a
        // per-call value was rejected above.
        value = JoinStrings(masters, ",");
      }
      required_args[r.name] = std::move(value);
      continue;
    }
    if (v == nullptr) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: missing required argument '$0'", r.name));
    }
    string value;
    if (!JsonScalarToString(*v, &value)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: argument '$0' must be a scalar", r.name));
    }
    required_args[r.name] = std::move(value);
  }

  // Variadic arg (at most one, at the end): a non-empty JSON array of scalars.
  if (args.variadic) {
    const string& vname = args.variadic->name;
    const rapidjson::Value* v = FindArgument(arguments, vname);
    if (v == nullptr || !v->IsArray() || v->Empty()) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: argument '$0' must be a non-empty array",
                     vname));
    }
    for (const auto& elem : v->GetArray()) {
      string value;
      if (!JsonScalarToString(elem, &value)) {
        return BuildErrorResponse(
            id, kJsonRpcInvalidParams,
            Substitute("Invalid params: elements of argument '$0' must be "
                       "scalars", vname));
      }
      variadic_args.emplace_back(std::move(value));
    }
  }

  // Collect the optional flags supplied for this call, in declaration order,
  // validating scalar-ness. Reserved control flags (mcp_* / dry_run) and
  // connection args are never taken from per-call arguments. This is shared by
  // the dry-run and execute paths below: for dry-run it renders the command, for
  // execute it becomes the child's --name=value argv elements.
  vector<pair<string, string>> optional_supplied;
  for (const auto& o : args.optional) {
    if (!IsModelSettableOptional(o.name)) {
      continue;
    }
    const rapidjson::Value* v = FindArgument(arguments, o.name);
    if (v == nullptr) {
      continue;
    }
    string value;
    if (!JsonScalarToString(*v, &value)) {
      return BuildErrorResponse(
          id, kJsonRpcInvalidParams,
          Substitute("Invalid params: argument '$0' must be a scalar", o.name));
    }
    optional_supplied.emplace_back(o.name, std::move(value));
  }

  // The child's flags: connection/security flags the operator set on 'serve'
  // first, then the per-call optionals (which take precedence; see
  // ForwardedServeFlags()).
  vector<pair<string, string>> child_flags =
      ForwardedServeFlags(optional_supplied);
  child_flags.insert(child_flags.end(),
                     optional_supplied.begin(), optional_supplied.end());

  // Dry-run: the args are fully validated above, so any malformed argument has
  // already produced a -32602. Reconstruct the faithful command line and return
  // it WITHOUT executing anything.
  if (dry_run) {
    const string command =
        BuildDryRunCommand(*entry, required_args, variadic_args, child_flags);
    return BuildToolResultResponse(id, command, /*is_error=*/false);
  }

  // Execute: run the action in a fresh child 'kudu' process. Optional flags go
  // on the child's command line (not process-global gflags), and its stdout is
  // the tool result -- no cout redirection or flag save/restore is needed
  // because the child is a separate process. Crucially, if the action aborts
  // (CHECK / LOG(FATAL) / exit / a failed flag validator) only the child dies;
  // RunToolChild reaps it and InterpretChildOutcome turns any non-clean exit
  // into an isError result, so the serve loop survives to answer the next call.
  if (kudu_binary_path.empty()) {
    return BuildToolResultResponse(
        id,
        "The MCP server could not locate its own 'kudu' binary, so no tool can "
        "be executed in this configuration.",
        /*is_error=*/true);
  }
  const vector<string> argv = BuildToolArgv(
      kudu_binary_path, *entry, required_args, variadic_args, child_flags);

  const int timeout_sec =
      gated ? FLAGS_mcp_gated_tool_timeout_sec : FLAGS_mcp_tool_timeout_sec;
  string child_out;
  string child_err;
  int wait_status = 0;
  bool timed_out = false;
  bool truncated = false;
  const Status run_status = RunToolChild(argv, timeout_sec,
                                         FLAGS_mcp_max_output_bytes,
                                         &child_out, &child_err, &wait_status,
                                         &timed_out, &truncated);
  if (!run_status.ok()) {
    // We could not spawn or reap the child at all -- a server-side failure, not
    // a tool failure. Surface it as a tool error so the loop still survives.
    return BuildToolResultResponse(
        id,
        Substitute("Failed to execute tool '$0': $1", name,
                   run_status.ToString()),
        /*is_error=*/true);
  }
  ToolOutcome outcome = InterpretChildOutcome(
      wait_status, timed_out, timeout_sec, /*mutating=*/gated,
      child_out, child_err);
  if (truncated) {
    outcome.text += Substitute(
        "\n[truncated: tool output exceeded --mcp_max_output_bytes=$0]",
        FLAGS_mcp_max_output_bytes);
  }
  return BuildToolResultResponse(id, outcome.text, outcome.is_error);
}

// ----------------------------------------------------------------------------
// JSON-RPC framing + method routing
// ----------------------------------------------------------------------------

// The result of applying JSON-RPC framing rules to one parsed input line.
struct ParsedRequest {
  enum class Kind {
    kRequest,  // well-formed request; 'id'/'method' are set and need a response
    kIgnore,   // notification (no id, or "notifications/*"): produce no output
    kError,    // malformed; 'error_line' holds the response to emit verbatim
  };
  Kind kind = Kind::kIgnore;
  // Borrows from the caller's live JsonReader; valid only when kind==kRequest.
  const rapidjson::Value* id = nullptr;
  string method;       // set when kind==kRequest
  string error_line;   // set when kind==kError
};

// Classifies an already-parsed JSON value 'root' per JSON-RPC framing rules:
// a non-object or a request missing 'method' yields kError (with the response
// line prebuilt); a notification (no 'id', or any "notifications/*" method)
// yields kIgnore; anything else is a kRequest carrying its id and method. This
// is pure transport framing -- no MCP method semantics live here.
ParsedRequest ClassifyRequest(const rapidjson::Value* root) {
  ParsedRequest req;
  if (!root->IsObject()) {
    // Valid JSON, but not a JSON-RPC request object (e.g. a bare number or an
    // array). We cannot recover an id, so reply with a null-id invalid
    // request. Batch requests (arrays) are not supported in v1.
    req.kind = ParsedRequest::Kind::kError;
    req.error_line = BuildErrorResponse(/*id=*/nullptr, kJsonRpcInvalidRequest,
                                        "Invalid Request");
    return req;
  }

  // The presence of an 'id' member is what distinguishes a request (expects a
  // response) from a notification (never answered). A null id still counts as
  // present per JSON-RPC.
  const bool has_id = root->HasMember("id");
  const rapidjson::Value* id = has_id ? &(*root)["id"] : nullptr;

  // Extract the method. It must be a string to be usable.
  const bool has_method =
      root->HasMember("method") && (*root)["method"].IsString();
  const string method = has_method ? (*root)["method"].GetString() : string();

  // MCP notifications (method "notifications/*") are one-way: never answered,
  // regardless of whether an id was (incorrectly) supplied.
  if (has_method && HasPrefixString(method, "notifications/")) {
    LOG(INFO) << "mcp: received notification '" << method << "' (no reply)";
    return req;  // kIgnore
  }

  // A request without an id is a JSON-RPC notification: produce no output.
  if (!has_id) {
    if (has_method) {
      LOG(INFO) << "mcp: received notification '" << method << "' (no reply)";
    }
    return req;  // kIgnore
  }

  // From here on we have a request that requires a response.

  // JSON-RPC 2.0 restricts 'id' to a String, Number, or Null. An id of any
  // other type (bool/object/array) is an Invalid Request: were we to process it,
  // the response id would have to be nulled (WriteJsonRpcId), leaving the client
  // unable to correlate the reply with its request -- silent data loss. Reject
  // it here instead. Per the spec, an unrecoverable/invalid id yields a null
  // response id, so the error carries no id.
  const bool id_is_valid_type =
      id->IsNull() || id->IsString() || id->IsNumber();
  if (!id_is_valid_type) {
    req.kind = ParsedRequest::Kind::kError;
    req.error_line = BuildErrorResponse(
        /*id=*/nullptr, kJsonRpcInvalidRequest,
        "Invalid Request: 'id' must be a string, number, or null");
    return req;
  }

  if (!has_method) {
    req.kind = ParsedRequest::Kind::kError;
    req.error_line = BuildErrorResponse(id, kJsonRpcInvalidRequest,
                                        "Invalid Request: missing 'method'");
    return req;
  }

  req.kind = ParsedRequest::Kind::kRequest;
  req.id = id;
  req.method = method;
  return req;
}

// Routes a well-formed request to its MCP handler and returns the response
// line. This is the MCP application layer: unlike ClassifyRequest above, it
// knows the concrete method names. 'method' is guaranteed non-empty and not a
// notification.
string DispatchMethod(const string& method,
                      const rapidjson::Value* id,
                      const JsonReader& reader,
                      const rapidjson::Value* root,
                      const vector<McpToolEntry>& tools,
                      const unordered_map<string, string>& hidden_tools,
                      const string& kudu_binary_path) {
  LOG(INFO) << "mcp: received request method '" << method << "'";
  if (method == "initialize") {
    return BuildInitializeResponse(id, NegotiateProtocolVersion(reader, root));
  }
  // The MCP spec requires a server to answer 'ping' promptly with an empty
  // result, at any point in the session.
  if (method == "ping") {
    return BuildResultResponse(id, [](JsonWriter* /*jw*/) {});
  }
  if (method == "tools/list") {
    return BuildToolsListResponse(id, tools);
  }
  if (method == "tools/call") {
    return HandleToolsCall(reader, root, id, tools, hidden_tools,
                           kudu_binary_path);
  }
  return BuildErrorResponse(id, kJsonRpcMethodNotFound,
                            Substitute("Method not found: $0", method));
}

} // anonymous namespace

// Runs the MCP serve loop over the provided streams: a single-threaded
// JSON-RPC 2.0 dispatcher implementing the MCP lifecycle.
//
// The loop reads newline-delimited JSON from 'in'. For each non-blank line it:
//   - parses the line with JsonReader; on parse failure it emits a JSON-RPC
//     parse-error response (-32700) with a null id and continues;
//   - distinguishes requests (which carry an 'id') from notifications (no 'id',
//     or any "notifications/*" method): notifications receive no response;
//   - dispatches requests by method:
//       * "initialize"  -> protocolVersion / capabilities.tools / serverInfo;
//       * "ping"        -> an empty result;
//       * "tools/list"  -> the surfaced tool registry;
//       * "tools/call"  -> see HandleToolsCall();
//       * unknown method -> JSON-RPC -32601 (method not found);
//       * a request with no method -> JSON-RPC -32600 (invalid request).
//
// Every response carries "jsonrpc":"2.0", echoes the request id exactly as
// sent, and contains EITHER "result" OR "error", never both. Blank lines are
// ignored. The loop terminates on EOF and returns Status::OK(). It is
// intentionally single-threaded; stdout carries the protocol only (glog goes to
// stderr), so 'out' must not be interleaved with other writers.
//
// 'kudu_binary_path' is the absolute path to this server's own 'kudu' binary;
// each tools/call executes its action in a fresh child of it, which is the
// crash boundary (a CHECK / LOG(FATAL) / exit / failed flag validator kills
// only the child and the loop lives on). The production entry point resolves it
// and fails fast when it cannot (see RunMcpServe), so a live server always has
// it. An empty path is still accepted -- tests that never execute a tool pass
// "" -- in which case HandleToolsCall returns an isError result rather than
// spawning; that path is defense-in-depth, not a normal server state.
//
// Factored out of the action runner so it can be unit tested against in-memory
// streams.
Status RunMcpServeLoop(istream& in, ostream& out,
                       const string& kudu_binary_path) {
  // Build the action tree once for this serve session and reflect it into the
  // exposure registry. The tree must outlive the registry (its entries borrow
  // from it), so both live for the whole loop.
  //
  // The startup invariant: fail loudly and immediately if any reachable CLI
  // action is not classified in the disposition table (i.e. a new action was
  // added without a disposition entry), rather than silently mis-handling it.
  unique_ptr<Mode> root = BuildMcpRootMode();
  ValidateDispositionCoverageOrDie(root.get());
  McpGates gates;
  gates.allow_writes = FLAGS_mcp_allow_writes;
  gates.allow_data_reads = FLAGS_mcp_allow_data_reads;
  const vector<McpToolEntry> tools = BuildMcpToolRegistry(root.get(), gates);

  // Every tool that exists but is hidden by a closed gate, mapped to the reason
  // (computed once from the registry with every gate open; building that
  // registry also CHECKs tool-name uniqueness across everything exposable). It
  // lets HandleToolsCall tell "this tool exists but its gate is closed" apart
  // from "no such tool" when a name misses the active registry.
  unordered_map<string, string> hidden_tools;
  {
    unordered_set<string> active;
    for (const auto& e : tools) {
      active.insert(e.tool_name);
    }
    McpGates all_open;
    all_open.allow_writes = true;
    all_open.allow_data_reads = true;
    for (const auto& e : BuildMcpToolRegistry(root.get(), all_open)) {
      if (ContainsKey(active, e.tool_name)) {
        continue;
      }
      vector<string> kinds;
      vector<string> flags;
      if (e.disposition.disposition == Disposition::GATED &&
          !gates.allow_writes) {
        kinds.emplace_back("is a gated (mutating) tool");
        flags.emplace_back("--mcp_allow_writes");
      }
      if (e.disposition.user_data && !gates.allow_data_reads) {
        kinds.emplace_back("returns row-level user data");
        flags.emplace_back("--mcp_allow_data_reads");
      }
      DCHECK(!flags.empty()) << e.tool_name;
      hidden_tools.emplace(e.tool_name, Substitute(
          "$0 and is not enabled; start the server with $1 to use it",
          JoinStrings(kinds, " and "), JoinStrings(flags, " and ")));
    }
  }

  string line;
  while (std::getline(in, line)) {
    // A failed response write from the previous iteration means the peer is no
    // longer reading our replies. Stop rather than keep consuming requests we
    // can no longer answer. fail() covers both badbit and failbit.
    if (out.fail()) {
      return Status::IOError(
          "mcp: error writing JSON-RPC response to stdout");
    }

    // Ignore blank / whitespace-only lines: nothing to parse, nothing to
    // answer.
    if (line.find_first_not_of(" \t\n\r\v\f") == string::npos) {
      continue;
    }

    JsonReader reader(line);
    Status s = reader.Init();
    if (!s.ok()) {
      // Malformed JSON must not take down a long-lived server. Reply with a
      // JSON-RPC parse error (null id, since we could not read one) and carry
      // on with the next line.
      LOG(WARNING) << "mcp: parse error on JSON-RPC line: " << s.ToString();
      out << BuildErrorResponse(/*id=*/nullptr, kJsonRpcParseError, "Parse error")
          << std::endl;
      continue;
    }

    // Framing: classify the parsed line as a request needing a response, an
    // ignorable notification, or a malformed line with a ready-to-emit error.
    // Method-specific handling stays out of the loop (see DispatchMethod).
    const rapidjson::Value* root = reader.root();
    ParsedRequest req = ClassifyRequest(root);
    switch (req.kind) {
      case ParsedRequest::Kind::kIgnore:
        continue;
      case ParsedRequest::Kind::kError:
        out << req.error_line << std::endl;
        continue;
      case ParsedRequest::Kind::kRequest:
        out << DispatchMethod(req.method, req.id, reader, root, tools,
                              hidden_tools, kudu_binary_path)
            << std::endl;
        continue;
    }
  }

  // Input side ('in', stdin). std::getline() always ends the loop by setting
  // failbit, so fail() alone cannot tell a clean shutdown from an error:
  // check bad() for an unrecoverable read error first, then require eofbit.
  // failbit without eofbit means the input stopped for another reason (e.g. a
  // line longer than string::max_size()).
  if (in.bad()) {
    return Status::IOError("mcp: error reading JSON-RPC input from stdin");
  }
  if (!in.eof()) {
    return Status::IOError("mcp: JSON-RPC input from stdin ended before EOF");
  }
  // Output side ('out', stdout). Nothing sets failbit on a healthy output
  // stream, so fail() (badbit or failbit) catches a failed write of the last
  // response, which the in-loop check never got to see.
  if (out.fail()) {
    return Status::IOError("mcp: error writing JSON-RPC response to stdout");
  }
  // Clean EOF: the peer closed its end. Terminate the server normally.
  return Status::OK();
}

namespace {

Status RunMcpServe(const RunnerContext& /*context*/) {
  // Resolve this server's own 'kudu' binary up front: every tools/call executes
  // its action in a fresh child of it, so without it no tool can ever run.
  string kudu_binary_path;
  RETURN_NOT_OK_PREPEND(GetKuduToolAbsolutePathSafe(&kudu_binary_path),
                        "could not locate the 'kudu' binary to execute tools");
  return RunMcpServeLoop(std::cin, std::cout, kudu_binary_path);
}

} // anonymous namespace

unique_ptr<Mode> BuildMcpMode() {
  unique_ptr<Action> serve =
      ActionBuilder("serve", &RunMcpServe)
      .Description("Serve Kudu admin actions as MCP tools over stdio")
      .McpDisposition(Disposition::REJECT)
      .ExtraDescription("Reads newline-delimited JSON-RPC requests from stdin "
                        "and writes responses to stdout, one line per request, "
                        "until EOF. Intended to be launched by an MCP host.")
      .AddOptionalParameter("mcp_allow_writes")
      .AddOptionalParameter("mcp_allow_data_reads")
      .AddOptionalParameter("mcp_tool_timeout_sec")
      .AddOptionalParameter("mcp_gated_tool_timeout_sec")
      .AddOptionalParameter("mcp_max_output_bytes")
      // The master addresses configured once at server launch
      // (--master_addresses). Injected into tools/call dispatch for actions
      // that need them; a call cannot point a tool at another cluster.
      .AddOptionalParameter("master_addresses")
      .Build();

  return ModeBuilder("mcp")
      .Description("Operate as a Model Context Protocol (MCP) server")
      .AddAction(std::move(serve))
      .Build();
}

} // namespace tools
} // namespace kudu
