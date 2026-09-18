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

// Server-internal declarations for the MCP server (tool_action_mcp.cc). These
// are the pieces the tests need to exercise directly but that do not belong in
// the public tool_action_mcp.h, whose only export is RunMcpServeLoop.

#include <string>

namespace kudu {
namespace tools {

// The MCP tool result derived from a finished child tool process, before JSON
// serialization.
struct ToolOutcome {
  std::string text;
  bool is_error;
};

// Interprets a finished child tool process into an MCP tool result. 'wait_status'
// is the raw waitpid() status (meaningful only when !timed_out); 'timed_out' is
// true if the child was killed for exceeding its deadline. A clean exit (code 0)
// yields the child's stdout as the (non-error) result. Any other outcome -- a
// non-zero exit, death by signal (e.g. a CHECK / LOG(FATAL) abort in a CLI action
// written for the run-once world), or a timeout -- is a tool-execution error: it
// is reported as isError=true text rather than being allowed to take down the
// long-lived server. The child's stderr (the action's own diagnostics) is
// preferred for the error text, falling back to stdout when stderr is empty.
//
// 'mutating' is true for a GATED tool: when such a child times out or dies by
// a signal, the error text also warns that the change may have been partially
// applied and the cluster state should be checked.
//
// Declared here (external linkage) so the unit test can exercise the exit-0 /
// non-zero / signal / timeout paths deterministically without spawning a real
// crashing binary.
ToolOutcome InterpretChildOutcome(int wait_status, bool timed_out,
                                  int timeout_seconds, bool mutating,
                                  const std::string& out,
                                  const std::string& err);

} // namespace tools
} // namespace kudu
