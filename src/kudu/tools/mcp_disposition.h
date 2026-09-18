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

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "kudu/util/status.h"

namespace kudu {
namespace tools {

class Action;
class Mode;

// The safety classification of a single CLI action for the MCP server. Every
// action reachable from the CLI action tree resolves to exactly one of these
// before it can be surfaced as an MCP tool.
//
//  - SURFACE: read-only. Always exposed. Node-local reads are surfaced but
//             tagged (see DispositionInfo::node_local) because they only mean
//             anything when the server runs on that node.
//  - GATED:   cluster mutation. Hidden unless --mcp_allow_writes is set;
//             surfaced with a host confirm prompt and dry-run.
//  - REJECT:  blocking / never-promptly-returning. Would freeze the
//             single-threaded serve loop (long-running admin ops and daemon
//             launchers). Never surfaced in v1.
//  - EXCLUDE: interactive (drives an editor / stdin) or node-local mutating.
//             Never surfaced.
//
// The underlying type is pinned to uint8_t so the enum is stored compactly on
// each Action. tool_action.h forward-declares it (enum class Disposition :
// uint8_t;) to hold it by value without depending on this header.
enum class Disposition : uint8_t {
  SURFACE,
  GATED,
  REJECT,
  EXCLUDE,
};

// Returns a stable ASCII name for 'd' (e.g. "SURFACE"). Never returns null.
const char* DispositionToString(Disposition d);

// Classification metadata for a single action.
struct DispositionInfo {
  // The action's disposition. Only meaningful when 'classified' is true.
  Disposition disposition = Disposition::EXCLUDE;

  // True if the action operates on the on-disk data of the single node the
  // server runs on (the fs / wal / pbc / local_replica families). Such actions
  // only mean anything when the server is co-located with that node's data;
  // node-local reads are still SURFACE-d but callers should tag them.
  bool node_local = false;

  // True for "unsafe_*" and other unsafe operations (e.g. raw Raft config
  // edits). Advisory metadata: a host may additionally hide these even when
  // their disposition is GATED.
  bool unsafe = false;

  // True if the action returns row-level user data (e.g. 'table scan', WAL
  // entry dumps, rowset dumps) rather than metadata. The CLI does not redact
  // such output by default, so a host should keep these hidden unless the
  // operator explicitly opts in to exposing user data to the model.
  bool user_data = false;

  // False if 'action' carries no disposition (ActionBuilder::McpDisposition was
  // never called), i.e. the action is not classified. When false, all other
  // fields are unspecified and must not be trusted. Callers MUST check this
  // before acting on the result.
  bool classified = false;
};

// Builds the full command path key for an action from its mode chain. 'chain'
// is the sequence of modes from the root to the action's parent (chain.front()
// is the root). The key drops the root mode's name and joins the remaining mode
// names followed by the action name with single spaces, e.g. a chain of
// {root, "tserver", "quiesce"} with action "status" yields
// "tserver quiesce status".
std::string DispositionCommandPath(const std::vector<Mode*>& chain,
                                   const Action* action);

// Returns the classification carried by 'action' (set via
// ActionBuilder::McpDisposition). Returns a DispositionInfo whose 'classified'
// field is false if the action carries no disposition (the caller must not
// surface such an action).
DispositionInfo DispositionFor(const Action* action);

// Callback for VisitActions(). 'chain' is the sequence of modes from the root
// to the action's parent (chain.front() is the root), as expected by
// DispositionCommandPath().
typedef std::function<Status(const std::vector<Mode*>& chain,
                             const Action* action)> ActionVisitor;

// Walks the entire action tree rooted at 'root' depth-first (a mode's own
// actions before its submodes) and invokes 'visitor' on every action. Stops at
// and returns the first non-OK status returned by 'visitor'.
Status VisitActions(const Mode* root, const ActionVisitor& visitor);

// Returns a human-readable table of the classification of every action in the
// tree rooted at 'root', one action per line (full command path, disposition,
// and node_local / unsafe / user_data tags). This is the generated audit view
// of the MCP safety surface -- what would otherwise be a hand-maintained table.
std::string DumpDispositions(const Mode* root);

// Walks the entire action tree rooted at 'root' and verifies that every action
// carries a disposition. Returns an error naming the first offending full
// command path if an action is not classified.
//
// This is the Status-returning form, suitable for unit tests. The MCP serve
// startup path should use ValidateDispositionCoverageOrDie() instead so a newly
// added but unclassified CLI action fails loudly and immediately.
Status ValidateDispositionCoverage(const Mode* root);

// CHECK-based form of ValidateDispositionCoverage() for use at server startup:
// crashes with the offending path if the invariant does not hold. A missing
// classification is a programming error (a CLI action was added without being
// classified), so failing fast at startup is the intended behavior.
void ValidateDispositionCoverageOrDie(const Mode* root);

} // namespace tools
} // namespace kudu
