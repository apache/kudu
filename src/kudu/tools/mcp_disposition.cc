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

#include "kudu/tools/mcp_disposition.h"

#include <stddef.h>

#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"

using std::optional;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

const char* DispositionToString(Disposition d) {
  switch (d) {
    case Disposition::SURFACE: return "SURFACE";
    case Disposition::GATED:   return "GATED";
    case Disposition::REJECT:  return "REJECT";
    case Disposition::EXCLUDE: return "EXCLUDE";
  }
  return "UNKNOWN";
}

string DispositionCommandPath(const vector<Mode*>& chain,
                              const Action* action) {
  string path;
  // Skip the root mode (chain.front()); join the remaining mode names.
  for (size_t i = 1; i < chain.size(); i++) {
    if (!path.empty()) {
      path += " ";
    }
    path += chain[i]->name();
  }
  if (action != nullptr) {
    if (!path.empty()) {
      path += " ";
    }
    path += action->name();
  }
  return path;
}

DispositionInfo DispositionFor(const Action* action) {
  CHECK(action != nullptr);
  const optional<Disposition>& d = action->mcp_disposition();
  if (!d.has_value()) {
    // Not classified. classified stays false; caller must not surface it.
    return DispositionInfo();
  }
  DispositionInfo info;
  info.disposition = *d;
  info.node_local = action->mcp_node_local();
  info.unsafe = action->mcp_unsafe();
  info.user_data = action->mcp_user_data();
  info.classified = true;
  return info;
}

namespace {

// Recursive worker for VisitActions(): 'chain' is the full chain from the root
// to 'mode', with 'mode' as its last element.
Status VisitModeActions(vector<Mode*>* chain,
                        const Mode* mode,
                        const ActionVisitor& visitor) {
  for (const auto& action : mode->actions()) {
    RETURN_NOT_OK(visitor(*chain, action.get()));
  }
  for (const auto& submode : mode->modes()) {
    chain->push_back(submode.get());
    const Status s = VisitModeActions(chain, submode.get(), visitor);
    chain->pop_back();
    RETURN_NOT_OK(s);
  }
  return Status::OK();
}

} // anonymous namespace

Status VisitActions(const Mode* root, const ActionVisitor& visitor) {
  CHECK(root != nullptr);
  vector<Mode*> chain = { const_cast<Mode*>(root) };
  return VisitModeActions(&chain, root, visitor);
}

string DumpDispositions(const Mode* root) {
  string out;
  CHECK_OK(VisitActions(root, [&](const vector<Mode*>& chain,
                                  const Action* action) {
    const DispositionInfo info = DispositionFor(action);
    string tags;
    if (info.node_local) {
      tags += " node_local";
    }
    if (info.unsafe) {
      tags += " unsafe";
    }
    if (info.user_data) {
      tags += " user_data";
    }
    out.append(Substitute("$0\t$1$2\n",
                          DispositionCommandPath(chain, action),
                          info.classified ? DispositionToString(info.disposition)
                                          : "UNCLASSIFIED",
                          tags));
    return Status::OK();
  }));
  return out;
}

Status ValidateDispositionCoverage(const Mode* root) {
  return VisitActions(root, [](const vector<Mode*>& chain,
                               const Action* action) {
    if (!DispositionFor(action).classified) {
      return Status::NotFound(Substitute(
          "CLI action is not classified for the MCP server (missing "
          "ActionBuilder::McpDisposition): '$0'",
          DispositionCommandPath(chain, action)));
    }
    return Status::OK();
  });
}

void ValidateDispositionCoverageOrDie(const Mode* root) {
  const Status s = ValidateDispositionCoverage(root);
  CHECK(s.ok()) << "MCP disposition coverage is incomplete: " << s.ToString();
}

} // namespace tools
} // namespace kudu
