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
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

// A no-op action runner for synthetic trees built in tests.
Status NoOpRunner(const RunnerContext& /*context*/) {
  return Status::OK();
}

unique_ptr<Action> MakeAction(const string& name) {
  return ActionBuilder(name, &NoOpRunner)
      .Description("synthetic test action")
      .Build();
}

// Resolves a command path (e.g. {"tserver", "quiesce", "status"}) against
// 'root', returning the mode chain (root..parent) and the leaf action. CHECKs
// if the path cannot be resolved.
void ResolvePath(const Mode* root,
                 const vector<string>& path,
                 vector<Mode*>* chain,
                 const Action** action) {
  ASSERT_FALSE(path.empty());
  chain->clear();
  chain->push_back(const_cast<Mode*>(root));
  const Mode* cur = root;
  // All but the last component are modes.
  for (size_t i = 0; i + 1 < path.size(); i++) {
    const Mode* next = nullptr;
    for (const auto& m : cur->modes()) {
      if (m->name() == path[i]) {
        next = m.get();
        break;
      }
    }
    ASSERT_NE(nullptr, next) << "no submode '" << path[i] << "'";
    chain->push_back(const_cast<Mode*>(next));
    cur = next;
  }
  // The last component is the action.
  const Action* found = nullptr;
  for (const auto& a : cur->actions()) {
    if (a->name() == path.back()) {
      found = a.get();
      break;
    }
  }
  ASSERT_NE(nullptr, found) << "no action '" << path.back() << "'";
  *action = found;
}

// Convenience: resolve 'path' against 'root' and return its disposition.
DispositionInfo Lookup(const Mode* root, const vector<string>& path) {
  vector<Mode*> chain;
  const Action* action = nullptr;
  ResolvePath(root, path, &chain, &action);
  // A fatal assertion in ResolvePath() only returns from it, not the caller, so
  // guard the null 'action' rather than dereferencing it in DispositionFor().
  // The recorded failure fails the test; otherwise the binary would SIGSEGV.
  if (action == nullptr) {
    return DispositionInfo();
  }
  return DispositionFor(action);
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Happy path: a representative action of each disposition class resolves to the
// expected classification and flags against the real action tree.
// ---------------------------------------------------------------------------
TEST(McpDispositionTest, RepresentativeActionsOfEachClass) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  // SURFACE: a core cluster read.
  {
    const DispositionInfo info = Lookup(root.get(), {"cluster", "ksck"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
    ASSERT_FALSE(info.node_local);
    ASSERT_FALSE(info.unsafe);
  }
  // GATED: a destructive cluster mutation.
  {
    const DispositionInfo info = Lookup(root.get(), {"table", "delete"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::GATED, info.disposition);
    ASSERT_FALSE(info.node_local);
  }
  // REJECT: a blocking long-runner.
  {
    const DispositionInfo info = Lookup(root.get(), {"cluster", "rebalance"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::REJECT, info.disposition);
  }
  // EXCLUDE: an interactive node-local action.
  {
    const DispositionInfo info = Lookup(root.get(), {"pbc", "edit"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::EXCLUDE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
  // SURFACE + node_local: a node-local read.
  {
    const DispositionInfo info = Lookup(root.get(), {"fs", "list"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
  // unsafe flag: an unsafe_* op is tagged.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tablet", "unsafe_replace_tablet"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::GATED, info.disposition);
    ASSERT_TRUE(info.unsafe);
  }
  // user_data flag: a row-level read is tagged.
  {
    const DispositionInfo info = Lookup(root.get(), {"table", "scan"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
    ASSERT_TRUE(info.user_data);
    ASSERT_FALSE(info.node_local);
  }
  // A metadata read is not tagged user_data.
  {
    const DispositionInfo info = Lookup(root.get(), {"table", "describe"});
    ASSERT_TRUE(info.classified);
    ASSERT_FALSE(info.user_data);
  }
}

// Actions that read row-level user data are all tagged, so a host can gate
// them separately from metadata reads.
TEST(McpDispositionTest, UserDataReadsAreTagged) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  const vector<vector<string>> kUserDataPaths = {
    {"table", "scan"},
    {"wal", "dump"},
    {"local_replica", "dump", "rowset"},
    {"local_replica", "dump", "wals"},
    {"fs", "dump", "cfile"},
    {"fs", "dump", "block"},
  };
  for (const auto& path : kUserDataPaths) {
    SCOPED_TRACE(JoinStrings(path, " "));
    const DispositionInfo info = Lookup(root.get(), path);
    ASSERT_TRUE(info.classified);
    ASSERT_TRUE(info.user_data);
  }
}

// Actions that mutate local state or run for a long time are never surfaced,
// even though parts of them look like reads.
TEST(McpDispositionTest, MutatingOrLongRunningActionsAreNotSurfaced) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  // 'fs check --repair' writes to disk.
  {
    const DispositionInfo info = Lookup(root.get(), {"fs", "check"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::EXCLUDE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
  // 'master add' is a multi-step job on the new master's node.
  {
    const DispositionInfo info = Lookup(root.get(), {"master", "add"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::EXCLUDE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
  // 'perf loadgen' runs long by design.
  {
    const DispositionInfo info = Lookup(root.get(), {"perf", "loadgen"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::REJECT, info.disposition);
  }
}

// Actions that read files on the local node are tagged node-local.
TEST(McpDispositionTest, LocalFileReadsAreNodeLocal) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  const vector<vector<string>> kNodeLocalPaths = {
    {"diagnose", "parse_stacks"},
    {"diagnose", "parse_metrics"},
    {"perf", "tablet_scan"},
  };
  for (const auto& path : kNodeLocalPaths) {
    SCOPED_TRACE(JoinStrings(path, " "));
    const DispositionInfo info = Lookup(root.get(), path);
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
}

// Multi-level (submode) command paths key on the full chain, not just parent
// mode + action.
TEST(McpDispositionTest, MultiLevelSubmodePaths) {
  unique_ptr<Mode> root = BuildRootMode("kudu");

  // tserver quiesce status: SURFACE read under the 'quiesce' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tserver", "quiesce", "status"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
  }
  // tserver quiesce start: GATED mutation under the 'quiesce' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"tserver", "quiesce", "start"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::GATED, info.disposition);
  }
  // table set_limit disk_size: GATED under the 'set_limit' submode.
  {
    const DispositionInfo info =
        Lookup(root.get(), {"table", "set_limit", "disk_size"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::GATED, info.disposition);
  }
  // fs dump uuid: SURFACE node-local read under the 'dump' submode.
  {
    const DispositionInfo info = Lookup(root.get(), {"fs", "dump", "uuid"});
    ASSERT_TRUE(info.classified);
    ASSERT_EQ(Disposition::SURFACE, info.disposition);
    ASSERT_TRUE(info.node_local);
  }
}

// The full command path key is built from the mode chain (minus root) plus the
// action name.
TEST(McpDispositionTest, CommandPathKeyBuilding) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  vector<Mode*> chain;
  const Action* action = nullptr;
  // Propagate ResolvePath()'s fatal assertions so a failed resolution stops the
  // test before DispositionCommandPath() dereferences a null 'action'.
  ASSERT_NO_FATAL_FAILURE(
      ResolvePath(root.get(), {"tserver", "quiesce", "status"}, &chain, &action));
  ASSERT_EQ("tserver quiesce status",
            DispositionCommandPath(chain, action));
}

// The whole action tree resolves: every action has exactly one table entry.
// This is the invariant that catches a newly added but unclassified CLI action.
TEST(McpDispositionTest, ValidateCoveragePassesAgainstCurrentTree) {
  unique_ptr<Mode> root = BuildRootMode("kudu");
  ASSERT_OK(ValidateDispositionCoverage(root.get()));
}

// The test-only "test" mode actions are explicitly classified EXCLUDE.
TEST(McpDispositionTest, TestModeIsExcluded) {
#if defined(KUDU_CLI_TEST_TOOL_ENABLED)
  unique_ptr<Mode> root = BuildRootMode("kudu");
  const DispositionInfo info = Lookup(root.get(), {"test", "mini_cluster"});
  ASSERT_TRUE(info.classified);
  ASSERT_EQ(Disposition::EXCLUDE, info.disposition);
#else
  GTEST_SKIP() << "test mode is not compiled in this build";
#endif
}

// VisitActions() reaches every action, including ones nested under submodes,
// with the full mode chain, and visits a mode's actions before its submodes.
TEST(McpDispositionTest, VisitActionsWalksWholeTree) {
  unique_ptr<Mode> submode = ModeBuilder("sub")
      .Description("synthetic submode")
      .AddAction(MakeAction("b"))
      .Build();
  unique_ptr<Mode> parent = ModeBuilder("top")
      .Description("synthetic parent mode")
      .AddAction(MakeAction("a"))
      .AddMode(std::move(submode))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(parent))
      .Build();

  vector<string> visited;
  ASSERT_OK(VisitActions(root.get(), [&](const vector<Mode*>& chain,
                                         const Action* action) {
    visited.emplace_back(DispositionCommandPath(chain, action));
    return Status::OK();
  }));
  ASSERT_EQ(vector<string>({ "top a", "top sub b" }), visited);

  // A non-OK status from the visitor stops the walk and is returned.
  visited.clear();
  const Status s = VisitActions(root.get(), [&](const vector<Mode*>& chain,
                                                const Action* action) {
    visited.emplace_back(DispositionCommandPath(chain, action));
    return Status::Aborted("stop");
  });
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  ASSERT_EQ(vector<string>({ "top a" }), visited);
}

// ---------------------------------------------------------------------------
// Bad paths.
// ---------------------------------------------------------------------------

// An unknown command path reports "not classified" rather than silently
// defaulting to some disposition.
TEST(McpDispositionTest, UnknownPathIsNotClassified) {
  // Build a synthetic tree with an action that is absent from the table.
  unique_ptr<Mode> child = ModeBuilder("nonsense")
      .Description("synthetic mode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(child))
      .Build();

  const DispositionInfo info = Lookup(root.get(), {"nonsense", "bogus_action"});
  ASSERT_FALSE(info.classified);
}

// The validator fails and names the offending full command path when the tree
// contains an action missing from the table.
TEST(McpDispositionTest, ValidatorFailsOnMissingClassification) {
  // "cluster" is a real mode, but "cluster bogus_action" is not in the table,
  // so the full path must be reported.
  unique_ptr<Mode> cluster = ModeBuilder("cluster")
      .Description("synthetic cluster mode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(cluster))
      .Build();

  const Status s = ValidateDispositionCoverage(root.get());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "cluster bogus_action");
}

// The validator reports the full multi-level path for an unclassified action
// nested under a submode.
TEST(McpDispositionTest, ValidatorReportsFullSubmodePath) {
  unique_ptr<Mode> submode = ModeBuilder("subthing")
      .Description("synthetic submode")
      .AddAction(MakeAction("bogus_action"))
      .Build();
  unique_ptr<Mode> parent = ModeBuilder("thing")
      .Description("synthetic parent mode")
      .AddMode(std::move(submode))
      .Build();
  unique_ptr<Mode> root = ModeBuilder("kudu")
      .Description("root")
      .AddMode(std::move(parent))
      .Build();

  const Status s = ValidateDispositionCoverage(root.get());
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "thing subthing bogus_action");
}

} // namespace tools
} // namespace kudu
