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

#include <memory>
#include <string>

#include "kudu/tools/tool_action.h"

using std::string;
using std::unique_ptr;

namespace kudu {
namespace tools {

// The single source of truth for the top-level CLI action tree (declared in
// tool_action.h). The 'kudu' binary's main() and any in-process consumer of
// the action tree delegate here so a newly added top-level mode is wired in
// exactly one place.
//
// This lives in its own translation unit rather than in tool_action.cc because
// tool_action.cc is compiled into the low-level kudu_tools_util library, and
// referencing every Build*Mode() factory from there would inject undefined
// symbols into every kudu_tools_util consumer (e.g. ksck) that does not link
// the tool_action_*.cc factories. This file is part of the CLI sources
// (KUDU_CLI_ACTION_SRCS, built into the kudu_cli_actions library), which
// already provide every factory, so it is a link-safe home.
unique_ptr<Mode> BuildRootMode(const string& name) {
  return ModeBuilder(name)
      .Description("Kudu Command Line Tools") // root mode description isn't printed
      .AddMode(BuildClusterMode())
      .AddMode(BuildDiagnoseMode())
      .AddMode(BuildFsMode())
      .AddMode(BuildHmsMode())
      .AddMode(BuildLocalReplicaMode())
      .AddMode(BuildMasterMode())
      .AddMode(BuildMcpMode())
      .AddMode(BuildPbcMode())
      .AddMode(BuildPerfMode())
      .AddMode(BuildRemoteReplicaMode())
      .AddMode(BuildTableMode())
      .AddMode(BuildTabletMode())
#if defined(KUDU_CLI_TEST_TOOL_ENABLED)
      .AddMode(BuildTestMode())
#endif
      .AddMode(BuildTxnMode())
      .AddMode(BuildTServerMode())
      .AddMode(BuildWalMode())
      .Build();
}

} // namespace tools
} // namespace kudu
