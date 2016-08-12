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

#include <deque>
#include <glog/logging.h>
#include <string>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

// Encapsulates all knowledge for a particular tool action.
//
// All actions are arranged in a tree. Leaf actions are invokable: they will
// do something meaningful when run() is called. Non-leaf actions do not have
// a run(); an attempt to invoke them will yield help() instead.
//
// Sample action tree:
//
//          fs
//         |  |
//      +--+  +--+
//      |        |
//   format   print_uuid
//
// Given this tree:
// - "<program> fs" will print some text explaining all of fs's actions.
// - "<program> fs format" will format a filesystem.
// - "<program> fs print_uuid" will print a filesystem's UUID.
struct Action {
  // The name of the action (e.g. "fs").
  std::string name;

  // The description of the action (e.g. "Operate on a local Kudu filesystem").
  std::string description;

  // Invokes an action, passing in the complete action chain and all remaining
  // command line arguments. The arguments are passed by value so that the
  // function can modify them if need be.
  std::function<Status(const std::vector<Action>&,
                       std::deque<std::string>)> run;

  // Get help for an action, passing in the complete action chain.
  std::function<std::string(const std::vector<Action>&)> help;

  // This action's children.
  std::vector<Action> sub_actions;

  // This action's gflags (if any).
  std::vector<std::string> gflags;
};

// Constructs a string with the names of all actions in the chain
// (e.g. "<program> fs format").
std::string BuildActionChainString(const std::vector<Action>& chain);

// Constructs a usage string (e.g. "Usage: <program> fs format").
std::string BuildUsageString(const std::vector<Action>& chain);

// Constructs a help string suitable for leaf actions.
std::string BuildLeafActionHelpString(const std::vector<Action>& chain);

// Constructs a help string suitable for non-leaf actions.
std::string BuildNonLeafActionHelpString(const std::vector<Action>& chain);

// Constructs a string appropriate for displaying program help, using
// 'sub_actions' as a list of actions to include and 'usage_str' as a string
// to prepend.
std::string BuildHelpString(const std::vector<Action>& sub_actions,
                            std::string usage_str);

// Removes one argument from 'remaining_args' and stores it in 'parsed_arg'.
//
// If 'remaining_args' is empty, returns InvalidArgument with 'arg_name' in the
// message.
Status ParseAndRemoveArg(const char* arg_name,
                         std::deque<std::string>* remaining_args,
                         std::string* parsed_arg);

// Checks that 'args' is empty. If not, returns a bad status.
template <typename CONTAINER>
Status CheckNoMoreArgs(const std::vector<Action>& chain,
                       const CONTAINER& args) {
  if (args.empty()) {
    return Status::OK();
  }
  DCHECK(!chain.empty());
  Action action = chain.back();
  return Status::InvalidArgument(strings::Substitute(
      "too many arguments\n$0", action.help(chain)));
}

// Returns the "fs" action node.
Action BuildFsAction();

// Returns the "tablet" action node.
Action BuildTabletAction();

} // namespace tools
} // namespace kudu
