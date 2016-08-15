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
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

class Action;
class Mode;

// The command line tool is structured as a tree with two kinds of nodes: modes
// and actions. Actions are leaf nodes, each representing a particular
// operation that the tool can take. Modes are non-leaf nodes that are
// basically just intuitive groupings of actions.
//
// Regardless of type, every node has a name which is used to match it against
// a command line argument during parsing. Additionally, every node has a
// description, displayed (along with the name) in help text.
//
// Every node (be it action or mode) has pointers to its children, but not to
// its parent mode. As such, operations that require information from the
// parent modes expect the caller to provide those modes as a "mode chain".
//
// Sample node tree:
//
//         root
//          |
//          |
//          |
//          fs
//         |  |
//      +--+  +--+
//      |        |
//   format   print_uuid
//
// Given this tree:
// - "<program> fs" will show all of fs's possible actions.
// - "<program> fs format" will format a filesystem.
// - "<program> fs print_uuid" will print a filesystem's UUID.

// Properties common to all nodes.
struct Label {
  // The node's name (e.g. "fs"). Uniquely identifies the node action amongst
  // its siblings in the tree.
  std::string name;

  // The node's description (e.g. "Operate on a local Kudu filesystem").
  std::string description;
};

// Builds a new mode (non-leaf) node.
class ModeBuilder {
 public:
  // Creates a new ModeBuilder with a specific label.
  explicit ModeBuilder(const Label& label);

  // Adds a new mode (non-leaf child node) to this builder.
  ModeBuilder& AddMode(std::unique_ptr<Mode> mode);

  // Adds a new action (leaf child node) to this builder.
  ModeBuilder& AddAction(std::unique_ptr<Action> action);

  // Creates a mode using builder state.
  //
  // May only be called once.
  std::unique_ptr<Mode> Build();

 private:
  const Label label_;

  std::vector<std::unique_ptr<Mode>> submodes_;

  std::vector<std::unique_ptr<Action>> actions_;
};

// A non-leaf node in the tree, representing a logical grouping for actions or
// more modes.
class Mode {
 public:

  // Returns the help for this mode given its parent mode chain.
  std::string BuildHelp(const std::vector<Mode*>& chain) const;

  const std::string& name() const { return label_.name; }

  const std::string& description() const { return label_.description; }

  const std::vector<std::unique_ptr<Mode>>& modes() const { return submodes_; }

  const std::vector<std::unique_ptr<Action>>& actions() const { return actions_; }

 private:
  friend class ModeBuilder;

  Mode();

  Label label_;

  std::vector<std::unique_ptr<Mode>> submodes_;

  std::vector<std::unique_ptr<Action>> actions_;
};

// Function signature for any operation represented by an action. When run, the
// operation receives the parent mode chain, the current action, and any
// remaining command line arguments.
typedef std::function<Status(const std::vector<Mode*>&,
                             const Action*,
                             std::deque<std::string>)> ActionRunner;

// Builds a new action (leaf) node.
class ActionBuilder {
 public:
  // Creates a new ActionBuilder with a specific label and action runner.
  ActionBuilder(const Label& label, const ActionRunner& runner);

  // Add a new gflag to this builder. They are used when generating help.
  ActionBuilder& AddGflag(const std::string& gflag);

  // Creates an action using builder state.
  std::unique_ptr<Action> Build();

 private:
  Label label_;

  ActionRunner runner_;

  std::vector<std::string> gflags_;
};

// A leaf node in the tree, representing a logical operation taken by the tool.
class Action {
 public:

  // Returns the help for this action given its parent mode chain.
  std::string BuildHelp(const std::vector<Mode*>& chain) const;

  // Runs the operation represented by this action, given a parent mode chain
  // and list of extra command line arguments.
  Status Run(const std::vector<Mode*>& chain, std::deque<std::string> args) const;

  const std::string& name() const { return label_.name; }

  const std::string& description() const { return label_.description; }

 private:
  friend class ActionBuilder;

  Action();

  Label label_;

  ActionRunner runner_;

  // This action's gflags (if any).
  std::vector<std::string> gflags_;
};

// Removes one argument from 'remaining_args' and stores it in 'parsed_arg'.
//
// If 'remaining_args' is empty, returns InvalidArgument with 'arg_name' in the
// message.
Status ParseAndRemoveArg(const char* arg_name,
                         std::deque<std::string>* remaining_args,
                         std::string* parsed_arg);

// Checks that 'args' is empty. If not, returns a bad status.
template <typename CONTAINER>
Status CheckNoMoreArgs(const std::vector<Mode*>& chain,
                       const Action* action,
                       const CONTAINER& args) {
  if (args.empty()) {
    return Status::OK();
  }
  DCHECK(!chain.empty());
  return Status::InvalidArgument(
      strings::Substitute("too many arguments: '$0'\n$1",
                          JoinStrings(args, " "), action->BuildHelp(chain)));
}

// Returns a new "fs" mode node.
std::unique_ptr<Mode> BuildFsMode();

// Returns a new "tablet" mode node.
std::unique_ptr<Mode> BuildTabletMode();

} // namespace tools
} // namespace kudu
