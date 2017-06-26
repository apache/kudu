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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>

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
//          fs
//         |  |
//      +--+  +--+
//      |        |
//   format    dump
//             | |
//         +--+  +--+
//         |        |
//      cfile      uuid
//
// Given this tree:
// - "<program> fs" will show all of fs's possible actions.
// - "<program> fs format" will format a filesystem.
// - "<program> fs dump uuid" will print a filesystem's UUID.

// Builds a new mode (non-leaf) node.
class ModeBuilder {
 public:
  // Creates a new ModeBuilder with a specific name (e.g. "fs"). The name
  // uniquely identifies the mode amongst its siblings in the tree.
  explicit ModeBuilder(std::string name);

  // Sets the description of this mode (e.g. "Operate on a local Kudu
  // filesystem"), to be used when printing help.
  //
  // Required.
  ModeBuilder& Description(const std::string& desc);

  // Adds a new mode (non-leaf child node) to this builder.
  ModeBuilder& AddMode(std::unique_ptr<Mode> mode);

  // Adds a new action (leaf child node) to this builder.
  ModeBuilder& AddAction(std::unique_ptr<Action> action);

  // Creates a mode using builder state.
  //
  // May only be called once.
  std::unique_ptr<Mode> Build();

 private:
  const std::string name_;

  std::string description_;

  std::vector<std::unique_ptr<Mode>> submodes_;

  std::vector<std::unique_ptr<Action>> actions_;
};

// A non-leaf node in the tree, representing a logical grouping for actions or
// more modes.
class Mode {
 public:

  // Returns the help for this mode given its parent mode chain.
  std::string BuildHelp(const std::vector<Mode*>& chain) const;

  // Returns the help xml for this mode and all child modes
  std::string BuildHelpXML(const std::vector<Mode*>& chain) const;

  const std::string& name() const { return name_; }

  const std::string& description() const { return description_; }

  const std::vector<std::unique_ptr<Mode>>& modes() const { return submodes_; }

  const std::vector<std::unique_ptr<Action>>& actions() const { return actions_; }

 private:
  friend class ModeBuilder;

  Mode() = default;

  std::string name_;

  std::string description_;

  std::vector<std::unique_ptr<Mode>> submodes_;

  std::vector<std::unique_ptr<Action>> actions_;
};

// Function signature for any operation represented by an Action.
//
// When run, the operation receives the parent mode chain, the current action,
// and the command line arguments.
//
// Prior to running, arguments registered by the action are put into the
// context. In the event that a required parameter is missing or there are
// unexpected arguments on the command line, an error is returned and the
// runner will not be invoked.
//
// Note: only required and variadic args are put in the context; it is expected
// that operations access optional args via gflag variables (i.e. FLAGS_foo).
struct RunnerContext {
  std::vector<Mode*> chain;
  const Action* action;
  std::unordered_map<std::string, std::string> required_args;
  std::vector<std::string> variadic_args;
};
typedef std::function<Status(const RunnerContext&)> ActionRunner;

// Describes all of the arguments used by an action. At runtime, the tool will
// parse these arguments out of the command line and marshal them into a
// key/value argument map (see Run()).
struct ActionArgsDescriptor {
  struct Arg {
    std::string name;
    std::string description;
  };

  // Holds an optional command line argument flag.
  struct Flag {
    // The gflag name.
    std::string name;
    // A default value to override the default gflag value.
    boost::optional<std::string> default_value;
    // A description to override the gflag description.
    boost::optional<std::string> description;
  };

  // Positional (required) command line arguments.
  std::vector<Arg> required;

  // Key-value command line arguments. These must correspond to defined gflags.
  //
  // Optional by definition, though some are required internally
  // (e.g. fs_wal_dir).
  std::vector<Flag> optional;

  // Variable length command line argument. There may be at most one per
  // Action, and it's always found at the end of the command line.
  boost::optional<Arg> variadic;
};

// Builds a new action (leaf) node.
class ActionBuilder {
 public:
  // Creates a new ActionBuilder with a specific name (e.g. "format") and
  // action runner. The name uniquely identifies the action amongst its
  // siblings in the tree.
  ActionBuilder(std::string name, ActionRunner runner);

  // Sets the description of this action (e.g. "Format a new Kudu filesystem"),
  // to be used when printing the parent mode's help and the action's help.
  //
  // Required.
  ActionBuilder& Description(const std::string& description);

  // Sets the long description of this action. If provided, will added to this
  // action's help following Description().
  ActionBuilder& ExtraDescription(const std::string& extra_description);

  // Add a new required parameter to this builder.
  //
  // This parameter will be parsed as a positional argument following the name
  // of the action. The order in which required parameters are added to the
  // builder reflects the order they are expected to be parsed from the command
  // line.
  ActionBuilder& AddRequiredParameter(
      const ActionArgsDescriptor::Arg& arg);

  // Add a new required variable-length parameter to this builder.
  //
  // This parameter will be parsed following all other positional parameters.
  // All remaining positional arguments on the command line will be parsed into
  // this parameter.
  //
  // There may be at most one variadic parameter defined per action.
  ActionBuilder& AddRequiredVariadicParameter(
      const ActionArgsDescriptor::Arg& arg);

  // Add a new optional parameter to this builder.
  //
  // This parameter will be parsed by the gflags system, and thus can be
  // provided by the user at any point in the command line. It must match a
  // previously-defined gflag; if a gflag with the same name cannot be found,
  // the tool will crash.
  //
  // The default value and description of the flag can be optionally overriden,
  // for cases where the values are action-dependent. Otherwise, the default
  // value and description from the gflag declaration will be used.
  ActionBuilder& AddOptionalParameter(std::string param,
                                      boost::optional<std::string> default_value = boost::none,
                                      boost::optional<std::string> description = boost::none);

  // Creates an action using builder state.
  std::unique_ptr<Action> Build();

 private:
  const std::string name_;

  std::string description_;

  boost::optional<std::string> extra_description_;

  ActionRunner runner_;

  ActionArgsDescriptor args_;
};

// A leaf node in the tree, representing a logical operation taken by the tool.
class Action {
 public:
  enum HelpMode {
    // Return the full help text, including descriptions for each
    // of the arguments.
    FULL_HELP,
    // Return only a single-line usage statement.
    USAGE_ONLY
  };

  // Returns the help for this action given its parent mode chain.
  std::string BuildHelp(const std::vector<Mode*>& chain,
                        HelpMode mode = FULL_HELP) const;

  // Returns the help xml for this action
  std::string BuildHelpXML(const std::vector<Mode*>& chain) const;

  // Runs the operation represented by this action, given a parent mode chain
  // and marshaled command line arguments.
  Status Run(const std::vector<Mode*>& chain,
             const std::unordered_map<std::string, std::string>& required_args,
             const std::vector<std::string>& variadic_args) const;

  const std::string& name() const { return name_; }

  const std::string& description() const { return description_; }

  const boost::optional<std::string>& extra_description() const {
    return extra_description_;
  }

  const ActionArgsDescriptor& args() const { return args_; }

 private:
  friend class ActionBuilder;

  Action() = default;

  // Sets optional flag parameter default value in cases where it has been
  // overridden from the default gflag value.
  void SetOptionalParameterDefaultValues() const;

  std::string name_;

  std::string description_;

  boost::optional<std::string> extra_description_;

  ActionRunner runner_;

  ActionArgsDescriptor args_;
};

// Returns new nodes for each major mode.
std::unique_ptr<Mode> BuildClusterMode();
std::unique_ptr<Mode> BuildFsMode();
std::unique_ptr<Mode> BuildLocalReplicaMode();
std::unique_ptr<Mode> BuildMasterMode();
std::unique_ptr<Mode> BuildPbcMode();
std::unique_ptr<Mode> BuildPerfMode();
std::unique_ptr<Mode> BuildRemoteReplicaMode();
std::unique_ptr<Mode> BuildTableMode();
std::unique_ptr<Mode> BuildTabletMode();
std::unique_ptr<Mode> BuildTServerMode();
std::unique_ptr<Mode> BuildWalMode();

} // namespace tools
} // namespace kudu
