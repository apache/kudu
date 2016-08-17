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

#include "kudu/tools/tool_action.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

string FakeDescribeOneFlag(const ActionArgsDescriptor::Arg& arg) {
  string res = google::DescribeOneFlag({
    arg.name,        // name
    "string",        // type
    arg.description, // description
    "",              // current_value
    "",              // default_value
    "",              // filename
    false,           // has_validator_fn
    true,            // is_default
    nullptr          // flag_ptr
  });

  // Strip the first dash from the description; this is a positional parameter
  // so let's make sure it looks like one.
  string::size_type first_dash_idx = res.find("-");
  DCHECK_NE(string::npos, first_dash_idx);
  return res.substr(0, first_dash_idx) + res.substr(first_dash_idx + 1);
}

string BuildUsageString(const vector<Mode*>& chain) {
  string modes = JoinMapped(chain, [](Mode* a){ return a->name(); }, " ");
  return Substitute("Usage: $0", modes);
}

} // anonymous namespace

ModeBuilder::ModeBuilder(const Label& label)
    : label_(label) {
}

ModeBuilder& ModeBuilder::AddMode(unique_ptr<Mode> mode) {
  submodes_.push_back(std::move(mode));
  return *this;
}

ModeBuilder& ModeBuilder::AddAction(unique_ptr<Action> action) {
  actions_.push_back(std::move(action));
  return *this;
}

unique_ptr<Mode> ModeBuilder::Build() {
  unique_ptr<Mode> mode(new Mode());
  mode->label_ = label_;
  mode->submodes_ = std::move(submodes_);
  mode->actions_ = std::move(actions_);
  return mode;
}

// Get help for this mode, passing in its parent mode chain.
string Mode::BuildHelp(const vector<Mode*>& chain) const {
  string msg = Substitute("$0 <action>\n", BuildUsageString(chain));
  msg += "Action can be one of the following:\n";
  for (const auto& m : modes()) {
    msg += Substitute("  $0 : $1\n", m->name(), m->description());
  }
  for (const auto& a : actions()) {
    msg += Substitute("  $0 : $1\n", a->name(), a->description());
  }
  return msg;
}

Mode::Mode() {
}

ActionBuilder::ActionBuilder(const Label& label, const ActionRunner& runner)
    : label_(label),
      runner_(runner) {
}

ActionBuilder& ActionBuilder::AddRequiredParameter(
    const ActionArgsDescriptor::Arg& arg) {
  args_.required.push_back(arg);
  return *this;
}

ActionBuilder& ActionBuilder::AddRequiredVariadicParameter(
    const ActionArgsDescriptor::Arg& arg) {
  DCHECK(!args_.variadic);
  args_.variadic = arg;
  return *this;
}

ActionBuilder& ActionBuilder::AddOptionalParameter(const string& param) {
#ifndef NDEBUG
  // Make sure this gflag exists.
  string option;
  DCHECK(google::GetCommandLineOption(param.c_str(), &option));
#endif
  args_.optional.push_back(param);
  return *this;
}

unique_ptr<Action> ActionBuilder::Build() {
  unique_ptr<Action> action(new Action());
  action->label_ = label_;
  action->runner_ = runner_;
  action->args_ = args_;
  return action;
}

Action::Action() {
}

Status Action::Run(const vector<Mode*>& chain,
                   const unordered_map<string, string>& required_args,
                   const vector<string>& variadic_args) const {
  return runner_({ chain, this, required_args, variadic_args });
}

string Action::BuildHelp(const vector<Mode*>& chain) const {
  string usage_msg = Substitute("$0 $1", BuildUsageString(chain), name());
  string desc_msg;
  for (const auto& param : args_.required) {
    usage_msg += Substitute(" <$0>", param.name);
    desc_msg += FakeDescribeOneFlag(param);
  }
  if (args_.variadic) {
    const ActionArgsDescriptor::Arg& param = args_.variadic.get();
    usage_msg += Substitute(" <$0>...", param.name);
    desc_msg += FakeDescribeOneFlag(param);
  }
  for (const auto& param : args_.optional) {
    google::CommandLineFlagInfo gflag_info =
        google::GetCommandLineFlagInfoOrDie(param.c_str());
    string noun;
    string::size_type last_underscore_idx = param.rfind('_');
    if (last_underscore_idx != string::npos &&
        last_underscore_idx != param.size() - 1) {
      noun = param.substr(last_underscore_idx + 1);
    } else {
      noun = param;
    }
    usage_msg += Substitute(" [-$0=<$1>]", param, noun);
    desc_msg += google::DescribeOneFlag(gflag_info);
  }
  string msg = usage_msg;
  msg += "\n";
  msg += Substitute("$0\n", label_.description);
  msg += desc_msg;
  return msg;
}

} // namespace tools
} // namespace kudu
