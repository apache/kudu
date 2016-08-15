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

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using std::deque;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

namespace {

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

ActionBuilder& ActionBuilder::AddGflag(const string& gflag) {
  gflags_.push_back(gflag);
  return *this;
}

unique_ptr<Action> ActionBuilder::Build() {
  unique_ptr<Action> action(new Action());
  action->label_ = label_;
  action->runner_ = runner_;
  action->gflags_ = gflags_;
  return action;
}

Action::Action() {
}

Status Action::Run(const vector<Mode*>& chain, deque<string> args) const {
  return runner_(chain, this, args);
}

string Action::BuildHelp(const vector<Mode*>& chain) const {
  string msg = Substitute("$0 $1", BuildUsageString(chain), name());
  string gflags_msg;
  for (const auto& gflag : gflags_) {
    google::CommandLineFlagInfo gflag_info =
        google::GetCommandLineFlagInfoOrDie(gflag.c_str());
    string noun;
    int last_underscore_idx = gflag.rfind('_');
    if (last_underscore_idx != string::npos &&
        last_underscore_idx != gflag.size() - 1) {
      noun = gflag.substr(last_underscore_idx + 1);
    } else {
      noun = gflag;
    }
    msg += Substitute(" [-$0=<$1>]", gflag, noun);
    gflags_msg += google::DescribeOneFlag(gflag_info);
  }
  msg += "\n";
  msg += Substitute("$0\n", label_.description);
  msg += gflags_msg;
  return msg;
}

Status ParseAndRemoveArg(const char* arg_name,
                         deque<string>* remaining_args,
                         string* parsed_arg) {
  if (remaining_args->empty()) {
    return Status::InvalidArgument(Substitute("must provide $0", arg_name));
  }
  *parsed_arg = remaining_args->front();
  remaining_args->pop_front();
  return Status::OK();
}

} // namespace tools
} // namespace kudu
