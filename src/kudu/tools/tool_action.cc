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
#include <string>
#include <vector>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"

using std::deque;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

string BuildActionChainString(const vector<Action>& chain) {
  return JoinMapped(chain, [](const Action& a){ return a.name; }, " ");
}

string BuildUsageString(const vector<Action>& chain) {
  return Substitute("Usage: $0", BuildActionChainString(chain));
}

string BuildHelpString(const vector<Action>& sub_actions, string usage_str) {
  string msg = Substitute("$0 <action>\n", usage_str);
  msg += "Action can be one of the following:\n";
  for (const auto& a : sub_actions) {
    msg += Substitute("  $0 : $1\n", a.name, a.description);
  }
  return msg;
}

string BuildLeafActionHelpString(const vector<Action>& chain) {
  DCHECK(!chain.empty());
  Action action = chain.back();
  string msg = Substitute("$0", BuildUsageString(chain));
  string gflags_msg;
  for (const auto& gflag : action.gflags) {
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
  msg += Substitute("$0\n", action.description);
  msg += gflags_msg;
  return msg;
}

string BuildNonLeafActionHelpString(const vector<Action>& chain) {
  string usage = BuildUsageString(chain);
  DCHECK(!chain.empty());
  return BuildHelpString(chain.back().sub_actions, usage);
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
