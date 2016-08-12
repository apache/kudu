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

#include <deque>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <vector>

#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action.h"
#include "kudu/util/flags.h"
#include "kudu/util/logging.h"
#include "kudu/util/status.h"

DECLARE_bool(help);
DECLARE_bool(helpshort);
DECLARE_string(helpon);
DECLARE_string(helpmatch);
DECLARE_bool(helppackage);
DECLARE_bool(helpxml);

using std::cerr;
using std::cout;
using std::deque;
using std::endl;
using std::string;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

int DispatchCommand(const vector<Action>& chain, const deque<string>& args) {
  DCHECK(!chain.empty());
  Action action = chain.back();
  Status s = action.run(chain, args);
  if (s.ok()) {
    return 0;
  } else {
    cerr << s.ToString() << endl;
    return 1;
  }
}

int RunTool(const Action& root, int argc, char** argv, bool show_help) {
  // Initialize arg parsing state.
  vector<Action> chain = { root };

  // Parse the arguments, matching them up with actions.
  for (int i = 1; i < argc; i++) {
    const Action* cur = &chain.back();
    const auto& sub_actions = cur->sub_actions;
    if (sub_actions.empty()) {
      // We've reached an invokable action.
      if (show_help) {
        cerr << cur->help(chain) << endl;
        return 1;
      } else {
        // Invoke it with whatever arguments remain.
        deque<string> remaining_args;
        for (int j = i; j < argc; j++) {
          remaining_args.push_back(argv[j]);
        }
        return DispatchCommand(chain, remaining_args);
      }
    }

    // This action is not invokable. Interpret the next command line argument
    // as a subaction and continue parsing.
    const Action* next = nullptr;
    for (const auto& a : sub_actions) {
      if (a.name == argv[i]) {
        next = &a;
        break;
      }
    }

    if (next == nullptr) {
      // We couldn't find a subaction for the next argument. Raise an error.
      string msg = Substitute("$0 $1\n",
                              BuildActionChainString(chain), argv[i]);
      msg += BuildHelpString(sub_actions, BuildUsageString(chain));
      Status s = Status::InvalidArgument(msg);
      cerr << s.ToString() << endl;
      return 1;
    }

    // We're done parsing this argument. Loop and continue.
    chain.emplace_back(*next);
  }

  // We made it to a subaction with no arguments left. Run the subaction if
  // possible, otherwise print its help.
  const Action* last = &chain.back();
  if (show_help || !last->run) {
    cerr << last->help(chain) << endl;
    return 1;
  } else {
    DCHECK(last->run);
    return DispatchCommand(chain, {});
  }
}

} // namespace tools
} // namespace kudu

static bool ParseCommandLineFlags(int* argc, char*** argv) {
  // Hide the regular gflags help unless --helpfull is used.
  //
  // Inspired by https://github.com/gflags/gflags/issues/43#issuecomment-168280647.
  bool show_help = false;
  gflags::ParseCommandLineNonHelpFlags(argc, argv, true);
  if (FLAGS_help ||
      FLAGS_helpshort ||
      !FLAGS_helpon.empty() ||
      !FLAGS_helpmatch.empty() ||
      FLAGS_helppackage ||
      FLAGS_helpxml) {
    FLAGS_help = false;
    FLAGS_helpshort = false;
    FLAGS_helpon = "";
    FLAGS_helpmatch = "";
    FLAGS_helppackage = false;
    FLAGS_helpxml = false;
    show_help = true;
  }
  gflags::HandleCommandLineHelpFlags();
  return show_help;
}

int main(int argc, char** argv) {
  kudu::tools::Action root = {
      argv[0],
      "The root action", // doesn't matter, won't get printed
      nullptr,
      &kudu::tools::BuildNonLeafActionHelpString,
      {
          kudu::tools::BuildFsAction(),
          kudu::tools::BuildTabletAction()
      },
      {} // no gflags
  };
  string usage = root.help({ root });
  google::SetUsageMessage(usage);
  bool show_help = ParseCommandLineFlags(&argc, &argv);
  FLAGS_logtostderr = true;
  kudu::InitGoogleLoggingSafe(argv[0]);
  return kudu::tools::RunTool(root, argc, argv, show_help);
}
