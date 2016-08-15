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
#include <memory>
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
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {
namespace tools {

int DispatchCommand(const vector<Mode*>& chain,
                    Action* action,
                    const deque<string>& args) {
  Status s = action->Run(chain, args);
  if (s.ok()) {
    return 0;
  } else {
    cerr << s.ToString() << endl;
    return 1;
  }
}

int RunTool(int argc, char** argv, bool show_help) {
  unique_ptr<Mode> root =
      ModeBuilder({ argv[0], "" }) // root mode description isn't printed
      .AddMode(BuildFsMode())
      .AddMode(BuildTabletMode())
      .Build();

  // Initialize arg parsing state.
  vector<Mode*> chain = { root.get() };

  // Parse the arguments, matching each to a mode or action.
  for (int i = 1; i < argc; i++) {
    Mode* cur = chain.back();
    Mode* next_mode = nullptr;
    Action* next_action = nullptr;

    // Match argument with a mode.
    for (const auto& m : cur->modes()) {
      if (m->name() == argv[i]) {
        next_mode = m.get();
        break;
      }
    }

    // Match argument with an action.
    for (const auto& a : cur->actions()) {
      if (a->name() == argv[i]) {
        next_action = a.get();
        break;
      }
    }

    // If both matched, there's an error with the tree.
    DCHECK(!next_mode || !next_action);

    if (next_mode) {
      // Add the mode and keep parsing.
      chain.push_back(next_mode);
    } else if (next_action) {
      if (show_help) {
        cerr << next_action->BuildHelp(chain) << endl;
        return 1;
      } else {
        // Invoke the action with whatever arguments remain, skipping this one.
        deque<string> remaining_args;
        for (int j = i + 1; j < argc; j++) {
          remaining_args.push_back(argv[j]);
        }
        return DispatchCommand(chain, next_action, remaining_args);
      }
    } else {
      // Couldn't match the argument at all. Print the help.
      Status s = Status::InvalidArgument(
          Substitute("unknown command '$0'\n", argv[i]));
      cerr << s.ToString() << cur->BuildHelp(chain) << endl;
      return 1;
    }
  }

  // Ran out of arguments before reaching an action. Print the last mode's help.
  DCHECK(!chain.empty());
  const Mode* last = chain.back();
  cerr << last->BuildHelp(chain) << endl;
  return 1;
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
  bool show_help = ParseCommandLineFlags(&argc, &argv);
  FLAGS_logtostderr = true;
  kudu::InitGoogleLoggingSafe(argv[0]);
  return kudu::tools::RunTool(argc, argv, show_help);
}
