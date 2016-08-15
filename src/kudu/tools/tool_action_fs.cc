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

#include <boost/optional/optional.hpp>
#include <deque>
#include <gflags/gflags.h>
#include <iostream>
#include <memory>
#include <string>

#include "kudu/fs/fs_manager.h"
#include "kudu/util/status.h"

using std::cout;
using std::deque;
using std::endl;
using std::string;
using std::unique_ptr;
using std::vector;

DEFINE_string(uuid, "",
              "The uuid to use in the filesystem. If not provided, one is generated");

namespace kudu {
namespace tools {

namespace {

Status Format(const vector<Mode*>& chain,
              const Action* action,
              deque<string> args) {
  RETURN_NOT_OK(CheckNoMoreArgs(chain, action, args));

  FsManager fs_manager(Env::Default(), FsManagerOpts());
  boost::optional<string> uuid;
  if (!FLAGS_uuid.empty()) {
    uuid = FLAGS_uuid;
  }
  return fs_manager.CreateInitialFileSystemLayout(uuid);
}

Status PrintUuid(const vector<Mode*>& chain,
                 const Action* action,
                 deque<string> args) {
  RETURN_NOT_OK(CheckNoMoreArgs(chain, action, args));

  FsManagerOpts opts;
  opts.read_only = true;
  FsManager fs_manager(Env::Default(), opts);
  RETURN_NOT_OK(fs_manager.Open());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildFsMode() {
  unique_ptr<Action> format = ActionBuilder(
      { "format", "Format a new Kudu filesystem" }, &Format)
    .AddGflag("fs_wal_dir")
    .AddGflag("fs_data_dirs")
    .AddGflag("uuid")
    .Build();

  unique_ptr<Action> print_uuid = ActionBuilder(
      { "print_uuid", "Print the UUID of a Kudu filesystem" }, &PrintUuid)
    .AddGflag("fs_wal_dir")
    .AddGflag("fs_data_dirs")
    .Build();

  return ModeBuilder({ "fs", "Operate on a local Kudu filesystem" })
      .AddAction(std::move(format))
      .AddAction(std::move(print_uuid))
      .Build();
}

} // namespace tools
} // namespace kudu
