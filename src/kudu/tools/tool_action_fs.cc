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
#include <iostream>
#include <string>

#include "kudu/fs/fs_manager.h"
#include "kudu/util/status.h"

using std::cout;
using std::deque;
using std::endl;
using std::string;
using std::vector;

namespace kudu {
namespace tools {

namespace {

Status Format(const vector<Action>& chain, deque<string> args) {
  RETURN_NOT_OK(CheckNoMoreArgs(chain, args));

  FsManager fs_manager(Env::Default(), FsManagerOpts());
  return fs_manager.CreateInitialFileSystemLayout();
}

Status PrintUuid(const vector<Action>& chain, deque<string> args) {
  RETURN_NOT_OK(CheckNoMoreArgs(chain, args));

  FsManagerOpts opts;
  opts.read_only = true;
  FsManager fs_manager(Env::Default(), opts);
  RETURN_NOT_OK(fs_manager.Open());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

} // anonymous namespace

Action BuildFsAction() {
  Action fs_format;
  fs_format.name = "format";
  fs_format.description = "Format a new Kudu filesystem";
  fs_format.help = &BuildLeafActionHelpString;
  fs_format.run = &Format;

  Action fs_print_uuid;
  fs_print_uuid.name = "print_uuid";
  fs_print_uuid.description = "Print the UUID of a Kudu filesystem";
  fs_print_uuid.help = &BuildLeafActionHelpString;
  fs_print_uuid.run = &PrintUuid;

  Action fs;
  fs.name = "fs";
  fs.description = "Operate on a local Kudu filesystem";
  fs.help = &BuildNonLeafActionHelpString;
  fs.sub_actions = {
      fs_format,
      fs_print_uuid
  };
  return fs;
}

} // namespace tools
} // namespace kudu
