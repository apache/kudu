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
#include <utility>

#include <gflags/gflags.h>

#include "kudu/gutil/map-util.h"
#include "kudu/master/master.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

using std::string;
using std::unique_ptr;

namespace {

const char* const kMasterAddressArg = "master_address";
const char* const kMasterAddressDesc = "Address of a Kudu Master of form "
    "'hostname:port'. Port may be omitted if the Master is bound to the "
    "default port.";
const char* const kFlagArg = "flag";
const char* const kValueArg = "value";

Status MasterSetFlag(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  const string& flag = FindOrDie(context.required_args, kFlagArg);
  const string& value = FindOrDie(context.required_args, kValueArg);
  return SetServerFlag(address, master::Master::kDefaultPort, flag, value);
}

Status MasterStatus(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerStatus(address, master::Master::kDefaultPort);
}

Status MasterTimestamp(const RunnerContext& context) {
  const string& address = FindOrDie(context.required_args, kMasterAddressArg);
  return PrintServerTimestamp(address, master::Master::kDefaultPort);
}

} // anonymous namespace

unique_ptr<Mode> BuildMasterMode() {
  unique_ptr<Action> set_flag =
      ActionBuilder("set_flag", &MasterSetFlag)
      .Description("Change a gflag value on a Kudu Master")
      .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
      .AddRequiredParameter({ kFlagArg, "Name of the gflag" })
      .AddRequiredParameter({ kValueArg, "New value for the gflag" })
      .AddOptionalParameter("force")
      .Build();

  unique_ptr<Action> status =
      ActionBuilder("status", &MasterStatus)
      .Description("Get the status of a Kudu Master")
      .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
      .Build();

  unique_ptr<Action> timestamp =
      ActionBuilder("timestamp", &MasterTimestamp)
      .Description("Get the current timestamp of a Kudu Master")
      .AddRequiredParameter({ kMasterAddressArg, kMasterAddressDesc })
      .Build();

  return ModeBuilder("master")
      .Description("Operate on a Kudu Master")
      .AddAction(std::move(set_flag))
      .AddAction(std::move(status))
      .AddAction(std::move(timestamp))
      .Build();
}

} // namespace tools
} // namespace kudu

