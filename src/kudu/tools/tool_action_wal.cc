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

#include "kudu/consensus/log_util.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/env.h"
#include "kudu/util/status.h"

namespace kudu {
namespace tools {

using log::ReadableLogSegment;
using std::string;
using std::unique_ptr;

namespace {

const char* const kPathArg = "path";

Status Dump(const RunnerContext& context) {
  string segment_path = FindOrDie(context.required_args, kPathArg);

  scoped_refptr<ReadableLogSegment> segment;
  RETURN_NOT_OK(ReadableLogSegment::Open(Env::Default(), segment_path, &segment));
  RETURN_NOT_OK(PrintSegment(segment));
  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildWalMode() {
  unique_ptr<Action> dump =
      ActionBuilder("dump", &Dump)
      .Description("Dump a WAL (write-ahead log) file")
      .AddRequiredParameter({ kPathArg, "path to WAL file" })
      .AddOptionalParameter("print_entries")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("truncate_data")
      .Build();

  return ModeBuilder("wal")
      .Description("Operate on WAL (write-ahead log) files")
      .AddAction(std::move(dump))
      .Build();
}

} // namespace tools
} // namespace kudu
