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

#include <gflags/gflags.h>
#include <iostream>
#include <memory>
#include <string>

#include "kudu/gutil/map-util.h"
#include "kudu/util/env.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;

DEFINE_bool(oneline, false, "print each protobuf on a single line");
TAG_FLAG(oneline, stable);

namespace kudu {
namespace tools {

namespace {

const char* const kPathArg = "path";

Status DumpPBContainerFile(const RunnerContext& context) {
  string path = FindOrDie(context.required_args, kPathArg);

  Env* env = Env::Default();
  gscoped_ptr<RandomAccessFile> reader;
  RETURN_NOT_OK(env->NewRandomAccessFile(path, &reader));
  pb_util::ReadablePBContainerFile pb_reader(std::move(reader));
  RETURN_NOT_OK(pb_reader.Open());
  RETURN_NOT_OK(pb_reader.Dump(&std::cout, FLAGS_oneline));

  return Status::OK();
}

} // anonymous namespace

unique_ptr<Mode> BuildPbcMode() {
  unique_ptr<Action> dump =
      ActionBuilder("dump", &DumpPBContainerFile)
      .Description("Dump a PBC (protobuf container) file")
      .AddOptionalParameter("oneline")
      .AddRequiredParameter({kPathArg, "path to PBC file"})
      .Build();

  return ModeBuilder("pbc")
      .Description("Operate on PBC (protobuf container) files")
      .AddAction(std::move(dump))
      .Build();
}

} // namespace tools
} // namespace kudu
