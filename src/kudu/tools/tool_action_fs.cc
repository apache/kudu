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

#include <iostream>
#include <memory>
#include <string>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tools/tool_action_common.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/status.h"

DECLARE_bool(print_meta);
DEFINE_bool(print_rows, true,
            "Print each row in the CFile");
DEFINE_string(uuid, "",
              "The uuid to use in the filesystem. "
              "If not provided, one is generated");

namespace kudu {
namespace tools {

using cfile::CFileReader;
using cfile::CFileIterator;
using cfile::ReaderOptions;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace {

Status Format(const RunnerContext& context) {
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  boost::optional<string> uuid;
  if (!FLAGS_uuid.empty()) {
    uuid = FLAGS_uuid;
  }
  return fs_manager.CreateInitialFileSystemLayout(uuid);
}

Status DumpUuid(const RunnerContext& context) {
  FsManagerOpts opts;
  opts.read_only = true;
  FsManager fs_manager(Env::Default(), opts);
  RETURN_NOT_OK(fs_manager.Open());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

Status DumpCFile(const RunnerContext& context) {
  const string& block_id_str = FindOrDie(context.required_args, "block_id");
  uint64_t numeric_id;
  if (!safe_strtou64(block_id_str, &numeric_id)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as numeric block ID", block_id_str));
  }
  BlockId block_id(numeric_id);

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), fs_opts);
  RETURN_NOT_OK(fs_manager.Open());

  gscoped_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  gscoped_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  if (FLAGS_print_meta) {
    cout << "Header:\n" << SecureDebugString(reader->header()) << endl;
    cout << "Footer:\n" << SecureDebugString(reader->footer()) << endl;
  }

  if (FLAGS_print_rows) {
    gscoped_ptr<CFileIterator> it;
    RETURN_NOT_OK(reader->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK));
    RETURN_NOT_OK(it->SeekToFirst());

    RETURN_NOT_OK(DumpIterator(*reader, it.get(), &cout, 0, 0));
  }

  return Status::OK();
}

Status DumpFsTree(const RunnerContext& context) {
  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), fs_opts);
  RETURN_NOT_OK(fs_manager.Open());

  fs_manager.DumpFileSystemTree(std::cout);
  return Status::OK();
}

} // anonymous namespace

static unique_ptr<Mode> BuildFsDumpMode() {
  unique_ptr<Action> dump_cfile =
      ActionBuilder("cfile", &DumpCFile)
      .Description("Dump the contents of a CFile (column file)")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("print_rows")
      .Build();

  unique_ptr<Action> dump_tree =
      ActionBuilder("tree", &DumpFsTree)
      .Description("Dump the tree of a Kudu filesystem")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .Build();

  unique_ptr<Action> dump_uuid =
      ActionBuilder("uuid", &DumpUuid)
      .Description("Dump the UUID of a Kudu filesystem")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .Build();

  return ModeBuilder("dump")
      .Description("Dump a Kudu filesystem")
      .AddAction(std::move(dump_cfile))
      .AddAction(std::move(dump_tree))
      .AddAction(std::move(dump_uuid))
      .Build();
}

unique_ptr<Mode> BuildFsMode() {
  unique_ptr<Action> format =
      ActionBuilder("format", &Format)
      .Description("Format a new Kudu filesystem")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("uuid")
      .Build();

  return ModeBuilder("fs")
      .Description("Operate on a local Kudu filesystem")
      .AddMode(BuildFsDumpMode())
      .AddAction(std::move(format))
      .Build();
}

} // namespace tools
} // namespace kudu
