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

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/cfile/cfile.pb.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/fs/fs_report.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/numbers.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DECLARE_bool(print_meta);
DEFINE_bool(print_rows, true,
            "Print each row in the CFile");
DEFINE_string(uuid, "",
              "The uuid to use in the filesystem. "
              "If not provided, one is generated");
DEFINE_bool(repair, false,
            "Repair any inconsistencies in the filesystem.");

namespace kudu {
namespace tools {

using cfile::CFileReader;
using cfile::CFileIterator;
using cfile::ReaderOptions;
using fs::FsReport;
using fs::ReadableBlock;
using std::cout;
using std::endl;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;
using tablet::TabletMetadata;

namespace {

Status Check(const RunnerContext& /*context*/) {
  FsManagerOpts opts;
  opts.read_only = !FLAGS_repair;
  FsManager fs_manager(Env::Default(), opts);
  FsReport report;
  RETURN_NOT_OK(fs_manager.Open(&report));

  // Stop now if we've already found a fatal error. Otherwise, continue;
  // we'll modify the report with our own check results and print it fully
  // at the end.
  if (report.HasFatalErrors()) {
    RETURN_NOT_OK(report.PrintAndCheckForFatalErrors());
  }

  // Get the "live" block IDs (i.e. those referenced by a tablet).
  vector<BlockId> live_block_ids;
  unordered_map<BlockId, string, BlockIdHash, BlockIdEqual> live_block_id_to_tablet;
  vector<string> tablet_ids;
  RETURN_NOT_OK(fs_manager.ListTabletIds(&tablet_ids));
  for (const auto& t : tablet_ids) {
    scoped_refptr<TabletMetadata> meta;
    RETURN_NOT_OK(TabletMetadata::Load(&fs_manager, t, &meta));
    vector<BlockId> tablet_live_block_ids = meta->CollectBlockIds();
    live_block_ids.insert(live_block_ids.end(),
                          tablet_live_block_ids.begin(),
                          tablet_live_block_ids.end());
    for (const auto& id : tablet_live_block_ids) {
      InsertOrDie(&live_block_id_to_tablet, id, t);
    }
  }

  // Get all of the block IDs reachable by the block manager.
  vector<BlockId> all_block_ids;
  RETURN_NOT_OK(fs_manager.block_manager()->GetAllBlockIds(&all_block_ids));

  std::sort(live_block_ids.begin(), live_block_ids.end(), BlockIdCompare());
  std::sort(all_block_ids.begin(), all_block_ids.end(), BlockIdCompare());

  // Blocks found in the block manager but not in a tablet. They are orphaned
  // and can be safely deleted.
  vector<BlockId> orphaned_block_ids;
  std::set_difference(all_block_ids.begin(), all_block_ids.end(),
                      live_block_ids.begin(), live_block_ids.end(),
                      std::back_inserter(orphaned_block_ids), BlockIdCompare());

  // Blocks found in a tablet but not in the block manager. They are missing
  // and indicative of corruption in the associated tablet(s).
  vector<BlockId> missing_block_ids;
  std::set_difference(live_block_ids.begin(), live_block_ids.end(),
                      all_block_ids.begin(), all_block_ids.end(),
                      std::back_inserter(missing_block_ids), BlockIdCompare());

  // Add missing blocks to the report.
  report.missing_block_check.emplace();
  for (const auto& id : missing_block_ids) {
    report.missing_block_check->entries.emplace_back(
        id, FindOrDie(live_block_id_to_tablet, id));
  }

  // Add orphaned blocks to the report after attempting to repair them.
  report.orphaned_block_check.emplace();
  for (const auto& id : orphaned_block_ids) {
    // Opening a block isn't free, but the number of orphaned blocks shouldn't
    // be extraordinarily high.
    uint64_t size;
    {
      unique_ptr<ReadableBlock> block;
      RETURN_NOT_OK(fs_manager.OpenBlock(id, &block));
      RETURN_NOT_OK(block->Size(&size));
    }
    fs::OrphanedBlockCheck::Entry entry(id, size);

    if (FLAGS_repair) {
      Status s = fs_manager.DeleteBlock(id);
      WARN_NOT_OK(s, "Could not delete orphaned block");
      if (s.ok()) {
        entry.repaired = true;
      }
    }
    report.orphaned_block_check->entries.emplace_back(entry);
  }
  return report.PrintAndCheckForFatalErrors();
}

Status Format(const RunnerContext& /*context*/) {
  FsManager fs_manager(Env::Default(), FsManagerOpts());
  boost::optional<string> uuid;
  if (!FLAGS_uuid.empty()) {
    uuid = FLAGS_uuid;
  }
  return fs_manager.CreateInitialFileSystemLayout(uuid);
}

Status DumpUuid(const RunnerContext& /*context*/) {
  FsManagerOpts opts;
  opts.read_only = true;
  FsManager fs_manager(Env::Default(), opts);
  RETURN_NOT_OK(fs_manager.Open());
  cout << fs_manager.uuid() << endl;
  return Status::OK();
}

Status ParseBlockIdArg(const RunnerContext& context,
                       BlockId* id) {
  const string& block_id_str = FindOrDie(context.required_args, "block_id");
  uint64_t numeric_id;
  if (!safe_strtou64(block_id_str, &numeric_id)) {
    return Status::InvalidArgument(Substitute(
        "Could not parse $0 as numeric block ID", block_id_str));
  }
  *id = BlockId(numeric_id);
  return Status::OK();
}

Status DumpCFile(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), fs_opts);
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  unique_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(std::move(block), ReaderOptions(), &reader));

  if (FLAGS_print_meta) {
    cout << "Header:\n" << pb_util::SecureDebugString(reader->header()) << endl;
    cout << "Footer:\n" << pb_util::SecureDebugString(reader->footer()) << endl;
  }

  if (FLAGS_print_rows) {
    gscoped_ptr<CFileIterator> it;
    RETURN_NOT_OK(reader->NewIterator(&it, CFileReader::DONT_CACHE_BLOCK));
    RETURN_NOT_OK(it->SeekToFirst());

    RETURN_NOT_OK(DumpIterator(*reader, it.get(), &cout, 0, 0));
  }

  return Status::OK();
}

Status DumpBlock(const RunnerContext& context) {
  BlockId block_id;
  RETURN_NOT_OK(ParseBlockIdArg(context, &block_id));

  FsManagerOpts fs_opts;
  fs_opts.read_only = true;
  FsManager fs_manager(Env::Default(), fs_opts);
  RETURN_NOT_OK(fs_manager.Open());

  unique_ptr<fs::ReadableBlock> block;
  RETURN_NOT_OK(fs_manager.OpenBlock(block_id, &block));

  uint64_t size = 0;
  RETURN_NOT_OK_PREPEND(block->Size(&size), "couldn't get block size");

  faststring buf;
  uint64_t offset = 0;
  while (offset < size) {
    int64_t chunk = std::min<int64_t>(size - offset, 64 * 1024);
    buf.resize(chunk);
    Slice s(buf);
    RETURN_NOT_OK(block->Read(offset, &s));
    offset += s.size();
    cout << s.ToString();
  }

  return Status::OK();
}

Status DumpFsTree(const RunnerContext& /*context*/) {
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
      .ExtraDescription("This interprets the contents of a CFile-formatted block "
                        "and outputs the decoded row data.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("print_meta")
      .AddOptionalParameter("print_rows")
      .Build();

  unique_ptr<Action> dump_block =
      ActionBuilder("block", &DumpBlock)
      .Description("Dump the binary contents of a data block")
      .ExtraDescription("This performs no parsing or interpretation of the data stored "
                        "in the block but rather outputs its binary contents directly.")
      .AddRequiredParameter({ "block_id", "block identifier" })
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
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
      .AddAction(std::move(dump_block))
      .AddAction(std::move(dump_cfile))
      .AddAction(std::move(dump_tree))
      .AddAction(std::move(dump_uuid))
      .Build();
}

unique_ptr<Mode> BuildFsMode() {
  unique_ptr<Action> check =
      ActionBuilder("check", &Check)
      .Description("Check a Kudu filesystem for inconsistencies")
      .AddOptionalParameter("fs_wal_dir")
      .AddOptionalParameter("fs_data_dirs")
      .AddOptionalParameter("repair")
      .Build();

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
      .AddAction(std::move(check))
      .AddAction(std::move(format))
      .Build();
}

} // namespace tools
} // namespace kudu
