// Copyright (c) 2014, Cloudera, inc.

#include "tools/fs_tool.h"

#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <iostream>
#include <vector>
#include <utility>

#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "gutil/strings/human_readable.h"
#include "util/status.h"
#include "util/env.h"
#include "util/logging.h"
#include "server/fsmanager.h"
#include "consensus/log_util.h"
#include "consensus/log_reader.h"
#include "tablet/tablet.h"

namespace kudu {
namespace tools {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;
using tablet::Tablet;
using metadata::TabletMasterBlockPB;
using metadata::TabletMetadata;
using metadata::RowSetMetadata;
using log::LogReader;
using log::ReadableLogSegment;

FsTool::FsTool(const string& base_dir, DetailLevel detail_level)
    : initialized_(false),
      base_dir_(base_dir),
      detail_level_(detail_level) {
}

FsTool::~FsTool() {
}

Status FsTool::Init() {
  CHECK(!initialized_) << "Already initialized";

  fs_manager_.reset(new FsManager(Env::Default(), base_dir_));

  if (!fs_manager_->Exists(base_dir_)) {
    return Status::NotFound("base directory does not exist", base_dir_);
  }
  RETURN_NOT_OK(fs_manager_->Open());

  LOG(INFO)
      << "Opened file system in " << base_dir_ << ", uuid: " << fs_manager_->uuid();

  initialized_ = true;
  return Status::OK();
}

Status FsTool::FsTree() {
  DCHECK(initialized_);

  fs_manager_->DumpFileSystemTree(std::cout);
  return Status::OK();
}

Status FsTool::ListAllLogSegments() {
  DCHECK(initialized_);

  string wals_dir = fs_manager_->GetWalsRootDir();
  if (!fs_manager_->Exists(wals_dir)) {
    return Status::Corruption(Substitute("root log directory '$0' does not exist under '$1'",
                                         wals_dir, base_dir_));
  }

  std::cout << "Root log directory: " << wals_dir << std::endl;

  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager_->ListDir(wals_dir, &children),
                        "Could not list log directories");
  BOOST_FOREACH(const string& child, children) {
    if (HasPrefixString(child, ".")) {
      // Hidden files or ./..
      VLOG(1) << "Ignoring hidden file in root log directory " << child;
      continue;
    }
    string path = JoinPathSegments(wals_dir, child);
    if (HasSuffixString(child, FsManager::kWalsRecoveryDirSuffix)) {
      std::cout << "Log recovery dir found: " << path << std::endl;
    } else {
      std::cout << "Log directory: " << path << std::endl;
    }
    RETURN_NOT_OK(ListSegmentsInDir(path));
  }
  return Status::OK();
}

Status FsTool::ListLogSegmentsForTablet(const string& tablet_id) {
  DCHECK(initialized_);

  string tablet_wal_dir = fs_manager_->GetTabletWalDir(tablet_id);
  if (!fs_manager_->Exists(tablet_wal_dir)) {
    return Status::NotFound(Substitute("tablet '$0' has no logs in base dir '$1'",
                                       tablet_id, base_dir_));
  }
  std::cout << "Tablet WAL dir found: " << tablet_wal_dir << std::endl;
  RETURN_NOT_OK(ListSegmentsInDir(tablet_wal_dir));
  string recovery_dir = fs_manager_->GetTabletWalRecoveryDir(tablet_id);
  if (fs_manager_->Exists(recovery_dir)) {
    std::cout << "Recovery dir found: " << recovery_dir << std::endl;
    RETURN_NOT_OK(ListSegmentsInDir(recovery_dir));
  }
  return Status::OK();
}

Status FsTool::GetTabletsInMasterBlockDir(vector<string>* tablets) {
  string master_block_dir = fs_manager_->GetMasterBlockDir();
  if (!fs_manager_->Exists(master_block_dir)) {
    return Status::NotFound(Substitute("no tablet master block (expected path: '$0') in dir '$1'",
                                       master_block_dir, base_dir_));
  }
  std::cout << "Tablets in master block directory " << master_block_dir << ":" << std::endl;
  vector<string> children;
  RETURN_NOT_OK_PREPEND(fs_manager_->ListDir(master_block_dir, &children),
                        "Couldn't list tablet master blocks");
  BOOST_FOREACH(const string& child, children) {
    if (!Tablet::IsTabletFileName(child)) {
      continue;
    }
    tablets->push_back(child);
  }
  return Status::OK();
}

Status FsTool::ListAllTablets() {
  DCHECK(initialized_);

  vector<string> tablets;
  RETURN_NOT_OK(GetTabletsInMasterBlockDir(&tablets));
  BOOST_FOREACH(const string& tablet, tablets) {
    if (detail_level_ >= HEADERS_ONLY) {
      std::cout << "Tablet: " << tablet << std::endl;
      string path = JoinPathSegments(fs_manager_->GetMasterBlockDir(), tablet);
      RETURN_NOT_OK(PrintTabletMeta(path, tablet));
    } else {
      std::cout << "\t" << tablet << std::endl;
    }
  }
  return Status::OK();
}

Status FsTool::ListSegmentsInDir(const string& segments_dir) {
  vector<string> segments;
  RETURN_NOT_OK_PREPEND(fs_manager_->ListDir(segments_dir, &segments),
                        "Unable to list log segments");
  std::cout << "Segments in " << segments_dir << ":" << std::endl;
  BOOST_FOREACH(const string& segment, segments) {
    if (!log::IsLogFileName(segment)) {
      continue;
    }
    if (detail_level_ >= HEADERS_ONLY) {
      std::cout << "Segment: " << segment << std::endl;
      string path = JoinPathSegments(segments_dir, segment);
      RETURN_NOT_OK(PrintLogSegmentHeader(path));
    } else {
      std::cout << "\t" << segment << std::endl;
    }
  }
  return Status::OK();
}

Status FsTool::PrintLogSegmentHeader(const string& path) {
  shared_ptr<ReadableLogSegment> segment;
  Status s = LogReader::InitSegment(fs_manager_->env(),
                                    path,
                                    &segment);

  if (s.IsUninitialized()) {
    LOG(ERROR) << path << " is not initialized: " << s.ToString();
    return Status::OK();
  }
  if (s.IsCorruption()) {
    LOG(ERROR) << path << " is corrupt: " << s.ToString();
    return Status::OK();
  }
  RETURN_NOT_OK_PREPEND(s, "Unexpected error reading log segment " + path);

  std::cout << "Size: "
            << HumanReadableNumBytes::ToStringWithoutRounding(segment->file_size())
            << std::endl;
  std::cout << "Header: " << std::endl;
  std::cout << segment->header().DebugString();
  return Status::OK();
}

Status FsTool::PrintTabletMeta(const string& master_block_path, const string& tablet_id) {
  gscoped_ptr<TabletMetadata> meta;
  RETURN_NOT_OK(LoadTabletMetadata(master_block_path, tablet_id, &meta));

  std::cout << "Start key: '" << meta->start_key() <<"' End key: '" << meta->end_key() << "'"
            << std::endl;
  std::cout << "Table name: " << meta->table_name()
            << " Table id: " << meta->table_id() << std::endl;
  std::cout << "Schema (version=" << meta->schema_version() << "): "
            << meta->schema().ToString() << std::endl;
  return Status::OK();
}

Status FsTool::LoadTabletMetadata(const string& master_block_path,
                                  const string& tablet_id,
                                  gscoped_ptr<metadata::TabletMetadata>* meta) {
  TabletMasterBlockPB master_block_pb;
  Status s = TabletMetadata::OpenMasterBlock(fs_manager_->env(),
                                             master_block_path,
                                             tablet_id,
                                             &master_block_pb);
  if (s.IsCorruption()) {
    LOG(ERROR) << "Master block at " << master_block_path << " is corrupt: " << s.ToString();
    return Status::OK();
  }
  RETURN_NOT_OK_PREPEND(
      s, Substitute("Unexpected error reading master block for tablet '$0' from '$1'",
                    tablet_id,
                    master_block_path));
  return TabletMetadata::Load(fs_manager_.get(), master_block_pb, meta);
}

Status FsTool::ListBlocksForAllTablets() {
  DCHECK(initialized_);

  vector<string> tablets;
  RETURN_NOT_OK(GetTabletsInMasterBlockDir(&tablets));
  BOOST_FOREACH(string tablet, tablets) {
    RETURN_NOT_OK(ListBlocksForTablet(tablet));
  }
  return Status::OK();
}

Status FsTool::ListBlocksForTablet(const string& tablet_id) {
  DCHECK(initialized_);

  string master_block_path = JoinPathSegments(fs_manager_->GetMasterBlockDir(),
                                              tablet_id);

  if (!fs_manager_->Exists(master_block_path)) {
    return Status::NotFound(Substitute("master block for tablet '$0' not found in '$1'",
                                       tablet_id, base_dir_));
  }

  std::cout << "Master block for tablet " << tablet_id << ": " << master_block_path << std::endl;

  gscoped_ptr<TabletMetadata> meta;
  RETURN_NOT_OK(LoadTabletMetadata(master_block_path, tablet_id, &meta));

  if (meta->rowsets().empty()) {
    std::cout << "No rowsets found on disk for tablet " << tablet_id << std::endl;
    return Status::OK();
  }

  std::cout << "Listing all data blocks in tablet " << tablet_id << ":" << std::endl;

  Schema schema = meta->schema();

  size_t idx = 0;
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rs_meta, meta->rowsets())  {
    std::cout << "Rowset " << idx++ << std::endl;
    RETURN_NOT_OK(ListBlocksInRowSet(schema, *rs_meta));
  }

  return Status::OK();
}

Status FsTool::ListBlocksInRowSet(const Schema& schema,
                                  const RowSetMetadata& rs_meta) {
  typedef std::pair<int64_t, BlockId> DeltaBlock;

  for (size_t col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {

    if (rs_meta.HasColumnDataBlockForTests(col_idx)) {
      std::cout << "Column block for column " << schema.column(col_idx).ToString() << ": ";
      std::cout << rs_meta.column_block(col_idx).ToString() << std::endl;
    } else {
      std::cout << "No column data blocks blocks for column "
                << schema.column(col_idx).ToString() << ". " << std::endl;
    }
  }

  for (size_t idx = 0; idx < rs_meta.undo_delta_blocks_count(); ++idx) {
    DeltaBlock block = rs_meta.undo_delta_block(idx);
    std::cout << "Undo delta block (delta id=" << block.first << "): " << block.second.ToString()
              << std::endl;
  }

  for (size_t idx = 0; idx < rs_meta.redo_delta_blocks_count(); ++idx) {
    DeltaBlock block = rs_meta.redo_delta_block(idx);
    std::cout << "Redo delta block (delta id=" << block.first << "): " << block.second.ToString()
              << std::endl;
  }

  return Status::OK();
}


} // namespace tools
} // namespace kudu
