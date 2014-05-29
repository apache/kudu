// Copyright (c) 2014, Cloudera, inc.

#include "tools/fs_tool.h"

#include <boost/foreach.hpp>
#include <boost/function.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <tr1/memory>
#include <algorithm>
#include <iostream>
#include <vector>

#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "gutil/strings/human_readable.h"
#include "util/status.h"
#include "util/env.h"
#include "util/memory/arena.h"
#include "util/logging.h"
#include "server/fsmanager.h"
#include "common/rowblock.h"
#include "common/row_changelist.h"
#include "consensus/log_util.h"
#include "consensus/log_reader.h"
#include "cfile/cfile_reader.h"
#include "tablet/cfile_set.h"
#include "tablet/deltafile.h"
#include "tablet/tablet.h"

namespace kudu {
namespace tools {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::DumpIterator;
using cfile::DumpIteratorOptions;
using cfile::ReaderOptions;
using strings::Substitute;
using tablet::DeltaFileReader;
using tablet::DeltaIterator;
using tablet::DeltaKeyAndUpdate;
using tablet::DeltaType;
using tablet::MvccSnapshot;
using tablet::Tablet;
using tablet::CFileSet;
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
      RETURN_NOT_OK(PrintTabletMetaInternal(path, tablet));
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
  scoped_refptr<ReadableLogSegment> segment;
  Status s = ReadableLogSegment::Open(fs_manager_->env(),
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

Status FsTool::PrintTabletMeta(const string& tablet_id) {
  string master_block_path;
  RETURN_NOT_OK(GetMasterBlockPath(tablet_id, &master_block_path));
  return PrintTabletMetaInternal(master_block_path, tablet_id);
}

Status FsTool::PrintTabletMetaInternal(const string& master_block_path,
                                       const string& tablet_id) {
  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(LoadTabletMetadata(master_block_path, tablet_id, &meta));

  const Schema& schema = meta->schema();

  std::cout << "Start key: " << schema.DebugEncodedRowKey(meta->start_key())
            <<" End key: " << schema.DebugEncodedRowKey(meta->end_key()) << std::endl;
  std::cout << "Table name: " << meta->table_name()
            << " Table id: " << meta->table_id() << std::endl;
  std::cout << "Schema (version=" << meta->schema_version() << "): "
            << schema.ToString() << std::endl;
  return Status::OK();
}

Status FsTool::LoadTabletMetadata(const string& master_block_path,
                                  const string& tablet_id,
                                  scoped_refptr<metadata::TabletMetadata>* meta) {
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
  VLOG(1) << "Loaded master block: " << master_block_pb.ShortDebugString();
  RETURN_NOT_OK(TabletMetadata::Load(fs_manager_.get(), master_block_pb, meta));
  return Status::OK();
}

Status FsTool::GetMasterBlockPath(const string& tablet_id,
                                  string* master_block_path) {
  string path = fs_manager_->GetMasterBlockPath(tablet_id);
  if (!fs_manager_->Exists(path)) {
    return Status::NotFound(Substitute("master block for tablet '$0' not found in '$1'",
                                       tablet_id, base_dir_));
  }

  *master_block_path = path;
  return Status::OK();
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

  string master_block_path;
  RETURN_NOT_OK(GetMasterBlockPath(tablet_id, &master_block_path));

  std::cout << "Master block for tablet " << tablet_id << ": " << master_block_path << std::endl;

  scoped_refptr<TabletMetadata> meta;
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


  for (size_t col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {

    if (rs_meta.HasColumnDataBlockForTests(col_idx)) {
      std::cout << "Column block for column " << schema.column(col_idx).ToString() << ": ";
      std::cout << rs_meta.column_block(col_idx).ToString() << std::endl;
    } else {
      std::cout << "No column data blocks for column "
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

Status FsTool::DumpTablet(const std::string& tablet_id,
                          const DumpOptions& opts) {
  DCHECK(initialized_);

  string master_block_path;
  RETURN_NOT_OK(GetMasterBlockPath(tablet_id, &master_block_path));

  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(LoadTabletMetadata(master_block_path, tablet_id, &meta));

  if (meta->rowsets().empty()) {
    std::cout << "No rowsets found on disk for tablet " << tablet_id << std::endl;
    return Status::OK();
  }

  Schema schema = meta->schema();

  size_t idx = 0;
  BOOST_FOREACH(const shared_ptr<RowSetMetadata>& rs_meta, meta->rowsets())  {
    std::cout << "Dumping rowset " << idx++ << std::endl;
    RETURN_NOT_OK(DumpRowSetInternal(meta->schema(), rs_meta, opts));
  }

  return Status::OK();
}

Status FsTool::DumpRowSet(const string& tablet_id,
                          size_t rowset_idx,
                          const DumpOptions& opts) {
  DCHECK(initialized_);

  string master_block_path;
  RETURN_NOT_OK(GetMasterBlockPath(tablet_id, &master_block_path));

  scoped_refptr<TabletMetadata> meta;
  RETURN_NOT_OK(LoadTabletMetadata(master_block_path, tablet_id, &meta));

  if (rowset_idx >= meta->rowsets().size()) {
    return Status::InvalidArgument(
        Substitute("index '$0' out of bounds for tablet '$1', number of rowsets=$2",
                   rowset_idx, tablet_id, meta->rowsets().size()));
  }

  return DumpRowSetInternal(meta->schema(), meta->rowsets()[rowset_idx], opts);
}

Status FsTool::DumpRowSetInternal(const Schema& schema,
                                  const shared_ptr<RowSetMetadata>& rs_meta,
                                  const DumpOptions& opts) {

  std::cout << "RowSet metadata: " << rs_meta->ToString() << std::endl;

  for (size_t col_idx = 0; col_idx < schema.num_columns(); ++col_idx) {
    if (rs_meta->HasColumnDataBlockForTests(col_idx)) {
      std::cout << "Dumping column block for column " << schema.column(col_idx).ToString() << ": ";
      std::cout << std::endl;
      RETURN_NOT_OK(DumpCFileBlockInternal(rs_meta->column_block(col_idx), opts));
    } else {
      std::cout << "No column data blocks for column "
                << schema.column(col_idx).ToString() << ". " << std::endl;
    }
  }

  for (size_t idx = 0; idx < rs_meta->undo_delta_blocks_count(); ++idx) {
    DeltaBlock block = rs_meta->undo_delta_block(idx);
    std::cout << "Dumping undo delta block delta id = " << block.first << ": " << std::endl;
    RETURN_NOT_OK(DumpDeltaCFileBlockInternal(schema,
                                              rs_meta,
                                              block.second,
                                              block.first,
                                              tablet::UNDO,
                                              opts));
  }

  for (size_t idx = 0; idx < rs_meta->redo_delta_blocks_count(); ++idx) {
    DeltaBlock block = rs_meta->redo_delta_block(idx);
    std::cout << "Redo delta block delta id = " << block.first << ": " << std::endl;
     RETURN_NOT_OK(DumpDeltaCFileBlockInternal(schema,
                                               rs_meta,
                                               block.second,
                                               block.first,
                                               tablet::REDO,
                                               opts));
  }

  return Status::OK();
}

Status FsTool::DumpCFileBlock(const std::string& block_id_str, const DumpOptions &opts) {
  BlockId block_id(block_id_str);
  if (!fs_manager_->BlockExists(block_id)) {
    return Status::NotFound(Substitute("block '$0' does not exist under '$1'",
                                       block_id_str, base_dir_));
  }
  return DumpCFileBlockInternal(block_id, opts);
}

Status FsTool::DumpCFileBlockInternal(const BlockId& block_id, const DumpOptions& opts) {
  shared_ptr<RandomAccessFile> block_reader;
  uint64_t file_size;
  RETURN_NOT_OK(OpenBlockAsFile(block_id, &file_size, &block_reader));
  gscoped_ptr<CFileReader> reader;
  RETURN_NOT_OK(CFileReader::Open(block_reader, file_size, ReaderOptions(), &reader));

  std::cout << "CFile Header: " << std::endl << reader->header().ShortDebugString() << std::endl;
  std::cout << "Number of values: " << reader->footer().num_values() << std::endl;

  gscoped_ptr<CFileIterator> it;
  RETURN_NOT_OK(reader->NewIterator(&it));
  RETURN_NOT_OK(it->SeekToFirst());
  DumpIteratorOptions iter_opts;
  iter_opts.nrows = opts.nrows;
  iter_opts.print_rows = detail_level_ > HEADERS_ONLY;
  return DumpIterator(*reader, it.get(), &std::cout, iter_opts);
}

Status FsTool::DumpDeltaCFileBlockInternal(const Schema& schema,
                                           const shared_ptr<RowSetMetadata>& rs_meta,
                                           const BlockId& block_id,
                                           int64_t delta_id,
                                           DeltaType delta_type,
                                           const DumpOptions& opts) {
  // Open the delta reader
  shared_ptr<RandomAccessFile> block_reader;
  uint64_t file_size;
  RETURN_NOT_OK(OpenBlockAsFile(block_id, &file_size, &block_reader));
  string path = fs_manager_->GetBlockPath(block_id);
  shared_ptr<DeltaFileReader> delta_reader;
  RETURN_NOT_OK(DeltaFileReader::Open(path,
                                      block_reader,
                                      file_size,
                                      delta_id,
                                      &delta_reader,
                                      delta_type));
  // Create the delta iterator.
  // TODO: see if it's worth re-factoring NewDeltaIterator to return a
  // gscoped_ptr that can then be released if we need a raw or shared
  // pointer.
  DeltaIterator* raw_iter;

  MvccSnapshot snap_all;
  if (delta_type == tablet::REDO) {
    snap_all = MvccSnapshot::CreateSnapshotIncludingAllTransactions();
  } else if (delta_type == tablet::UNDO) {
    snap_all = MvccSnapshot::CreateSnapshotIncludingNoTransactions();
  }

  Status s = delta_reader->NewDeltaIterator(&schema, snap_all, &raw_iter);

  if (s.IsNotFound()) {
    std::cout << "Empty delta block." << std::endl;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  // NewDeltaIterator returns Status::OK() iff a new DeltaIterator is created. Thus,
  // it's safe to have a gscoped_ptr take possesion of 'raw_iter' here.
  gscoped_ptr<DeltaIterator> delta_iter(raw_iter);
  RETURN_NOT_OK(delta_iter->Init());
  RETURN_NOT_OK(delta_iter->SeekToOrdinal(0));

  // TODO: it's awkward that whenever we want to iterate over deltas we also
  // need to open the CFileSet for the rowset. Ideally, we should use information stored
  // in the footer/store additional information in the footer as to make it feasible
  // iterate over all deltas using a DeltaFileIterator alone.
  shared_ptr<CFileSet> cfileset(new CFileSet(rs_meta));
  RETURN_NOT_OK(cfileset->Open());
  gscoped_ptr<CFileSet::Iterator> cfileset_iter(cfileset->NewIterator(&schema));

  RETURN_NOT_OK(cfileset_iter->Init(NULL));

  const size_t kRowsPerBlock  = 100;
  size_t nrows = 0;
  size_t ndeltas = 0;
  Arena arena(32 * 1024, 128 * 1024);
  RowBlock block(schema, kRowsPerBlock, &arena);

  // See tablet/delta_compaction.cc to understand why this loop is structured the way
  // it is.
  while (cfileset_iter->HasNext()) {
    size_t n;
    if (opts.nrows > 0) {
      // Note: number of deltas may not equal the number of rows, but
      // since this is a CLI tool (and the nrows option exists
      // primarily to limit copious output) it's okay not to be
      // exact here.
      size_t remaining = opts.nrows - nrows;
      if (remaining == 0) break;
      n = std::min(remaining, kRowsPerBlock);
    } else {
      n = kRowsPerBlock;
    }

    arena.Reset();
    cfileset_iter->PrepareBatch(&n);

    block.Resize(n);

    RETURN_NOT_OK(delta_iter->PrepareBatch(n));
    vector<DeltaKeyAndUpdate> out;
    RETURN_NOT_OK(delta_iter->FilterColumnsAndAppend(vector<size_t>(),
                                                     &out,
                                                     &arena));
    BOOST_FOREACH(const DeltaKeyAndUpdate& upd, out) {
      if (detail_level_ > HEADERS_ONLY) {
        std::cout << upd.key.ToString() << " "
                  << RowChangeList(upd.cell).ToString(schema) << std::endl;
        ++ndeltas;
      }
    }
    RETURN_NOT_OK(cfileset_iter->FinishBatch());

    nrows += n;
  }

  LOG(INFO) << "Processed " << ndeltas << " deltas, for total of " << nrows << " possible rows.";
  return Status::OK();
}

Status FsTool::OpenBlockAsFile(const BlockId& block_id,
                               uint64_t* file_size,
                               shared_ptr<RandomAccessFile>* block_reader) {
  RETURN_NOT_OK_PREPEND(fs_manager_->OpenBlock(block_id, block_reader),
                        Substitute("Failed to open block $0", block_id.ToString()));
  RETURN_NOT_OK_PREPEND((*block_reader)->Size(file_size),
                        Substitute("Failed to determine the size of block $0",
                                   block_id.ToString()));
  return Status::OK();
}

} // namespace tools
} // namespace kudu
