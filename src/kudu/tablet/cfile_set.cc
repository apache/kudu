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
#include "kudu/tablet/cfile_set.h"

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <boost/container/flat_map.hpp>
#include <boost/container/vector.hpp>
#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/common/column_materialization_context.h"
#include "kudu/common/columnblock.h"
#include "kudu/common/encoded_key.h"
#include "kudu/common/iterator_stats.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/rowid.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/diskrowset.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/rowset_metadata.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

DEFINE_bool(consult_bloom_filters, true, "Whether to consult bloom filters on row presence checks");
TAG_FLAG(consult_bloom_filters, hidden);

DECLARE_bool(rowset_metadata_store_keys);

namespace kudu {

class MemTracker;

namespace tablet {

using cfile::BloomFileReader;
using cfile::CFileIterator;
using cfile::CFileReader;
using cfile::ColumnIterator;
using cfile::ReaderOptions;
using cfile::DefaultColumnValueIterator;
using fs::IOContext;
using fs::ReadableBlock;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

////////////////////////////////////////////////////////////
// Utilities
////////////////////////////////////////////////////////////

static Status OpenReader(FsManager* fs,
                         shared_ptr<MemTracker> cfile_reader_tracker,
                         const BlockId& block_id,
                         const IOContext* io_context,
                         unique_ptr<CFileReader>* new_reader) {
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs->OpenBlock(block_id, &block));

  ReaderOptions opts;
  opts.parent_mem_tracker = std::move(cfile_reader_tracker);
  opts.io_context = io_context;
  return CFileReader::OpenNoInit(std::move(block),
                                 std::move(opts),
                                 new_reader);
}

////////////////////////////////////////////////////////////
// CFile Base
////////////////////////////////////////////////////////////

CFileSet::CFileSet(shared_ptr<RowSetMetadata> rowset_metadata,
                   shared_ptr<MemTracker> bloomfile_tracker,
                   shared_ptr<MemTracker> cfile_reader_tracker)
    : rowset_metadata_(std::move(rowset_metadata)),
      bloomfile_tracker_(std::move(bloomfile_tracker)),
      cfile_reader_tracker_(std::move(cfile_reader_tracker)) {
}

CFileSet::~CFileSet() {
}

Status CFileSet::Open(shared_ptr<RowSetMetadata> rowset_metadata,
                      shared_ptr<MemTracker> bloomfile_tracker,
                      shared_ptr<MemTracker> cfile_reader_tracker,
                      const IOContext* io_context,
                      shared_ptr<CFileSet>* cfile_set) {
  shared_ptr<CFileSet> cfs(new CFileSet(std::move(rowset_metadata),
                                        std::move(bloomfile_tracker),
                                        std::move(cfile_reader_tracker)));
  RETURN_NOT_OK(cfs->DoOpen(io_context));

  cfile_set->swap(cfs);
  return Status::OK();
}

Status CFileSet::DoOpen(const IOContext* io_context) {
  RETURN_NOT_OK(OpenBloomReader(io_context));

  // Lazily open the column data cfiles. Each one will be fully opened
  // later, when the first iterator seeks for the first time.
  RowSetMetadata::ColumnIdToBlockIdMap block_map = rowset_metadata_->GetColumnBlocksById();
  for (const RowSetMetadata::ColumnIdToBlockIdMap::value_type& e : block_map) {
    ColumnId col_id = e.first;
    DCHECK(!ContainsKey(readers_by_col_id_, col_id)) << "already open";

    unique_ptr<CFileReader> reader;
    RETURN_NOT_OK(OpenReader(rowset_metadata_->fs_manager(),
                             cfile_reader_tracker_,
                             rowset_metadata_->column_data_block_for_col_id(col_id),
                             io_context,
                             &reader));
    readers_by_col_id_[col_id] = std::move(reader);
    VLOG(1) << "Successfully opened cfile for column id " << col_id
            << " in " << rowset_metadata_->ToString();
  }
  readers_by_col_id_.shrink_to_fit();

  if (rowset_metadata_->has_adhoc_index_block()) {
    RETURN_NOT_OK(OpenReader(rowset_metadata_->fs_manager(),
                             cfile_reader_tracker_,
                             rowset_metadata_->adhoc_index_block(),
                             io_context,
                             &ad_hoc_idx_reader_));
  }

  // If the user specified to store the min/max keys in the rowset metadata,
  // fetch them. Otherwise, load the min and max keys from the key reader.
  if (FLAGS_rowset_metadata_store_keys && rowset_metadata_->has_encoded_keys()) {
    min_encoded_key_ = rowset_metadata_->min_encoded_key();
    max_encoded_key_ = rowset_metadata_->max_encoded_key();
  } else {
    RETURN_NOT_OK(LoadMinMaxKeys(io_context));
  }
  // Verify the loaded keys are valid.
  if (Slice(min_encoded_key_).compare(max_encoded_key_) > 0) {
    return Status::Corruption(Substitute("Min key $0 > max key $1",
                                         KUDU_REDACT(Slice(min_encoded_key_).ToDebugString()),
                                         KUDU_REDACT(Slice(max_encoded_key_).ToDebugString())),
                              ToString());
  }

  return Status::OK();
}

Status CFileSet::OpenBloomReader(const IOContext* io_context) {
  FsManager* fs = rowset_metadata_->fs_manager();
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs->OpenBlock(rowset_metadata_->bloom_block(), &block));

  ReaderOptions opts;
  opts.io_context = io_context;
  opts.parent_mem_tracker = bloomfile_tracker_;
  Status s = BloomFileReader::OpenNoInit(std::move(block),
                                         std::move(opts),
                                         &bloom_reader_);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to open bloom file in " << rowset_metadata_->ToString() << ": "
                 << s.ToString();
    // Continue without bloom.
  }

  return Status::OK();
}

Status CFileSet::LoadMinMaxKeys(const IOContext* io_context) {
  CFileReader* key_reader = key_index_reader();
  RETURN_NOT_OK(key_index_reader()->Init(io_context));
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMinKeyMetaEntryName, &min_encoded_key_)) {
    return Status::Corruption("No min key found", ToString());
  }
  if (!key_reader->GetMetadataEntry(DiskRowSet::kMaxKeyMetaEntryName, &max_encoded_key_)) {
    return Status::Corruption("No max key found", ToString());
  }
  return Status::OK();
}

CFileReader* CFileSet::key_index_reader() const {
  if (ad_hoc_idx_reader_) {
    return ad_hoc_idx_reader_.get();
  }
  // If there is no special index cfile, then we have a non-compound key
  // and we can just use the key column.
  // This is always the first column listed in the tablet schema.
  int key_col_id = tablet_schema().column_id(0);
  return FindOrDie(readers_by_col_id_, key_col_id).get();
}

Status CFileSet::NewColumnIterator(ColumnId col_id,
                                   CFileReader::CacheControl cache_blocks,
                                   const fs::IOContext* io_context,
                                   unique_ptr<CFileIterator>* iter) const {
  return FindOrDie(readers_by_col_id_, col_id)->NewIterator(iter, cache_blocks,
                                                            io_context);
}

unique_ptr<CFileSet::Iterator> CFileSet::NewIterator(
    const Schema* projection,
    const IOContext* io_context) const {
  return unique_ptr<CFileSet::Iterator>(
      new CFileSet::Iterator(shared_from_this(), projection, io_context));
}

Status CFileSet::CountRows(const IOContext* io_context, rowid_t *count) const {
  RETURN_NOT_OK(key_index_reader()->Init(io_context));
  return key_index_reader()->CountRows(count);
}

Status CFileSet::GetBounds(string* min_encoded_key,
                           string* max_encoded_key) const {
  *min_encoded_key = min_encoded_key_;
  *max_encoded_key = max_encoded_key_;
  return Status::OK();
}

uint64_t CFileSet::AdhocIndexOnDiskSize() const {
  if (ad_hoc_idx_reader_) {
    return ad_hoc_idx_reader_->file_size();
  }
  return 0;
}

uint64_t CFileSet::BloomFileOnDiskSize() const {
  return bloom_reader_->FileSize();
}

uint64_t CFileSet::OnDiskDataSize() const {
  uint64_t ret = 0;
  for (const auto& e : readers_by_col_id_) {
    ret += e.second->file_size();
  }
  return ret;
}

uint64_t CFileSet::OnDiskColumnDataSize(const ColumnId& col_id) const {
  return FindOrDie(readers_by_col_id_, col_id)->file_size();
}

Status CFileSet::FindRow(const RowSetKeyProbe &probe,
                         const IOContext* io_context,
                         boost::optional<rowid_t>* idx,
                         ProbeStats* stats) const {
  if (FLAGS_consult_bloom_filters) {
    // Fully open the BloomFileReader if it was lazily opened earlier.
    //
    // If it's already initialized, this is a no-op.
    RETURN_NOT_OK(bloom_reader_->Init(io_context));

    stats->blooms_consulted++;
    bool present;
    Status s = bloom_reader_->CheckKeyPresent(probe.bloom_probe(), io_context, &present);
    if (s.ok() && !present) {
      *idx = boost::none;
      return Status::OK();
    }
    if (!s.ok()) {
      KLOG_EVERY_N_SECS(WARNING, 1) << Substitute("Unable to query bloom in $0: $1",
          rowset_metadata_->bloom_block().ToString(), s.ToString());
      if (PREDICT_FALSE(s.IsDiskFailure())) {
        // If the bloom lookup failed because of a disk failure, return early
        // since I/O to the tablet should be stopped.
        return s;
      }
      // Continue with the slow path
    }
  }

  stats->keys_consulted++;
  unique_ptr<CFileIterator> key_iter;
  RETURN_NOT_OK(NewKeyIterator(io_context, &key_iter));

  bool exact;
  Status s = key_iter->SeekAtOrAfter(probe.encoded_key(), &exact);
  if (s.IsNotFound() || (s.ok() && !exact)) {
    *idx = boost::none;
    return Status::OK();
  }
  RETURN_NOT_OK(s);

  *idx = key_iter->GetCurrentOrdinal();
  return Status::OK();
}

Status CFileSet::CheckRowPresent(const RowSetKeyProbe& probe, const IOContext* io_context,
                                 bool* present, rowid_t* rowid, ProbeStats* stats) const {
  boost::optional<rowid_t> opt_rowid;
  RETURN_NOT_OK(FindRow(probe, io_context, &opt_rowid, stats));
  *present = opt_rowid != boost::none;
  if (*present) {
  // Suppress false positive about 'opt_rowid' used when uninitialized.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    *rowid = *opt_rowid;
#pragma GCC diagnostic pop
  }
  return Status::OK();
}

Status CFileSet::NewKeyIterator(const IOContext* io_context,
                                unique_ptr<CFileIterator>* key_iter) const {
  RETURN_NOT_OK(key_index_reader()->Init(io_context));
  return key_index_reader()->NewIterator(key_iter, CFileReader::CACHE_BLOCK, io_context);
}

////////////////////////////////////////////////////////////
// Iterator
////////////////////////////////////////////////////////////
CFileSet::Iterator::~Iterator() {
}

Status CFileSet::Iterator::CreateColumnIterators(const ScanSpec* spec) {
  DCHECK_EQ(0, col_iters_.size());
  vector<unique_ptr<ColumnIterator>> ret_iters;
  ret_iters.reserve(projection_->num_columns());

  CFileReader::CacheControl cache_blocks = CFileReader::CACHE_BLOCK;
  if (spec && !spec->cache_blocks()) {
    cache_blocks = CFileReader::DONT_CACHE_BLOCK;
  }

  for (int proj_col_idx = 0;
       proj_col_idx < projection_->num_columns();
       proj_col_idx++) {
    ColumnId col_id = projection_->column_id(proj_col_idx);

    if (!base_data_->has_data_for_column_id(col_id)) {
      // If we have no data for a column, most likely it was added via an ALTER
      // operation after this CFileSet was flushed. In that case, we're guaranteed
      // that it is either NULLable, or has a "read-default". Otherwise, consider it a corruption.
      const ColumnSchema& col_schema = projection_->column(proj_col_idx);
      if (PREDICT_FALSE(!col_schema.is_nullable() && !col_schema.has_read_default())) {
        return Status::Corruption(Substitute("column $0 has no data in rowset $1",
                                             col_schema.ToString(), base_data_->ToString()));
      }
      ret_iters.emplace_back(new DefaultColumnValueIterator(col_schema.type_info(),
                                                            col_schema.read_default_value()));
      continue;
    }
    unique_ptr<CFileIterator> iter;
    RETURN_NOT_OK_PREPEND(base_data_->NewColumnIterator(col_id, cache_blocks, io_context_, &iter),
                          Substitute("could not create iterator for column $0",
                                     projection_->column(proj_col_idx).ToString()));
    ret_iters.emplace_back(std::move(iter));
  }

  col_iters_.swap(ret_iters);
  prepared_iters_.reserve(col_iters_.size());
  return Status::OK();
}

Status CFileSet::Iterator::Init(ScanSpec *spec) {
  CHECK(!initted_);

  RETURN_NOT_OK(base_data_->CountRows(io_context_, &row_count_));

  // Setup key iterator.
  RETURN_NOT_OK(base_data_->NewKeyIterator(io_context_, &key_iter_));

  // Setup column iterators.
  RETURN_NOT_OK(CreateColumnIterators(spec));

  // If there is a range predicate on the key column, push that down into an
  // ordinal range.
  RETURN_NOT_OK(PushdownRangeScanPredicate(spec));

  initted_ = true;

  // Don't actually seek -- we'll seek when we first actually read the
  // data.
  cur_idx_ = lower_bound_idx_;
  Unprepare(); // Reset state.
  return Status::OK();
}

Status CFileSet::Iterator::PushdownRangeScanPredicate(ScanSpec *spec) {
  CHECK_GT(row_count_, 0);

  lower_bound_idx_ = 0;
  upper_bound_idx_ = row_count_;

  if (spec == nullptr) {
    // No predicate.
    return Status::OK();
  }

  Schema key_schema_for_vlog;
  if (VLOG_IS_ON(1)) {
    key_schema_for_vlog = base_data_->tablet_schema().CreateKeyProjection();
  }

  if (spec->lower_bound_key() &&
      spec->lower_bound_key()->encoded_key().compare(base_data_->min_encoded_key_) > 0) {
    bool exact;
    Status s = key_iter_->SeekAtOrAfter(*spec->lower_bound_key(), &exact);
    if (s.IsNotFound()) {
      // The lower bound is after the end of the key range.
      // Thus, no rows will pass the predicate, so we set the lower bound
      // to the end of the file.
      lower_bound_idx_ = row_count_;
      return Status::OK();
    }
    RETURN_NOT_OK(s);

    lower_bound_idx_ = std::max(lower_bound_idx_, key_iter_->GetCurrentOrdinal());
    VLOG(1) << "Pushed lower bound value "
            << spec->lower_bound_key()->Stringify(key_schema_for_vlog)
            << " as row_idx >= " << lower_bound_idx_;
  }
  if (spec->exclusive_upper_bound_key() &&
      spec->exclusive_upper_bound_key()->encoded_key().compare(
          base_data_->max_encoded_key_) <= 0) {
    bool exact;
    Status s = key_iter_->SeekAtOrAfter(*spec->exclusive_upper_bound_key(), &exact);
    if (PREDICT_FALSE(s.IsNotFound())) {
      LOG(DFATAL) << "CFileSet indicated upper bound was within range, but "
                  << "key iterator could not seek. "
                  << "CFileSet upper_bound = "
                  << KUDU_REDACT(Slice(base_data_->max_encoded_key_).ToDebugString())
                  << ", enc_key = "
                  << KUDU_REDACT(spec->exclusive_upper_bound_key()->encoded_key().ToDebugString());
    } else {
      RETURN_NOT_OK(s);

      rowid_t cur = key_iter_->GetCurrentOrdinal();
      upper_bound_idx_ = std::min(upper_bound_idx_, cur);

      VLOG(1) << "Pushed upper bound value "
              << spec->exclusive_upper_bound_key()->Stringify(key_schema_for_vlog)
              << " as row_idx < " << upper_bound_idx_;
    }
  }
  return Status::OK();
}

void CFileSet::Iterator::Unprepare() {
  prepared_count_ = 0;
  prepared_iters_.clear();
}

Status CFileSet::Iterator::PrepareBatch(size_t *nrows) {
  DCHECK_EQ(prepared_count_, 0) << "Already prepared";

  size_t remaining = upper_bound_idx_ - cur_idx_;
  if (*nrows > remaining) {
    *nrows = remaining;
  }

  prepared_count_ = *nrows;

  // Lazily prepare the first column when it is materialized.
  return Status::OK();
}

Status CFileSet::Iterator::PrepareColumn(ColumnMaterializationContext *ctx) {
  ColumnIterator* col_iter = col_iters_[ctx->col_idx()].get();
  size_t n = prepared_count_;

  if (!col_iter->seeked() || col_iter->GetCurrentOrdinal() != cur_idx_) {
    // Either this column has not yet been accessed, or it was accessed
    // but then skipped in a prior block (e.g because predicates on other
    // columns completely eliminated the block).
    //
    // Either way, we need to seek it to the correct offset.
    RETURN_NOT_OK(col_iter->SeekToOrdinal(cur_idx_));
  }

  Status s = col_iter->PrepareBatch(&n);
  if (!s.ok()) {
    LOG(WARNING) << "Unable to prepare column " << ctx->col_idx() << ": " << s.ToString();
    return s;
  }

  if (n != prepared_count_) {
    return Status::Corruption(
            StringPrintf("Column %zd (%s) didn't yield enough rows at offset %zd: expected "
                                 "%zd but only got %zd", ctx->col_idx(),
                         projection_->column(ctx->col_idx()).ToString().c_str(),
                         cur_idx_, prepared_count_, n));
  }

  prepared_iters_.emplace_back(col_iter);

  return Status::OK();
}

Status CFileSet::Iterator::InitializeSelectionVector(SelectionVector *sel_vec) {
  sel_vec->SetAllTrue();
  return Status::OK();
}

Status CFileSet::Iterator::MaterializeColumn(ColumnMaterializationContext *ctx) {
  CHECK_EQ(prepared_count_, ctx->block()->nrows());
  DCHECK_LT(ctx->col_idx(), col_iters_.size());

  RETURN_NOT_OK(PrepareColumn(ctx));
  ColumnIterator* iter = col_iters_[ctx->col_idx()].get();

  RETURN_NOT_OK(iter->Scan(ctx));

  return Status::OK();
}

Status CFileSet::Iterator::FinishBatch() {
  DCHECK_GT(prepared_count_, 0);

  for (ColumnIterator* col_iter : prepared_iters_) {
    RETURN_NOT_OK(col_iter->FinishBatch());
  }

  cur_idx_ += prepared_count_;
  Unprepare();

  return Status::OK();
}


void CFileSet::Iterator::GetIteratorStats(vector<IteratorStats>* stats) const {
  stats->clear();
  stats->reserve(col_iters_.size());
  for (const auto& iter : col_iters_) {
    ANNOTATE_IGNORE_READS_BEGIN();
    stats->push_back(iter->io_statistics());
    ANNOTATE_IGNORE_READS_END();
  }
}

} // namespace tablet
} // namespace kudu
