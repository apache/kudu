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

#include "kudu/tablet/deltafile.h"

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/cfile/binary_plain_block.h"
#include "kudu/cfile/cfile_reader.h"
#include "kudu/cfile/cfile_util.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/cfile/index_btree.h"
#include "kudu/common/common.pb.h"
#include "kudu/common/row_changelist.h"
#include "kudu/common/rowblock.h"
#include "kudu/common/scan_spec.h"
#include "kudu/common/schema.h"
#include "kudu/common/timestamp.h"
#include "kudu/common/types.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/delta_relevancy.h"
#include "kudu/tablet/mvcc.h"
#include "kudu/tablet/rowset.h"
#include "kudu/tablet/tablet.pb.h"
#include "kudu/util/compression/compression_codec.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/trace.h"

DECLARE_bool(cfile_lazy_open);
DEFINE_int32(deltafile_default_block_size, 32*1024,
            "Block size for delta files. In the future, this may become configurable "
             "on a per-table basis.");
TAG_FLAG(deltafile_default_block_size, experimental);

DEFINE_string(deltafile_default_compression_codec, "lz4",
              "The compression codec used when writing deltafiles.");
TAG_FLAG(deltafile_default_compression_codec, experimental);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

class MemTracker;

using cfile::BinaryPlainBlockDecoder;
using cfile::BlockPointer;
using cfile::CFileReader;
using cfile::IndexTreeIterator;
using cfile::ReaderOptions;
using fs::BlockCreationTransaction;
using fs::BlockManager;
using fs::IOContext;
using fs::ReadableBlock;
using fs::WritableBlock;

namespace tablet {

const char * const DeltaFileReader::kDeltaStatsEntryName = "deltafilestats";

DeltaFileWriter::DeltaFileWriter(unique_ptr<WritableBlock> block)
#ifndef NDEBUG
 : has_appended_(false)
#endif
{ // NOLINT(*)
  cfile::WriterOptions opts;
  opts.write_validx = true;
  opts.storage_attributes.cfile_block_size = FLAGS_deltafile_default_block_size;
  opts.storage_attributes.encoding = PLAIN_ENCODING;
  opts.storage_attributes.compression =
      GetCompressionCodecType(FLAGS_deltafile_default_compression_codec);

  // The CFile value index is 'compressed' by truncating delta values to only
  // contain the delta key. The entire deltakey is required in order to support
  // efficient seeks without deserializing the entire block. The generic value
  // index optimization is disabled, since it could truncate portions of the
  // deltakey.
  //
  // Note: The deltafile usage of the CFile value index is irregular, since it
  // inserts values in non-sorted order (the timestamp portion of the deltakey
  // in UNDO files is sorted in descending order). This doesn't appear to cause
  // problems in practice.
  opts.optimize_index_keys = false;
  opts.validx_key_encoder = [] (const void* value, faststring* buffer) {
    buffer->clear();
    const Slice* s1 = static_cast<const Slice*>(value);
    Slice s2(*s1);
    DeltaKey key;
    CHECK_OK(key.DecodeFrom(&s2));
    key.EncodeTo(buffer);
  };

  writer_.reset(new cfile::CFileWriter(std::move(opts),
                                       GetTypeInfo(BINARY),
                                       false,
                                       std::move(block)));
}

Status DeltaFileWriter::Start() {
  return writer_->Start();
}

Status DeltaFileWriter::Finish() {
  BlockManager* bm = writer_->block()->block_manager();
  unique_ptr<BlockCreationTransaction> transaction = bm->NewCreationTransaction();
  RETURN_NOT_OK(FinishAndReleaseBlock(transaction.get()));
  return transaction->CommitCreatedBlocks();
}

Status DeltaFileWriter::FinishAndReleaseBlock(BlockCreationTransaction* transaction) {
  if (writer_->written_value_count() == 0) {
    return Status::Aborted("no deltas written");
  }
  return writer_->FinishAndReleaseBlock(transaction);
}

Status DeltaFileWriter::DoAppendDelta(const DeltaKey &key,
                                      const RowChangeList &delta) {
  Slice delta_slice(delta.slice());
  tmp_buf_.clear();

  // Write the encoded form of the key to the file.
  key.EncodeTo(&tmp_buf_);

  tmp_buf_.append(delta_slice.data(), delta_slice.size());
  Slice tmp_buf_slice(tmp_buf_);

  return writer_->AppendEntries(&tmp_buf_slice, 1);
}

template<>
Status DeltaFileWriter::AppendDelta<REDO>(
  const DeltaKey &key, const RowChangeList &delta) {

#ifndef NDEBUG
  // Sanity check insertion order in debug mode.
  if (has_appended_) {
    DCHECK(last_key_.CompareTo<REDO>(key) <= 0)
      << "must insert redo deltas in sorted order (ascending key, then ascending ts): "
      << "got key " << key.ToString() << " after "
      << last_key_.ToString();
  }
  has_appended_ = true;
  last_key_ = key;
#endif

  return DoAppendDelta(key, delta);
}

template<>
Status DeltaFileWriter::AppendDelta<UNDO>(
  const DeltaKey &key, const RowChangeList &delta) {

#ifndef NDEBUG
  // Sanity check insertion order in debug mode.
  if (has_appended_) {
    DCHECK(last_key_.CompareTo<UNDO>(key) <= 0)
      << "must insert undo deltas in sorted order (ascending key, then descending ts): "
      << "got key " << key.ToString() << " after "
      << last_key_.ToString();
  }
  has_appended_ = true;
  last_key_ = key;
#endif

  return DoAppendDelta(key, delta);
}

void DeltaFileWriter::WriteDeltaStats(const DeltaStats& stats) {
  DeltaStatsPB delta_stats_pb;
  stats.ToPB(&delta_stats_pb);

  faststring buf;
  pb_util::SerializeToString(delta_stats_pb, &buf);
  writer_->AddMetadataPair(DeltaFileReader::kDeltaStatsEntryName, buf.ToString());
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DeltaFileReader::Open(unique_ptr<ReadableBlock> block,
                             DeltaType delta_type,
                             ReaderOptions options,
                             shared_ptr<DeltaFileReader>* reader_out) {
  shared_ptr<DeltaFileReader> df_reader;
  const IOContext* io_context = options.io_context;
  RETURN_NOT_OK(DeltaFileReader::OpenNoInit(std::move(block),
                                            delta_type,
                                            std::move(options),
                                            /*delta_stats*/nullptr,
                                            &df_reader));
  RETURN_NOT_OK(df_reader->Init(io_context));

  *reader_out = df_reader;
  return Status::OK();
}

Status DeltaFileReader::OpenNoInit(unique_ptr<ReadableBlock> block,
                                   DeltaType delta_type,
                                   ReaderOptions options,
                                   unique_ptr<DeltaStats> delta_stats,
                                   shared_ptr<DeltaFileReader>* reader_out) {
  unique_ptr<CFileReader> cf_reader;
  const IOContext* io_context = options.io_context;
  RETURN_NOT_OK(CFileReader::OpenNoInit(std::move(block),
                                        std::move(options),
                                        &cf_reader));
  unique_ptr<DeltaFileReader> df_reader(
      new DeltaFileReader(std::move(cf_reader), std::move(delta_stats), delta_type));
  if (!FLAGS_cfile_lazy_open) {
    RETURN_NOT_OK(df_reader->Init(io_context));
  }

  reader_out->reset(df_reader.release());

  return Status::OK();
}

DeltaFileReader::DeltaFileReader(unique_ptr<CFileReader> cf_reader,
                                 unique_ptr<DeltaStats> delta_stats,
                                 DeltaType delta_type)
    : reader_(cf_reader.release()),
      delta_stats_(std::move(delta_stats)),
      delta_type_(delta_type) {}

Status DeltaFileReader::Init(const IOContext* io_context) {
  return init_once_.Init([this, io_context] { return InitOnce(io_context); });
}

Status DeltaFileReader::InitOnce(const IOContext* io_context) {
  // Fully open the CFileReader if it was lazily opened earlier.
  //
  // If it's already initialized, this is a no-op.
  RETURN_NOT_OK(reader_->Init(io_context));

  if (!reader_->has_validx()) {
    return Status::NotSupported("file does not have a value index!");
  }

  // Initialize delta file stats
  if (!has_delta_stats()) {
    RETURN_NOT_OK(ReadDeltaStats());
  }
  return Status::OK();
}

Status DeltaFileReader::ReadDeltaStats() {
  string filestats_pb_buf;
  if (!reader_->GetMetadataEntry(kDeltaStatsEntryName, &filestats_pb_buf)) {
    return Status::NotSupported("missing delta stats from the delta file metadata");
  }

  DeltaStatsPB deltastats_pb;
  if (!deltastats_pb.ParseFromString(filestats_pb_buf)) {
    return Status::Corruption("unable to parse the delta stats protobuf");
  }
  unique_ptr<DeltaStats> stats(new DeltaStats());
  RETURN_NOT_OK(stats->InitFromPB(deltastats_pb));
  std::lock_guard<simple_spinlock> l(stats_lock_);
  delta_stats_ = std::move(stats);
  return Status::OK();
}

bool DeltaFileReader::IsRelevantForSnapshots(
    const boost::optional<MvccSnapshot>& snap_to_exclude,
    const MvccSnapshot& snap_to_include) const {
  if (!init_once_.init_succeeded()) {
    // If we're not initted, it means we have no delta stats and must
    // assume that this file is relevant for every snapshot.
    return true;
  }

  // We don't know whether the caller's intent is to apply deltas, to select
  // them, or both. As such, we must be conservative and assume 'both', which
  // means the file is relevant if any relevancy criteria is true.
  bool relevant = delta_type_ == REDO ?
                  IsDeltaRelevantForApply<REDO>(snap_to_include,
                                                delta_stats_->min_timestamp()) :
                  IsDeltaRelevantForApply<UNDO>(snap_to_include,
                                                delta_stats_->max_timestamp());
  if (snap_to_exclude) {
    // The select criteria is the same regardless of delta_type_.
    relevant |= IsDeltaRelevantForSelect(*snap_to_exclude, snap_to_include,
                                         delta_stats_->min_timestamp(),
                                         delta_stats_->max_timestamp());
  }
  return relevant;
}

Status DeltaFileReader::CloneForDebugging(FsManager* fs_manager,
                                          const shared_ptr<MemTracker>& parent_mem_tracker,
                                          shared_ptr<DeltaFileReader>* out) const {
  unique_ptr<ReadableBlock> block;
  RETURN_NOT_OK(fs_manager->OpenBlock(reader_->block_id(), &block));
  ReaderOptions options;
  options.parent_mem_tracker = parent_mem_tracker;
  return DeltaFileReader::OpenNoInit(std::move(block), delta_type_, options,
                                     /*delta_stats*/nullptr, out);
}

Status DeltaFileReader::NewDeltaIterator(const RowIteratorOptions& opts,
                                         unique_ptr<DeltaIterator>* iterator) const {
  if (IsRelevantForSnapshots(opts.snap_to_exclude, opts.snap_to_include)) {
    if (VLOG_IS_ON(2)) {
      if (!init_once_.init_succeeded()) {
        TRACE_COUNTER_INCREMENT("delta_iterators_lazy_initted", 1);

        VLOG(2) << (delta_type_ == REDO ? "REDO" : "UNDO") << " delta " << ToString()
                << " has no delta stats"
                << ": can't cull for " << opts.snap_to_include.ToString();
      } else if (delta_type_ == REDO) {
        VLOG(2) << "REDO delta " << ToString()
                << " has min ts " << delta_stats_->min_timestamp().ToString()
                << ": can't cull for " << opts.snap_to_include.ToString();
      } else {
        VLOG(2) << "UNDO delta " << ToString()
                << " has max ts " << delta_stats_->max_timestamp().ToString()
                << ": can't cull for " << opts.snap_to_include.ToString();
      }
    }

    TRACE_COUNTER_INCREMENT("delta_iterators_relevant", 1);
    // Ugly cast, but it lets the iterator fully initialize the reader
    // during its first seek.
    auto s_this = const_cast<DeltaFileReader*>(this)->shared_from_this();
    if (delta_type_ == REDO) {
      iterator->reset(new DeltaFileIterator<REDO>(std::move(s_this), opts));
    } else {
      iterator->reset(new DeltaFileIterator<UNDO>(std::move(s_this), opts));
    }
    return Status::OK();
  }
  VLOG(2) << "Culling "
          << ((delta_type_ == REDO) ? "REDO":"UNDO")
          << " delta " << ToString() << " for " << opts.snap_to_include.ToString();
  return Status::NotFound("MvccSnapshot outside the range of this delta.");
}

Status DeltaFileReader::CheckRowDeleted(rowid_t row_idx, const IOContext* io_context,
                                        bool* deleted) const {
  RETURN_NOT_OK(const_cast<DeltaFileReader*>(this)->Init(io_context));
  // If there are no deletes in the delta file at all, we can short-circuit
  // the seek.
  if (delta_stats_->delete_count() == 0) {
    *deleted = false;
    return Status::OK();
  }

  // TODO(todd): would be nice to avoid allocation here, but we don't want to
  // duplicate all the logic from NewDeltaIterator. So, we'll heap-allocate
  // for now.
  Schema empty_schema;
  RowIteratorOptions opts;
  opts.projection = &empty_schema;
  opts.io_context = io_context;
  unique_ptr<DeltaIterator> iter;
  Status s = NewDeltaIterator(opts, &iter);
  if (s.IsNotFound()) {
    *deleted = false;
    return Status::OK();
  }
  RETURN_NOT_OK(s);
  ScanSpec spec;
  RETURN_NOT_OK(iter->Init(&spec));
  RETURN_NOT_OK(iter->SeekToOrdinal(row_idx));
  RETURN_NOT_OK(iter->PrepareBatch(1, DeltaIterator::PREPARE_FOR_APPLY));

  // TODO: this does an allocation - can we stack-allocate the bitmap
  // and make SelectionVector able to "release" its buffer?
  SelectionVector sel_vec(1);
  sel_vec.SetAllTrue();
  RETURN_NOT_OK(iter->ApplyDeletes(&sel_vec));
  *deleted = !sel_vec.IsRowSelected(0);
  return Status::OK();
}

uint64_t DeltaFileReader::EstimateSize() const {
  return reader_->file_size();
}


////////////////////////////////////////////////////////////
// DeltaFileIterator
////////////////////////////////////////////////////////////

template<DeltaType Type>
DeltaFileIterator<Type>::DeltaFileIterator(shared_ptr<DeltaFileReader> dfr,
                                           RowIteratorOptions opts)
    : dfr_(std::move(dfr)),
      preparer_(std::move(opts)),
      prepared_(false),
      exhausted_(false),
      initted_(false),
      cache_blocks_(CFileReader::CACHE_BLOCK) {}

template<DeltaType Type>
Status DeltaFileIterator<Type>::Init(ScanSpec* spec) {
  DCHECK(!initted_) << "Already initted";

  if (spec) {
    cache_blocks_ = spec->cache_blocks() ? CFileReader::CACHE_BLOCK :
                                           CFileReader::DONT_CACHE_BLOCK;
  }

  initted_ = true;
  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::SeekToOrdinal(rowid_t idx) {
  DCHECK(initted_) << "Must call Init()";

  // Finish the initialization of any lazily-initialized state.
  RETURN_NOT_OK(dfr_->Init(preparer_.opts().io_context));

  // Check again whether this delta file is relevant given the snapshots
  // that we are querying. We did this already before creating the
  // DeltaFileIterator, but due to lazy initialization, it's possible
  // that we weren't able to check at that time.
  if (!dfr_->IsRelevantForSnapshots(preparer_.opts().snap_to_exclude,
                                    preparer_.opts().snap_to_include)) {
    exhausted_ = true;
    delta_blocks_.clear();
    return Status::OK();
  }

  if (!index_iter_) {
    index_iter_.reset(IndexTreeIterator::Create(
        preparer_.opts().io_context,
        dfr_->cfile_reader().get(),
        dfr_->cfile_reader()->validx_root()));
  }

  tmp_buf_.clear();
  DeltaKey(idx, Timestamp(0)).EncodeTo(&tmp_buf_);
  Slice key_slice(tmp_buf_);

  Status s = index_iter_->SeekAtOrBefore(key_slice);
  if (PREDICT_FALSE(s.IsNotFound())) {
    // Seeking to a value before the first value in the file
    // will return NotFound, due to the way the index seek
    // works. We need to special-case this and have the
    // iterator seek all the way down its leftmost branches
    // to get the correct result.
    s = index_iter_->SeekToFirst();
  }
  RETURN_NOT_OK(s);

  preparer_.Seek(idx);
  prepared_ = false;
  delta_blocks_.clear();
  exhausted_ = false;
  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::ReadCurrentBlockOntoQueue() {
  DCHECK(initted_) << "Must call Init()";
  DCHECK(index_iter_) << "Must call SeekToOrdinal()";

  unique_ptr<PreparedDeltaBlock> pdb(new PreparedDeltaBlock());
  BlockPointer dblk_ptr = index_iter_->GetCurrentBlockPointer();
  shared_ptr<CFileReader> reader = dfr_->cfile_reader();
  RETURN_NOT_OK(reader->ReadBlock(preparer_.opts().io_context,
                                  dblk_ptr, cache_blocks_, &pdb->block_));

  // The data has been successfully read. Finish creating the decoder.
  pdb->prepared_block_start_idx_ = 0;
  pdb->block_ptr_ = dblk_ptr;

  // Decode the block.
  pdb->decoder_.reset(new BinaryPlainBlockDecoder(pdb->block_.data()));
  RETURN_NOT_OK_PREPEND(pdb->decoder_->ParseHeader(),
                        Substitute("unable to decode data block header in delta block $0 ($1)",
                                   dfr_->cfile_reader()->block_id().ToString(),
                                   dblk_ptr.ToString()));

  RETURN_NOT_OK(GetFirstRowIndexInCurrentBlock(&pdb->first_updated_idx_));
  RETURN_NOT_OK(GetLastRowIndexInDecodedBlock(*pdb->decoder_, &pdb->last_updated_idx_));

  #ifndef NDEBUG
  VLOG(2) << "Read delta block which updates " <<
    pdb->first_updated_idx_ << " through " <<
    pdb->last_updated_idx_;
  #endif

  delta_blocks_.emplace_back(std::move(pdb));
  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::GetFirstRowIndexInCurrentBlock(rowid_t *idx) {
  DCHECK(index_iter_) << "Must call SeekToOrdinal()";

  Slice index_entry = index_iter_->GetCurrentKey();
  DeltaKey k;
  RETURN_NOT_OK(k.DecodeFrom(&index_entry));
  *idx = k.row_idx();
  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::GetLastRowIndexInDecodedBlock(const BinaryPlainBlockDecoder &dec,
                                                              rowid_t *idx) {
  DCHECK_GT(dec.Count(), 0);
  Slice s(dec.string_at_index(dec.Count() - 1));
  DeltaKey k;
  RETURN_NOT_OK(k.DecodeFrom(&s));
  *idx = k.row_idx();
  return Status::OK();
}

template<DeltaType Type>
string DeltaFileIterator<Type>::PreparedDeltaBlock::ToString() const {
  return StringPrintf("%d-%d (%s)", first_updated_idx_, last_updated_idx_,
                      block_ptr_.ToString().c_str());
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::PrepareBatch(size_t nrows, int prepare_flags) {
  DCHECK(initted_) << "Must call Init()";
  DCHECK(exhausted_ || index_iter_) << "Must call SeekToOrdinal()";

  CHECK_GT(nrows, 0);

  rowid_t start_row = preparer_.cur_prepared_idx();
  rowid_t stop_row = start_row + nrows - 1;

  // Remove blocks from our list which are no longer relevant to the range
  // being prepared.
  while (!delta_blocks_.empty() &&
         delta_blocks_.front()->last_updated_idx_ < start_row) {
    delta_blocks_.pop_front();
  }

  while (!exhausted_) {
    rowid_t next_block_rowidx = 0;
    RETURN_NOT_OK(GetFirstRowIndexInCurrentBlock(&next_block_rowidx));
    VLOG(2) << "Current delta block starting at row " << next_block_rowidx;

    if (next_block_rowidx > stop_row) {
      break;
    }

    RETURN_NOT_OK(ReadCurrentBlockOntoQueue());

    Status s = index_iter_->Next();
    if (s.IsNotFound()) {
      exhausted_ = true;
      break;
    }
    RETURN_NOT_OK(s);
  }

  if (!delta_blocks_.empty()) {
    PreparedDeltaBlock& block = *delta_blocks_.front();
    int i = 0;
    for (i = block.prepared_block_start_idx_;
         i < block.decoder_->Count();
         i++) {
      Slice s(block.decoder_->string_at_index(i));
      DeltaKey key;
      RETURN_NOT_OK(key.DecodeFrom(&s));
      if (key.row_idx() >= start_row) break;
    }
    block.prepared_block_start_idx_ = i;
  }

  #ifndef NDEBUG
  VLOG(2) << "Done preparing deltas for " << start_row << "-" << stop_row
          << ": row block spans " << delta_blocks_.size() << " delta blocks";
  #endif
  prepared_ = true;

  preparer_.Start(nrows, prepare_flags);
  RETURN_NOT_OK(AddDeltas(start_row, stop_row));
  preparer_.Finish(nrows);
  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::AddDeltas(rowid_t start_row, rowid_t stop_row) {
  DCHECK(prepared_) << "must Prepare";

  for (auto& block : delta_blocks_) {
    const BinaryPlainBlockDecoder& bpd = *block->decoder_;
    DVLOG(2) << "Adding deltas from delta block " << block->first_updated_idx_ << "-"
             << block->last_updated_idx_ << " for row block starting at " << start_row;

    if (PREDICT_FALSE(start_row > block->last_updated_idx_)) {
      // The block to be updated completely falls after this delta block:
      //  <-- delta block -->      <-- delta block -->
      //                      <-- block to update     -->
      // This can happen because we don't know the block's last entry until after
      // we queued it in PrepareBatch(). We could potentially remove it at that
      // point during the prepare step, but for now just skip it here.
      continue;
    }

    bool finished_row = false;
    for (int i = block->prepared_block_start_idx_; i < bpd.Count(); i++) {
      Slice slice = bpd.string_at_index(i);

      // Decode and check the ID of the row we're going to update.
      DeltaKey key;
      RETURN_NOT_OK(key.DecodeFrom(&slice));

      // If this delta is for the same row as before, skip it if the previous
      // AddDelta() call told us that we're done with this row.
      if (preparer_.last_added_idx() &&
          preparer_.last_added_idx() == key.row_idx() &&
          finished_row) {
        continue;
      }
      finished_row = false;

      // Check that the delta is within the block we're currently processing.
      if (key.row_idx() > stop_row) {
        // Delta is for a row which comes after the block we're processing.
        return Status::OK();
      }
      if (key.row_idx() < start_row) {
        // Delta is for a row which comes before the block we're processing.
        continue;
      }

      // Note: if AddDelta sets 'finished_row' to true, we could skip the
      // remaining deltas for this row by seeking the block decoder. This trades
      // off the cost of a seek against the cost of decoding some irrelevant delta keys.
      //
      // Given that updates are expected to be uncommon and that most scans are
      // _not_ historical, the current implementation eschews seeking in favor of
      // skipping irrelevant deltas one by one.
      RETURN_NOT_OK(preparer_.AddDelta(key, slice, &finished_row));
      if (VLOG_IS_ON(3)) {
        RowChangeList rcl(slice);
        DVLOG(3) << "Visited " << DeltaType_Name(DeltaTypeSelector<Type>::kTag)
                 << " delta for key: " << key.ToString() << " Mut: "
                 << rcl.ToString(*preparer_.opts().projection)
                 << " Continue?: " << (!finished_row ? "TRUE" : "FALSE");
      }
    }
  }

  return Status::OK();
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::ApplyUpdates(size_t col_to_apply, ColumnBlock* dst,
                                             const SelectionVector& filter) {
  return preparer_.ApplyUpdates(col_to_apply, dst, filter);
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::ApplyDeletes(SelectionVector* sel_vec) {
  return preparer_.ApplyDeletes(sel_vec);
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::SelectDeltas(SelectedDeltas* deltas) {
  return preparer_.SelectDeltas(deltas);
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::CollectMutations(vector<Mutation*>* dst, Arena* arena) {
  return preparer_.CollectMutations(dst, arena);
}

template<DeltaType Type>
bool DeltaFileIterator<Type>::HasNext() {
  return !exhausted_ || !delta_blocks_.empty();
}

template<DeltaType Type>
bool DeltaFileIterator<Type>::MayHaveDeltas() const {
  return preparer_.MayHaveDeltas();
}

template<DeltaType Type>
string DeltaFileIterator<Type>::ToString() const {
  return "DeltaFileIterator(" + dfr_->ToString() + ")";
}

template<DeltaType Type>
Status DeltaFileIterator<Type>::FilterColumnIdsAndCollectDeltas(
    const vector<ColumnId>& col_ids,
    vector<DeltaKeyAndUpdate>* out,
    Arena* arena) {
  return preparer_.FilterColumnIdsAndCollectDeltas(col_ids, out, arena);
}

template<DeltaType Type>
void DeltaFileIterator<Type>::FatalUnexpectedDelta(const DeltaKey &key, const Slice &deltas,
                                             const string &msg) {
  LOG(FATAL) << "Saw unexpected delta type in deltafile " << dfr_->ToString() << ": "
             << " rcl=" << RowChangeList(deltas).ToString(*preparer_.opts().projection)
             << " key=" << key.ToString() << " (" << msg << ")";
}

} // namespace tablet
} // namespace kudu
