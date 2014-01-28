// Copyright (c) 2012, Cloudera, inc.

#include <arpa/inet.h>
#include <string>

#include "common/wire_protocol.h"
#include "cfile/block_cache.h"
#include "cfile/block_encodings.h"
#include "cfile/cfile.h"
#include "cfile/cfile_reader.h"
#include "cfile/string_plain_block.h"
#include "gutil/gscoped_ptr.h"
#include "tablet/deltafile.h"
#include "tablet/mutation.h"
#include "util/coding-inl.h"
#include "util/env.h"
#include "util/env_util.h"
#include "util/hexdump.h"
#include "util/pb_util.h"

DEFINE_int32(deltafile_block_size, 32*1024,
            "Block size for delta files. TODO: this should be configurable "
             "on a per-table basis");

namespace kudu {

using cfile::BlockCacheHandle;
using cfile::BlockPointer;
using cfile::IndexTreeIterator;
using cfile::StringPlainBlockDecoder;
using cfile::CFileReader;

namespace tablet {

const char * const DeltaFileReader::kSchemaMetaEntryName = "schema";
const char * const DeltaFileReader::kDeltaStatsEntryName = "deltafilestats";

namespace {

Status DeltaStatsToPB(const DeltaStats& delta_stats,
                      size_t ncols,
                      DeltaStatsPB* pb) {
  pb->Clear();
  pb->set_delete_count(delta_stats.delete_count());
  for (size_t idx = 0; idx < ncols; idx++) {
    int64_t update_count = delta_stats.update_count(idx);
    pb->add_per_column_update_count(update_count);
  }
  return Status::OK();
}

Status DeltaStatsFromPB(const DeltaStatsPB& pb,
                        DeltaStats* delta_stats) {
  delta_stats->IncrDeleteCount<false>(pb.delete_count());
  for (size_t idx = 0; idx < delta_stats->num_columns(); idx++) {
    delta_stats->IncrUpdateCount<false>(idx, pb.per_column_update_count(idx));
  }
  return Status::OK();
}

} // namespace



DeltaFileWriter::DeltaFileWriter(const Schema &schema,
                                 const shared_ptr<WritableFile> &file) :
    schema_(schema)
#ifndef NDEBUG
  ,has_appended_(false)
#endif
{ // NOLINT(*)
  cfile::WriterOptions opts;
  opts.write_validx = true;
  opts.block_size = FLAGS_deltafile_block_size;
  opts.storage_attributes = ColumnStorageAttributes(PLAIN_ENCODING);
  writer_.reset(new cfile::Writer(opts, STRING, false, file));
}


Status DeltaFileWriter::Start() {
  return writer_->Start();
}

Status DeltaFileWriter::Finish() {
  RETURN_NOT_OK(WriteSchema());
  return writer_->Finish();
}

Status DeltaFileWriter::AppendDelta(
  const DeltaKey &key, const RowChangeList &delta) {
  Slice delta_slice(delta.slice());

  // See TODO in RowChangeListEncoder::SetToReinsert
  CHECK(!delta.is_reinsert())
    << "TODO: REINSERT deltas cannot currently be written to disk "
    << "since they don't have a standalone encoded form.";

#ifndef NDEBUG
  // Sanity check insertion order in debug mode.
  if (has_appended_) {
    DCHECK(last_key_.CompareTo(key) < 0)
      << "must insert deltas in sorted order: "
      << "got key " << key.ToString() << " after "
      << last_key_.ToString();
  }
  has_appended_ = true;
  last_key_= key;
#endif

  tmp_buf_.clear();

  // Write the encoded form of the key to the file.
  key.EncodeTo(&tmp_buf_);

  tmp_buf_.append(delta_slice.data(), delta_slice.size());
  Slice tmp_buf_slice(tmp_buf_);
  return writer_->AppendEntries(&tmp_buf_slice, 1);
}

Status DeltaFileWriter::WriteSchema() {
  SchemaPB schema_pb;
  CHECK_OK(SchemaToPB(schema_, &schema_pb));

  faststring buf;
  if (!pb_util::SerializeToString(schema_pb, &buf)) {
    return Status::IOError("Unable to serialize SchemaPB", schema_pb.DebugString());
  }

  writer_->AddMetadataPair(DeltaFileReader::kSchemaMetaEntryName, buf.ToString());
  return Status::OK();
}

Status DeltaFileWriter::WriteDeltaStats(const DeltaStats& stats) {
  DeltaStatsPB delta_stats_pb;
  CHECK_OK(DeltaStatsToPB(stats, schema_.num_columns(), &delta_stats_pb));

  faststring buf;
  if (!pb_util::SerializeToString(delta_stats_pb, &buf)) {
    return Status::IOError("Unable to serialize DeltaStatsPB", delta_stats_pb.DebugString());
  }

  writer_->AddMetadataPair(DeltaFileReader::kDeltaStatsEntryName, buf.ToString());
  return Status::OK();
}


////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DeltaFileReader::Open(Env *env,
                             const string &path,
                             int64_t delta_id,
                             gscoped_ptr<DeltaFileReader> *reader_out) {
  shared_ptr<RandomAccessFile> file;
  RETURN_NOT_OK(env_util::OpenFileForRandom(env, path, &file));
  uint64_t size;
  RETURN_NOT_OK(env->GetFileSize(path, &size));
  return Open(path, file, size, delta_id, reader_out);
}

Status DeltaFileReader::Open(const string& path,
                             const shared_ptr<RandomAccessFile> &file,
                             uint64_t file_size,
                             int64_t delta_id,
                             gscoped_ptr<DeltaFileReader> *reader_out) {
  gscoped_ptr<CFileReader> cf_reader;
  RETURN_NOT_OK(CFileReader::Open(file, file_size, cfile::ReaderOptions(), &cf_reader));

  gscoped_ptr<DeltaFileReader> df_reader(new DeltaFileReader(delta_id,
                                                             cf_reader.release(),
                                                             path));

  RETURN_NOT_OK(df_reader->Init());
  reader_out->reset(df_reader.release());

  return Status::OK();
}

DeltaFileReader::DeltaFileReader(const int64_t id,
                                 CFileReader *cf_reader,
                                 const string &path)
  : id_(id),
    reader_(cf_reader),
    path_(path) {
}

Status DeltaFileReader::Init() {
  // CF reader already initialized
  if (!reader_->has_validx()) {
    return Status::Corruption("file does not have a value index!");
  }

  // Initialize delta file schema
  RETURN_NOT_OK(ReadSchema());
  // Initialize delta file stats
  RETURN_NOT_OK(ReadDeltaStats());
  return Status::OK();
}

Status DeltaFileReader::ReadSchema() {
  string schema_pb_buf;
  if (!reader_->GetMetadataEntry(kSchemaMetaEntryName, &schema_pb_buf)) {
    return Status::Corruption("missing schema from the delta file metadata");
  }

  SchemaPB schema_pb;
  if (!schema_pb.ParseFromString(schema_pb_buf)) {
    return Status::Corruption("unable to parse schema protobuf");
  }
  return SchemaFromPB(schema_pb, &schema_);
}

Status DeltaFileReader::ReadDeltaStats() {
  string filestats_pb_buf;
  if (!reader_->GetMetadataEntry(kDeltaStatsEntryName, &filestats_pb_buf)) {
    return Status::Corruption("missing delta stats from the delta file metadata");
  }

  DeltaStatsPB deltastats_pb;
  if (!deltastats_pb.ParseFromString(filestats_pb_buf)) {
    return Status::Corruption("unable to parse the delta stats protobuf");
  }
  gscoped_ptr<DeltaStats>stats(new DeltaStats(schema_.num_columns()));
  RETURN_NOT_OK(DeltaStatsFromPB(deltastats_pb, stats.get()));
  delta_stats_.swap(stats);
  return Status::OK();
}

DeltaIterator *DeltaFileReader::NewDeltaIterator(const Schema *projection,
                                                 const MvccSnapshot &snap) const {
  return new DeltaFileIterator(this, projection, snap);
}

Status DeltaFileReader::CheckRowDeleted(rowid_t row_idx, bool *deleted) const {
  MvccSnapshot snap_all(MvccSnapshot::CreateSnapshotIncludingAllTransactions());

  // TODO: can use an empty schema here? also, would be nice to avoid the
  // allocations, but would probably require some refactoring.
  gscoped_ptr<DeltaIterator> iter(NewDeltaIterator(&schema_, snap_all));
  RETURN_NOT_OK(iter->Init());
  RETURN_NOT_OK(iter->SeekToOrdinal(row_idx));
  RETURN_NOT_OK(iter->PrepareBatch(1));

  // TODO: this does an allocation - can we stack-allocate the bitmap
  // and make SelectionVector able to "release" its buffer?
  SelectionVector sel_vec(1);
  sel_vec.SetAllTrue();
  RETURN_NOT_OK(iter->ApplyDeletes(&sel_vec));
  *deleted = !sel_vec.IsRowSelected(0);
  return Status::OK();
}


////////////////////////////////////////////////////////////
// DeltaFileIterator
////////////////////////////////////////////////////////////

DeltaFileIterator::DeltaFileIterator(const DeltaFileReader *dfr,
                                     const Schema *projection,
                                     const MvccSnapshot &snap) :
  dfr_(dfr),
  cfile_reader_(dfr->cfile_reader()),
  projector_(&dfr->schema(), projection),
  mvcc_snap_(snap),
  prepared_idx_(0xdeadbeef),
  prepared_count_(0),
  prepared_(false),
  exhausted_(false)
{}

Status DeltaFileIterator::Init() {
  CHECK(index_iter_.get() == NULL) << "Already initted";

  RETURN_NOT_OK(projector_.Init());

  BlockPointer validx_root = cfile_reader_->validx_root();
  index_iter_.reset(
    IndexTreeIterator::Create(cfile_reader_.get(), STRING, validx_root));

  return Status::OK();
}

Status DeltaFileIterator::SeekToOrdinal(rowid_t idx) {
  CHECK(index_iter_.get() != NULL) << "Must call Init()";

  tmp_buf_.clear();
  DeltaKey(idx, txid_t(0)).EncodeTo(&tmp_buf_);
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

  prepared_idx_ = idx;
  prepared_count_ = 0;
  prepared_ = false;
  delta_blocks_.clear();
  exhausted_ = false;
  return Status::OK();
}

Status DeltaFileIterator::ReadCurrentBlockOntoQueue() {
  DCHECK(index_iter_ != NULL);
  PreparedDeltaBlock prep_block;

  BlockCacheHandle dblk_handle;
  BlockPointer dblk_ptr = index_iter_->GetCurrentBlockPointer();
  RETURN_NOT_OK(cfile_reader_->ReadBlock(dblk_ptr, &dblk_handle));

  // The data has been successfully read. Create the decoder.
  gscoped_ptr<PreparedDeltaBlock> pdb(new PreparedDeltaBlock());
  pdb->prepared_block_start_idx_ = 0;
  pdb->block_ptr_ = dblk_ptr;
  pdb->block_.swap(&dblk_handle);

  // Decode the block.
  pdb->decoder_.reset(new StringPlainBlockDecoder(pdb->block_.data()));
  RETURN_NOT_OK(pdb->decoder_->ParseHeader());

  RETURN_NOT_OK(GetFirstRowIndexInCurrentBlock(&pdb->first_updated_idx_));
  RETURN_NOT_OK(GetLastRowIndexInDecodedBlock(*pdb->decoder_, &pdb->last_updated_idx_));

  #ifndef NDEBUG
  VLOG(2) << "Read delta block which updates " <<
    pdb->first_updated_idx_ << " through " <<
    pdb->last_updated_idx_;
  #endif

  delta_blocks_.push_back(pdb.release());
  return Status::OK();
}

Status DeltaFileIterator::GetFirstRowIndexInCurrentBlock(rowid_t *idx) {
  Slice index_entry = index_iter_->GetCurrentKey();
  DeltaKey k;
  RETURN_NOT_OK(k.DecodeFrom(&index_entry));
  *idx = k.row_idx();
  return Status::OK();
}

Status DeltaFileIterator::GetLastRowIndexInDecodedBlock(const StringPlainBlockDecoder &dec,
                                                        rowid_t *idx) {
  DCHECK_GT(dec.Count(), 0);
  Slice s(dec.string_at_index(dec.Count() - 1));
  DeltaKey k;
  RETURN_NOT_OK(k.DecodeFrom(&s));
  *idx = k.row_idx();
  return Status::OK();
}


string DeltaFileIterator::PreparedDeltaBlock::ToString() const {
  return StringPrintf("%d-%d (%s)", first_updated_idx_, last_updated_idx_,
                      block_ptr_.ToString().c_str());
}

Status DeltaFileIterator::PrepareBatch(size_t nrows) {
  CHECK(index_iter_.get() != NULL) << "Must call Init()";
  CHECK_GT(nrows, 0);

  rowid_t start_row = prepared_idx_ + prepared_count_;
  rowid_t stop_row = start_row + nrows - 1;

  // Remove blocks from our list which are no longer relevant to the range
  // being prepared.
  while (!delta_blocks_.empty() &&
         delta_blocks_.front().last_updated_idx_ < start_row) {
    delta_blocks_.pop_front();
  }

  while (!exhausted_) {
    rowid_t next_block_rowidx;
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
    PreparedDeltaBlock &block = delta_blocks_.front();
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
  prepared_idx_ = start_row;
  prepared_count_ = nrows;
  prepared_ = true;
  return Status::OK();
}




template<class Visitor>
Status DeltaFileIterator::VisitMutations(Visitor *visitor) {
  DCHECK(prepared_) << "must Prepare";

  rowid_t start_row = prepared_idx_;

  BOOST_FOREACH(PreparedDeltaBlock &block, delta_blocks_) {
    StringPlainBlockDecoder &sbd = *block.decoder_;
    DVLOG(2) << "Visiting delta block " << block.first_updated_idx_ << "-"
      << block.last_updated_idx_ << " for row block starting at " << start_row;

    if (PREDICT_FALSE(start_row > block.last_updated_idx_)) {
      // The block to be updated completely falls after this delta block:
      //  <-- delta block -->      <-- delta block -->
      //                      <-- block to update     -->
      // This can happen because we don't know the block's last entry until after
      // we queued it in PrepareToApply(). We could potentially remove it at that
      // point during the prepare step, but for now just skip it here.
      continue;
    }

    for (int i = block.prepared_block_start_idx_; i < sbd.Count(); i++) {
      Slice slice = sbd.string_at_index(i);

      // Decode and check the ID of the row we're going to update.
      DeltaKey key;
      RETURN_NOT_OK(key.DecodeFrom(&slice));
      rowid_t row_idx = key.row_idx();

      // Check that the delta is within the block we're currently processing.
      if (row_idx >= start_row + prepared_count_) {
        // Delta is for a row which comes after the block we're processing.
        return Status::OK();
      } else if (row_idx < start_row) {
        // Delta is for a row which comes before the block we're processing.
        continue;
      }

      RETURN_NOT_OK(visitor->Visit(key, slice));
    }
  }

  return Status::OK();
}

// Visitor which applies updates to a specific column.
struct ApplyingVisitor {
  Status Visit(const DeltaKey &key, const Slice &deltas) {

    // Check that the delta is considered committed by the current reader's MVCC state.
    if (!dfi->mvcc_snap_.IsCommitted(key.txid())) {
      return Status::OK();
    }

    int64_t rel_idx = key.row_idx() - dfi->prepared_idx_;
    DCHECK_GE(rel_idx, 0);

    // TODO: this code looks eerily similar to DMSIterator::ApplyUpdates!
    // I bet it can be combined.

    RowChangeListDecoder decoder(dfi->dfr_->schema(), RowChangeList(deltas));
    RETURN_NOT_OK(decoder.Init());
    if (decoder.is_update()) {
      return decoder.ApplyToOneColumn(rel_idx, dst, col_to_apply, dst->arena());
    } else if (decoder.is_delete()) {
      // If it's a DELETE, then it will be processed by DeletingVisitor.
      return Status::OK();
    } else {
      dfi->FatalUnexpectedDelta(key, deltas, "Expect only UPDATE or DELETE deltas on disk");
    }
    return Status::OK();
  }

  DeltaFileIterator *dfi;
  size_t col_to_apply;
  ColumnBlock *dst;
};

Status DeltaFileIterator::ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) {
  DCHECK_LE(prepared_count_, dst->nrows());

  size_t projected_col;
  if (projector_.get_base_col_from_proj_idx(col_to_apply, &projected_col)) {
    ApplyingVisitor visitor = {this, projected_col, dst};
    return VisitMutations(&visitor);
  } else if (projector_.get_adapter_col_from_proj_idx(col_to_apply, &projected_col)) {
    // TODO: Handle the "different type" case (adapter_cols_mapping)
    LOG(DFATAL) << "Alter type is not implemented yet";
    return Status::NotSupported("Alter type is not implemented yet");
  } else {
    // Column not present in the deltas... skip!
    return Status::OK();
  }
}

// Visitor which applies deletes to the selection vector.
struct DeletingVisitor {
  Status Visit(const DeltaKey &key, const Slice &deltas) {

    // Check that the delta is considered committed by the current reader's MVCC state.
    if (!dfi->mvcc_snap_.IsCommitted(key.txid())) {
      return Status::OK();
    }

    int64_t rel_idx = key.row_idx() - dfi->prepared_idx_;
    DCHECK_GE(rel_idx, 0);

    RowChangeListDecoder decoder(dfi->dfr_->schema(), RowChangeList(deltas));
    RETURN_NOT_OK(decoder.Init());
    if (decoder.is_update()) {
      return Status::OK();
    } else if (decoder.is_delete()) {
      sel_vec->SetRowUnselected(rel_idx);
    } else {
      dfi->FatalUnexpectedDelta(key, deltas, "Expect only UPDATE or DELETE deltas on disk");
    }
    return Status::OK();
  }

  DeltaFileIterator *dfi;
  SelectionVector *sel_vec;
};

Status DeltaFileIterator::ApplyDeletes(SelectionVector *sel_vec) {
  DCHECK_LE(prepared_count_, sel_vec->nrows());
  DeletingVisitor visitor = {this, sel_vec};
  return VisitMutations(&visitor);
}

// Visitor which, for each mutation, appends it into a ColumnBlock of
// Mutation *s. See CollectMutations()
// Each mutation is projected into the iterator schema, if required.
struct CollectingVisitor {
  Status Visit(const DeltaKey &key, const Slice &deltas) {
    int64_t rel_idx = key.row_idx() - dfi->prepared_idx_;
    DCHECK_GE(rel_idx, 0);

    RowChangeList changelist(deltas);
    if (!dfi->projector_.is_identity()) {
      RETURN_NOT_OK(RowChangeListDecoder::ProjectUpdate(dfi->projector_,
                                                        changelist,
                                                        &dfi->delta_buf_));
      // The projection resulted in an empty mutation (e.g. update of a removed column)
      if (dfi->delta_buf_.size() == 0) return Status::OK();
      changelist = RowChangeList(dfi->delta_buf_);
    }

    Mutation *mutation = Mutation::CreateInArena(dst_arena, key.txid(), changelist);
    mutation->AppendToList(&dst->at(rel_idx));

    return Status::OK();
  }

  DeltaFileIterator *dfi;
  vector<Mutation *> *dst;
  Arena *dst_arena;
};

Status DeltaFileIterator::CollectMutations(vector<Mutation *> *dst, Arena *dst_arena) {
  DCHECK_LE(prepared_count_, dst->size());
  CollectingVisitor visitor = {this, dst, dst_arena};
  return VisitMutations(&visitor);
}

string DeltaFileIterator::ToString() const {
  return "DeltaFileIterator(" + dfr_->path() + ")";
}

struct FilterAndAppendVisitor {

  Status Visit(const DeltaKey& key, const Slice& deltas) {
    faststring buf;
    RowChangeListEncoder enc(dfi->dfr_->schema(), &buf);
    RETURN_NOT_OK(
        RowChangeListDecoder::RemoveColumnsFromChangeList(RowChangeList(deltas),
                                                          column_indexes,
                                                          dfi->dfr_->schema(),
                                                          &enc));
    if (enc.is_initialized()) {
      RowChangeList rcl = enc.as_changelist();
      DeltaKeyAndUpdate upd;
      upd.key = key;
      CHECK(arena->RelocateSlice(rcl.slice(), &upd.cell));
      out->push_back(upd);
    }
    // if enc.is_initialized() return false, that means deltas only
    // contained the specified columns.
    return Status::OK();
  }

  const DeltaFileIterator* dfi;
  const metadata::ColumnIndexes& column_indexes;
  vector<DeltaKeyAndUpdate>* out;
  Arena* arena;
};

Status DeltaFileIterator::FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                                 vector<DeltaKeyAndUpdate>* out,
                                                 Arena* arena) {
  FilterAndAppendVisitor visitor = {this, col_indexes, out, arena};
  return VisitMutations(&visitor);
}

void DeltaFileIterator::FatalUnexpectedDelta(const DeltaKey &key, const Slice &deltas,
                                             const string &msg) {
  LOG(FATAL) << "Saw unexpected delta type in deltafile " << dfr_->path() << ": "
             << " rcl=" << RowChangeList(deltas).ToString(dfr_->schema())
             << " key=" << key.ToString() << " (" << msg << ")";
}

} // namespace tablet
} // namespace kudu
