// Copyright (c) 2012, Cloudera, inc.

#include <arpa/inet.h>
#include <string>

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
#include "util/hexdump.h"

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

DeltaFileWriter::DeltaFileWriter(const Schema &schema,
                                 const shared_ptr<WritableFile> &file) :
  schema_(schema)
#ifndef NDEBUG
  ,has_appended_(false)
#endif
{
  cfile::WriterOptions opts;
  opts.write_validx = true;
  opts.block_size = FLAGS_deltafile_block_size;
  writer_.reset(new cfile::Writer(opts, STRING, cfile::PLAIN, file));
}


Status DeltaFileWriter::Start() {
  return writer_->Start();
}

Status DeltaFileWriter::Finish() {
  return writer_->Finish();
}

Status DeltaFileWriter::AppendDelta(
  const DeltaKey &key, const RowChangeList &delta) {
  Slice delta_slice(delta.slice());

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
  return writer_->AppendEntries(&tmp_buf_, 1, 0);
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status DeltaFileReader::Open(Env *env, const string &path,
                             const Schema &schema,
                             gscoped_ptr<DeltaFileReader> *reader_out) {
  gscoped_ptr<CFileReader> cf_reader;
  RETURN_NOT_OK(CFileReader::Open(env, path, cfile::ReaderOptions(), &cf_reader));

  gscoped_ptr<DeltaFileReader> df_reader(
    new DeltaFileReader(cf_reader.release(), path, schema));

  RETURN_NOT_OK(df_reader->Init());
  reader_out->reset(df_reader.release());

  return Status::OK();
}

DeltaFileReader::DeltaFileReader(CFileReader *cf_reader, const string &path,
                                 const Schema &schema) :
  reader_(cf_reader),
  schema_(schema),
  path_(path)
{
}

Status DeltaFileReader::Init() {
  // CF reader already initialized
  if (!reader_->has_validx()) {
    return Status::Corruption("file does not have a value index!");
  }
  return Status::OK();
}

DeltaIteratorInterface *DeltaFileReader::NewDeltaIterator(const Schema &projection,
                                                          const MvccSnapshot &snap) {
  return new DeltaFileIterator(this, projection, snap);
}

////////////////////////////////////////////////////////////
// DeltaFileIterator
////////////////////////////////////////////////////////////

DeltaFileIterator::DeltaFileIterator(DeltaFileReader *dfr,
                                     const Schema &projection,
                                     const MvccSnapshot &snap) :
  dfr_(dfr),
  cfile_reader_(dfr->cfile_reader()),
  projection_(projection),
  mvcc_snap_(snap),
  prepared_idx_(0xdeadbeef),
  prepared_count_(0),
  prepared_(false),
  exhausted_(false)
{}

Status DeltaFileIterator::Init() {
  CHECK(index_iter_.get() == NULL) << "Already initted";

  RETURN_NOT_OK(projection_.GetProjectionFrom(dfr_->schema(), &projection_indexes_));

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

  Status s = index_iter_->SeekAtOrBefore(&key_slice);
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
  Slice index_entry(*DCHECK_NOTNULL(reinterpret_cast<const Slice *>(
                                      index_iter_->GetCurrentKey())));
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
  CHECK(!projection_indexes_.empty()) << "Must Init()";
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
Status DeltaFileIterator::VisitUpdates(Visitor *visitor) {
  DCHECK(prepared_) << "must Prepare";

  rowid_t start_row = prepared_idx_;

  BOOST_FOREACH(PreparedDeltaBlock &block, delta_blocks_) {
    StringPlainBlockDecoder &sbd = *block.decoder_;
    DVLOG(2) << "Visiting delta block " << block.first_updated_idx_ << "-" << block.last_updated_idx_
      << " for row block starting at " << start_row;

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

    RowChangeListDecoder decoder(dfi->dfr_->schema(), RowChangeList(deltas));
    return decoder.ApplyToOneColumn(col_to_apply, dst->cell_ptr(rel_idx), dst->arena());
  }

  DeltaFileIterator *dfi;
  size_t col_to_apply;
  ColumnBlock *dst;
};


Status DeltaFileIterator::ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) {
  DCHECK_LE(prepared_count_, dst->nrows());
  size_t projected_col = projection_indexes_[col_to_apply];
  ApplyingVisitor visitor = {this, projected_col, dst};

  return VisitUpdates(&visitor);
}

// Visitor which, for each mutation, appends it into a ColumnBlock of
// Mutation *s. See CollectMutations()
struct CollectingVisitor {
  Status Visit(const DeltaKey &key, const Slice &deltas) {
    int64_t rel_idx = key.row_idx() - dfi->prepared_idx_;
    DCHECK_GE(rel_idx, 0);

    Mutation *mutation = Mutation::CreateInArena(
      dst_arena, key.txid(), RowChangeList(deltas));
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
  return VisitUpdates(&visitor);
}

string DeltaFileIterator::ToString() const {
  return "DeltaFileIterator(" + dfr_->path() + ")";
}

} // namespace tablet
} // namespace kudu
