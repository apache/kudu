// Copyright (c) 2012, Cloudera, inc.

#include <utility>

#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "tablet/delta_tracker.h"
#include "gutil/port.h"
#include "util/hexdump.h"
#include "util/status.h"
#include "tablet/mvcc.h"

namespace kudu { namespace tablet {

////////////////////////////////////////////////////////////
// DeltaMemStore implementation
////////////////////////////////////////////////////////////

DeltaMemStore::DeltaMemStore(int64_t id, const Schema &schema)
  : id_(id),
    schema_(schema),
    arena_(8*1024, 1*1024*1024) {
  CHECK(schema.has_column_ids());
}


Status DeltaMemStore::Update(txid_t txid,
                             rowid_t row_idx,
                             const RowChangeList &update) {
  DeltaKey key(row_idx, txid);

  // TODO: this allocation isn't great. Make faststring
  // allocate its initial buffer on the stack?
  faststring delta_buf;
  faststring buf;

  key.EncodeTo(&buf);
  Slice key_slice(buf);

  btree::PreparedMutation<btree::BTreeTraits> mutation(key_slice);
  mutation.Prepare(&tree_);
  CHECK(!mutation.exists())
    << "Already have an entry for rowid " << row_idx << " at txid "
    << txid;
  if (!mutation.Insert(update.slice())) {
    return Status::IOError("Unable to insert into tree");
  }
  return Status::OK();
}

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw) const {
  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  iter->SeekToStart();
  while (iter->IsValid()) {
    Slice key_slice, val;
    iter->GetCurrentEntry(&key_slice, &val);
    DeltaKey key;
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    DCHECK_EQ(0, key_slice.size()) <<
      "After decoding delta key, should be empty";

    dfw->AppendDelta(key, RowChangeList(val));
    iter->Next();
  }
  return Status::OK();
}

DeltaIterator *DeltaMemStore::NewDeltaIterator(const Schema &projection,
                                               const MvccSnapshot &snapshot) const {
  return new DMSIterator(shared_from_this(), projection, snapshot);
}

Status DeltaMemStore::CheckRowDeleted(rowid_t row_idx, bool *deleted) const {
  *deleted = false;

  DeltaKey key(row_idx, txid_t(0));
  faststring buf;
  key.EncodeTo(&buf);
  Slice key_slice(buf);

  bool exact;

  // TODO: can we avoid the allocation here?
  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  if (!iter->SeekAtOrAfter(key_slice, &exact)) {
    return Status::OK();
  }

  while (iter->IsValid()) {
    // Iterate forward until reaching an entry with a larger row idx.
    Slice key_slice, v;
    iter->GetCurrentEntry(&key_slice, &v);
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    DCHECK_EQ(0, key_slice.size()) << "Key should not have leftover data";
    DCHECK_GE(key.row_idx(), row_idx);
    if (key.row_idx() != row_idx) break;

    // Mutation is for the target row, check deletion status.
    RowChangeListDecoder decoder(schema_, RowChangeList(v));
    RETURN_NOT_OK(decoder.Init());
    decoder.TwiddleDeleteStatus(deleted);

    iter->Next();
  }

  return Status::OK();
}

Status DeltaMemStore::AlterSchema(const Schema& schema) {
  if (Count() != 0) {
    return Status::NotSupported("The DeltaMemStore must be empty to alter the schema");
  }

  schema_ = schema;
  return Status::OK();
}

void DeltaMemStore::DebugPrint() const {
  tree_.DebugPrint();
}

////////////////////////////////////////////////////////////
// DMSIterator
////////////////////////////////////////////////////////////

DMSIterator::DMSIterator(const shared_ptr<const DeltaMemStore> &dms,
                         const Schema &projection,
                         const MvccSnapshot &snapshot)
  : dms_(dms),
    mvcc_snapshot_(snapshot),
    iter_(dms->tree_.NewIterator()),
    initted_(false),
    prepared_idx_(0),
    prepared_count_(0),
    prepared_(false),
    seeked_(false),
    prepared_buf_(kPreparedBufInitialCapacity),
    projector_(dms->schema(), projection) {
}

Status DMSIterator::Init() {
  RETURN_NOT_OK(projector_.Init());
  initted_ = true;
  return Status::OK();
}

Status DMSIterator::SeekToOrdinal(rowid_t row_idx) {
  faststring buf;
  DeltaKey key(row_idx, txid_t(0));
  key.EncodeTo(&buf);

  bool exact; /* unused */
  iter_->SeekAtOrAfter(Slice(buf), &exact);
  prepared_idx_ = row_idx;
  prepared_count_ = 0;
  prepared_ = false;
  seeked_ = true;
  return Status::OK();
}

Status DMSIterator::PrepareBatch(size_t nrows) {

  // This current implementation copies the whole batch worth of deltas
  // into a buffer local to this iterator, after filtering out deltas which
  // aren't yet committed in the current MVCC snapshot. The theory behind
  // this approach is the following:

  // Each batch needs to be processed once per column, meaning that unless
  // we make a local copy, we'd have to reset the CBTree iterator back to the
  // start of the batch and re-iterate for each column. CBTree iterators make
  // local copies as they progress in order to shield from concurrent mutation,
  // so with N columns, we'd end up making N copies of the data. Making a local
  // copy here is instead a single copy of the data, so is likely faster.
  CHECK(seeked_);
  DCHECK(initted_) << "must init";
  rowid_t start_row = prepared_idx_ + prepared_count_;
  rowid_t stop_row = start_row + nrows - 1;

  prepared_buf_.clear();

  while (iter_->IsValid()) {
    Slice key_slice, val;
    iter_->GetCurrentEntry(&key_slice, &val);
    DeltaKey key;
    RETURN_NOT_OK(key.DecodeFrom(&key_slice));
    DCHECK_EQ(0, key_slice.size()) << "Key should not have leftover data";
    DCHECK_GE(key.row_idx(), start_row);
    if (key.row_idx() > stop_row) break;

    if (!mvcc_snapshot_.IsCommitted(key.txid())) {
      // The transaction which applied this update is not yet committed
      // in this iterator's MVCC snapshot. Hence, skip it.
      iter_->Next();
      continue;
    }

    uint32_t len = val.size();
    CHECK_LT(len, 256*1024) << "outrageously sized delta: "
                            << "size=" << len
                            << " dump=" << val.ToDebugString(100);

    // TODO: 64-bit rowids
    COMPILE_ASSERT(sizeof(rowid_t) == sizeof(uint32_t), err_non_32bit_rowid);
    prepared_buf_.reserve(prepared_buf_.size() + len +
                          sizeof(key) + sizeof(uint32_t));

    prepared_buf_.append(&key, sizeof(key));
    PutFixed32(&prepared_buf_, len);
    prepared_buf_.append(val.data(), val.size());

    iter_->Next();
  }
  prepared_idx_ = start_row;
  prepared_count_ = nrows;
  prepared_ = true;

  // TODO: this whole process can be made slightly more efficient:
  // the CBTree iterator is already making copies on a per-leaf-node basis
  // and then we copy it again into our own local snapshot. We can probably
  // do away with one of these copies to improve performance, but for now,
  // not bothering, since the main assumption of kudu is a low update rate.

  return Status::OK();
}

Status DMSIterator::ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) {
  DCHECK(prepared_);
  DCHECK_EQ(prepared_count_, dst->nrows());
  Slice src(prepared_buf_);

  // TODO: Handle the "different type" case (adapter_cols_mapping)
  size_t projected_col;
  if (projector_.get_base_col_from_proj_idx(col_to_apply, &projected_col)) {
    // continue applying updates
  } else if (projector_.get_adapter_col_from_proj_idx(col_to_apply, &projected_col)) {
    // TODO: Handle the "different type" case (adapter_cols_mapping)
    LOG(DFATAL) << "Alter type is not implemented yet";
    return Status::NotSupported("Alter type is not implemented yet");
  } else {
    // Column not present in the deltas... skip!
    return Status::OK();
  }

  while (!src.empty()) {
    DeltaKey key;
    RowChangeList changelist;

    RETURN_NOT_OK(DecodeMutation(&src, &key, &changelist));
    uint32_t idx_in_block = key.row_idx() - prepared_idx_;
    RowChangeListDecoder decoder(dms_->schema(), changelist);

    RETURN_NOT_OK_RET(decoder.Init(),
                      CorruptionStatus(string("Unable to decode changelist: ") + s.ToString(),
                                       key.row_idx(), &changelist));

    if (decoder.is_update()) {
      RETURN_NOT_OK_RET(
        decoder.ApplyToOneColumn(idx_in_block, dst, projected_col, dst->arena()),
        CorruptionStatus(string("Unable to apply changelist: ") + s.ToString(),
                         key.row_idx(), &changelist));
    } else if (decoder.is_delete()) {
      // Handled by ApplyDeletes()
    } else {
      LOG(FATAL) << "TODO: unhandled mutation type. " << changelist.ToString(dms_->schema());
    }
  }


  return Status::OK();
}


Status DMSIterator::ApplyDeletes(SelectionVector *sel_vec) {
  // TODO: this shares lots of code with ApplyUpdates
  // probably Prepare() should just separate out the updates into deletes, and then
  // a set of updates for each column.
  DCHECK(prepared_);
  DCHECK_EQ(prepared_count_, sel_vec->nrows());
  Slice src(prepared_buf_);

  while (!src.empty()) {
    DeltaKey key;
    RowChangeList changelist;

    RETURN_NOT_OK(DecodeMutation(&src, &key, &changelist));

    uint32_t idx_in_block = key.row_idx() - prepared_idx_;
    RowChangeListDecoder decoder(dms_->schema(), changelist);

    RETURN_NOT_OK_RET(decoder.Init(),
                      CorruptionStatus(string("Unable to decode changelist: ") + s.ToString(),
                                       key.row_idx(), &changelist));
    if (decoder.is_delete()) {
      sel_vec->SetRowUnselected(idx_in_block);
    }
  }

  return Status::OK();
}


Status DMSIterator::CollectMutations(vector<Mutation *> *dst, Arena *arena) {
  DCHECK(prepared_);
  Slice src(prepared_buf_);

  while (!src.empty()) {
    DeltaKey key;
    RowChangeList changelist;

    RETURN_NOT_OK(DecodeMutation(&src, &key, &changelist));
    uint32_t rel_idx = key.row_idx() - prepared_idx_;

    if (!projector_.is_identity()) {
      RETURN_NOT_OK(RowChangeListDecoder::ProjectUpdate(projector_, changelist, &delta_buf_));
      // The projection resulted in an empty mutation (e.g. update of a removed column)
      if (delta_buf_.size() == 0) continue;
      changelist = RowChangeList(delta_buf_);
    }

    Mutation *mutation = Mutation::CreateInArena(arena, key.txid(), changelist);
    mutation->AppendToList(&dst->at(rel_idx));
  }

  return Status::OK();
}


Status DMSIterator::DecodeMutation(Slice *src, DeltaKey *key, RowChangeList *changelist) const {
  if (src->size() < sizeof(DeltaKey) + sizeof(uint32_t)) {
    return Status::Corruption("Corrupt prepared updates");
  }

  memcpy(key, src->data(), sizeof(*key));
  src->remove_prefix(sizeof(*key));
  rowid_t idx = key->row_idx();
  DCHECK_GE(idx, prepared_idx_);
  DCHECK_LT(idx, prepared_idx_ + prepared_count_);

  uint32_t delta_len = DecodeFixed32(src->data());
  src->remove_prefix(sizeof(uint32_t));

  if (delta_len > src->size()) {
    return CorruptionStatus("Corrupt prepared updates", idx, NULL);
  }
  *changelist = RowChangeList(Slice(src->data(), delta_len));
  src->remove_prefix(delta_len);

  return Status::OK();
}

Status DMSIterator::CorruptionStatus(const string &message, rowid_t row,
                                     const RowChangeList *changelist) const {
  string ret = message;
  StringAppendF(&ret, "[row=%"ROWID_PRINT_FORMAT"", row);
  if (changelist != NULL) {
    ret.append(" CL=");
    ret.append(changelist->ToString(dms_->schema()));
  }
  ret.append("]");

  return Status::Corruption(ret);
}

Status DMSIterator::FilterColumnsAndAppend(const metadata::ColumnIndexes& col_indexes,
                                           vector<DeltaKeyAndUpdate>* out,
                                           Arena* arena) {
  return Status::InvalidArgument("FilterColumsAndAppend() is not supported by DMSIterator");
}

string DMSIterator::ToString() const {
  return "DMSIterator";
}

} // namespace tablet
} // namespace kudu
