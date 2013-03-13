// Copyright (c) 2012, Cloudera, inc.

#include <utility>

#include "tablet/deltafile.h"
#include "tablet/deltamemstore.h"
#include "util/hexdump.h"
#include "util/status.h"

namespace kudu { namespace tablet {

struct EncodedKeySlice : public Slice {
public:
  explicit EncodedKeySlice(uint32_t row_idx) :
    Slice(reinterpret_cast<const uint8_t *>(&buf_int_),
          sizeof(uint32_t)),
    buf_int_(htonl(row_idx))
  {
  }

private:
  uint32_t buf_int_;
};

////////////////////////////////////////////////////////////
// DeltaMemStore implementation
////////////////////////////////////////////////////////////

DeltaMemStore::DeltaMemStore(const Schema &schema) :
  schema_(schema),
  arena_(8*1024, 1*1024*1024)
{
}


void DeltaMemStore::Update(uint32_t row_idx,
                           const RowChangeList &update) {
  EncodedKeySlice key(row_idx);


  btree::PreparedMutation<btree::BTreeTraits> mutation(key);
  mutation.Prepare(&tree_);
  if (mutation.exists()) {
    CHECK(mutation.Update(update.slice()));
  } else {
    CHECK(mutation.Insert(update.slice()));
  }
}

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw) const {
  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  iter->SeekToStart();
  while (iter->IsValid()) {
    Slice key, val;
    iter->GetCurrentEntry(&key, &val);
    uint32_t row_idx = DecodeKey(key);
    dfw->AppendDelta(row_idx, RowChangeList(val));
    iter->Next();
  }
  return Status::OK();
}

DeltaIteratorInterface *DeltaMemStore::NewDeltaIterator(const Schema &projection) {
  return new DMSIterator(shared_from_this(), projection);
}

uint32_t DeltaMemStore::DecodeKey(const Slice &key) {
  DCHECK_EQ(sizeof(uint32_t), key.size());
  return ntohl(*reinterpret_cast<const uint32_t *>(key.data()));
}

////////////////////////////////////////////////////////////
// DMSIterator
////////////////////////////////////////////////////////////

DMSIterator::DMSIterator(const shared_ptr<DeltaMemStore> &dms,
                         const Schema &projection) :
  dms_(dms),
  projection_(projection),
  iter_(dms->tree_.NewIterator()),
  cur_idx_(0),
  prepared_idx_(0),
  prepared_block_(NULL),
  prepared_buf_(kPreparedBufInitialCapacity)
{
}

Status DMSIterator::Init() {
  projection_indexes_.clear();
  RETURN_NOT_OK(projection_.GetProjectionFrom(dms_->schema_, &projection_indexes_));
  return Status::OK();
}

Status DMSIterator::SeekToOrdinal(uint32_t row_idx) {
  EncodedKeySlice start_key(row_idx);
  bool exact; /* unused */
  iter_->SeekAtOrAfter(start_key, &exact);
  cur_idx_ = row_idx;
  prepared_block_ = NULL;
  return Status::OK();
}

Status DMSIterator::PrepareToApply(RowBlock *dst) {
  DCHECK(!projection_indexes_.empty()) << "must init";
  DCHECK(dst->schema().Equals(projection_));
  DCHECK_GT(dst->nrows(), 0);
  uint32_t start_row = cur_idx_;
  uint32_t stop_row = cur_idx_ + dst->nrows() - 1;

  prepared_buf_.clear();

  while (iter_->IsValid()) {
    Slice key, val;
    iter_->GetCurrentEntry(&key, &val);
    size_t row_idx = DeltaMemStore::DecodeKey(key);
    DCHECK_GE(row_idx, start_row);
    if (row_idx > stop_row) break;

    uint32_t len = val.size();
    CHECK_LT(len, 256*1024) << "outrageously sized delta: "
                            << "size=" << len
                            << " dump=" << val.ToDebugString(100);

    prepared_buf_.reserve(prepared_buf_.size() + len + sizeof(uint32_t) * 2);

    PutFixed32(&prepared_buf_, row_idx);
    PutFixed32(&prepared_buf_, len);
    prepared_buf_.append(val.data(), val.size());

    iter_->Next();
  }
  prepared_idx_ = cur_idx_;
  cur_idx_ += dst->nrows();
  prepared_block_ = dst;

  // TODO: this whole process can be made slightly more efficient:
  // the CBTree iterator is already making copies on a per-leaf-node basis
  // and then we copy it again into our own local snapshot. We can probably
  // do away with one of these copies to improve performance, but for now,
  // not bothering, since the main assumption of kudu is a low update rate.

  return Status::OK();
}

Status DMSIterator::ApplyUpdates(RowBlock *dst, size_t col_to_apply) {
  DCHECK_EQ(prepared_block_, dst);
  Slice src(prepared_buf_);

  size_t projected_col = projection_indexes_[col_to_apply];
  ColumnBlock dst_col(dst->column_block(col_to_apply, dst->nrows()));

  while (!src.empty()) {
    if (src.size() < sizeof(uint32_t) * 2) {
      return Status::Corruption("Corrupt prepared updates");
    }
    uint32_t idx = DecodeFixed32(src.data());
    src.remove_prefix(sizeof(uint32_t));
    DCHECK_GE(idx, prepared_idx_);
    DCHECK_LT(idx, prepared_idx_ + dst->nrows());
    uint32_t idx_in_block = idx - prepared_idx_;

    uint32_t delta_len = DecodeFixed32(src.data());
    src.remove_prefix(sizeof(uint32_t));

    if (delta_len > src.size()) {
      return Status::Corruption(StringPrintf("Corrupt prepared updates at row %d", idx));
    }
    Slice delta(src.data(), delta_len);
    src.remove_prefix(delta_len);

    RowChangeListDecoder decoder(dms_->schema(), delta);

    Status s = decoder.ApplyToOneColumn(
      projected_col, dst_col.cell_ptr(idx_in_block), dst->arena());
    if (!s.ok()) {
      return Status::Corruption(
        StringPrintf("Corrupt prepared updates at row %d: ", idx) +
        s.ToString());
    }
  }

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
