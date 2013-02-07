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
                           const RowDelta &update) {
  EncodedKeySlice key(row_idx);

  btree::PreparedMutation<btree::BTreeTraits> mutation(key);
  mutation.Prepare(&tree_);
  if (mutation.exists()) {
    // This row was already updated - just merge the new updates
    // in with the old.
    Slice cur_val = mutation.current_mutable_value();

    RowDelta cur_delta = DecodeDelta(&cur_val);
    cur_delta.MergeUpdatesFrom(schema_, update, &arena_);
  } else {
    // This row hasn't been updated. Create a new delta for it.
    // Copy the update into the arena.
    // TODO: no need to copy the update - just need to copy the
    // referred-to data!
    RowDelta copied = update.CopyToArena(schema_, &arena_);
    Slice new_val(reinterpret_cast<uint8_t *>(&copied), sizeof(copied));
    CHECK(mutation.Insert(new_val));
  }
}


Status DeltaMemStore::ApplyUpdates(
  size_t col_idx, uint32_t start_row,
  ColumnBlock *dst) const
{
  DCHECK_EQ(schema_.column(col_idx).type_info().type(),
            dst->type_info().type());

  scoped_ptr<DMSTreeIter> iter(tree_.NewIterator());

  EncodedKeySlice start_key(start_row);

  bool exact;
  if (!iter->SeekAtOrAfter(start_key, &exact)) {
    // No updates matching this row or higher
    return Status::OK();
  }

  while (iter->IsValid()) {
    Slice key, val;
    iter->GetCurrentEntry(&key, &val);
    uint32_t decoded_key = DecodeKey(key);
    DCHECK_GE(decoded_key, start_row);
    if (decoded_key >= start_row + dst->size()) {
      break;
    }

    uint32_t rel_idx = decoded_key - start_row;

    RowDelta delta = DecodeDelta(&val);
    delta.ApplyColumnUpdate(schema_, col_idx,
                            dst->cell_ptr(rel_idx));
    iter->Next();
  }

  return Status::OK();
}

Status DeltaMemStore::FlushToFile(DeltaFileWriter *dfw) const {
  scoped_ptr<DMSTreeIter> iter(tree_.NewIterator());
  iter->SeekToStart();
  while (iter->IsValid()) {
    Slice key, val;
    iter->GetCurrentEntry(&key, &val);
    uint32_t row_idx = DecodeKey(key);
    RowDelta delta = DecodeDelta(&val);
    dfw->AppendDelta(row_idx, delta);
    iter->Next();
  }
  return Status::OK();
}

uint32_t DeltaMemStore::DecodeKey(const Slice &key) const {
  DCHECK_EQ(sizeof(uint32_t), key.size());
  return ntohl(*reinterpret_cast<const uint32_t *>(key.data()));
}

RowDelta DeltaMemStore::DecodeDelta(Slice *val) const {
  DCHECK_EQ(sizeof(RowDelta), val->size());
  return *reinterpret_cast<RowDelta *>(val->mutable_data());
}



} // namespace tablet
} // namespace kudu
