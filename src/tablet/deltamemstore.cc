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


Status DeltaMemStore::ApplyUpdates(
  size_t col_idx, uint32_t start_row,
  ColumnBlock *dst) const
{
  DCHECK_EQ(schema_.column(col_idx).type_info().type(),
            dst->type_info().type());

  gscoped_ptr<DMSTreeIter> iter(tree_.NewIterator());

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

    RowChangeListDecoder decoder(schema_, val);
    // TODO: do we need to actually copy to arena here? or can we just refer to the
    // DMS arena?
    Status s = decoder.ApplyToOneColumn(col_idx, dst->cell_ptr(rel_idx), dst->arena());
    if (!s.ok()) {
      LOG(WARNING) << "Unable to decode changelist at row " << decoded_key
                   << ": " << s.ToString();
      return s;
    }

    iter->Next();
  }

  return Status::OK();
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

uint32_t DeltaMemStore::DecodeKey(const Slice &key) const {
  DCHECK_EQ(sizeof(uint32_t), key.size());
  return ntohl(*reinterpret_cast<const uint32_t *>(key.data()));
}


} // namespace tablet
} // namespace kudu
