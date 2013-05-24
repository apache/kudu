// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "common/generic_iterators.h"
#include "tablet/rowset.h"

namespace kudu { namespace tablet {

DuplicatingRowSet::DuplicatingRowSet(const vector<shared_ptr<RowSet> > &old_rowsets,
                                   const shared_ptr<RowSet> &new_rowset) :
  old_rowsets_(old_rowsets),
  new_rowset_(new_rowset)
{
  CHECK_GT(old_rowsets_.size(), 0);
  always_locked_.lock();
}

DuplicatingRowSet::~DuplicatingRowSet() {
  always_locked_.unlock();
}

string DuplicatingRowSet::ToString() const {
  string ret;
  ret.append("DuplicatingRowSet([");
  bool first = true;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, old_rowsets_) {
    if (!first) {
      ret.append(", ");
    }
    first = false;
    ret.append(rs->ToString());
  }
  ret.append("] + ");
  ret.append(new_rowset_->ToString());
  ret.append(")");
  return ret;
}

RowwiseIterator *DuplicatingRowSet::NewRowIterator(const Schema &projection,
                                                  const MvccSnapshot &snap) const {
  // Use the original rowset.
  if (old_rowsets_.size() == 1) {
    return old_rowsets_[0]->NewRowIterator(projection, snap);
  } else {
    // Union between them

    vector<shared_ptr<RowwiseIterator> > iters;
    BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
      shared_ptr<RowwiseIterator> iter(rowset->NewRowIterator(projection, snap));
      iters.push_back(iter);
    }

    return new UnionIterator(iters);
  }
}

CompactionInput *DuplicatingRowSet::NewCompactionInput(const MvccSnapshot &snap) const  {
  LOG(FATAL) << "duplicating rowsets do not act as compaction input";
  return NULL;
}


Status DuplicatingRowSet::UpdateRow(txid_t txid,
                                       const void *key,
                                       const RowChangeList &update) {
  ConstContiguousRow row_key(schema(), key);

  // First update the new rowset
  RETURN_NOT_OK(new_rowset_->UpdateRow(txid, key, update));

  // If it succeeded there, we also need to mirror into the old rowset.
  // Duplicate the update to both the relevant input rowset and the output rowset.
  // First propagate to the relevant input rowset.
  bool updated = false;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
    Status s = rowset->UpdateRow(txid, key, update);
    if (s.ok()) {
      updated = true;
      break;
    } else if (!s.IsNotFound()) {
      LOG(ERROR) << "Unable to update key "
                 << schema().CreateKeyProjection().DebugRow(row_key)
                 << " (failed on rowset " << rowset->ToString() << "): "
                 << s.ToString();
      return s;
    }
  }

  if (!updated) {
    LOG(DFATAL) << "Found row in compaction output but not in any input rowset: "
                << schema().CreateKeyProjection().DebugRow(row_key);
    return Status::NotFound("not found in any input rowset of compaction");
  }

  return Status::OK();
}

Status DuplicatingRowSet::CheckRowPresent(const RowSetKeyProbe &probe,
                              bool *present) const {
  *present = false;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
    RETURN_NOT_OK(rowset->CheckRowPresent(probe, present));
    if (present) {
      return Status::OK();
    }
  }
  return Status::OK();
}

Status DuplicatingRowSet::CountRows(rowid_t *count) const {
  return new_rowset_->CountRows(count);
}

uint64_t DuplicatingRowSet::EstimateOnDiskSize() const {
  // The actual value of this doesn't matter, since it won't be selected
  // for compaction.
  return new_rowset_->EstimateOnDiskSize();
}

Status DuplicatingRowSet::Delete() {
  LOG(FATAL) << "Unsupported op";
  return Status::NotSupported("");
}


} // namespace tablet
} // namespace kudu
