// Copyright (c) 2013, Cloudera, inc.
// All rights reserved.

#include "tablet/rowset.h"
#include <string>
#include <vector>
#include "common/generic_iterators.h"

namespace kudu { namespace tablet {

DuplicatingRowSet::DuplicatingRowSet(const vector<shared_ptr<RowSet> > &old_rowsets,
                                     const shared_ptr<RowSet> &new_rowset)
  : old_rowsets_(old_rowsets),
    new_rowset_(new_rowset),
    schema_(new_rowset->schema()),
    key_schema_(schema_.CreateKeyProjection()) {
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


Status DuplicatingRowSet::MutateRow(txid_t txid,
                                    const RowSetKeyProbe &probe,
                                    const RowChangeList &update) {
  // Duplicate the update to both the relevant input rowset and the output rowset.
  //
  // It's crucial to do the mutation against the input side first, due to the potential
  // for a race during flush: the output rowset may not yet hold a DELETE which
  // is present in the input rowset. In that case, the UPDATE against the output rowset would
  // succeed whereas it can't be applied to the input rowset. So, we update the input rowset first,
  // and if it succeeds, propagate to the output.

  // First mutate the relevant input rowset.
  bool updated = false;
  BOOST_FOREACH(const shared_ptr<RowSet> &rowset, old_rowsets_) {
    Status s = rowset->MutateRow(txid, probe, update);
    if (s.ok()) {
      updated = true;
      break;
    } else if (!s.IsNotFound()) {
      LOG(ERROR) << "Unable to update key "
                 << schema().CreateKeyProjection().DebugRow(probe.row_key())
                 << " (failed on rowset " << rowset->ToString() << "): "
                 << s.ToString();
      return s;
    }
  }

  if (!updated) {
    return Status::NotFound("not found in any compaction input");
  }

  // If it succeeded there, we also need to mirror into the new rowset.
  Status s = new_rowset_->MutateRow(txid, probe, update);
  if (!s.ok()) {
    LOG(FATAL) << "Updated row in compaction input, but didn't exist in any compaction output: "
               << schema().CreateKeyProjection().DebugRow(probe.row_key());
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

Status DuplicatingRowSet::GetBounds(Slice *min_encoded_key,
                                    Slice *max_encoded_key) const {
  // The range out of the output rowset always spans the full range
  // of the input rowsets, since no new rows can be inserted.
  return new_rowset_->GetBounds(min_encoded_key, max_encoded_key);
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

Status DuplicatingRowSet::DebugDump(vector<string> *lines) {
  int i = 1;
  BOOST_FOREACH(const shared_ptr<RowSet> &rs, old_rowsets_) {
    LOG_STRING(INFO, lines) << "Duplicating rowset input " << ToString() << " "
                            << i << "/" << old_rowsets_.size() << ":";
    RETURN_NOT_OK(rs->DebugDump(lines));
    i++;
  }
  LOG_STRING(INFO, lines) << "Duplicating rowset output " << ToString() << ":";
  RETURN_NOT_OK(new_rowset_->DebugDump(lines));

  return Status::OK();
}

} // namespace tablet
} // namespace kudu
