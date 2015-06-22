// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.

#include "kudu/tablet/delta_iterator_merger.h"

#include <algorithm>

#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/strcat.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/tablet/deltafile.h"

namespace kudu {
namespace tablet {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using strings::Substitute;

DeltaIteratorMerger::DeltaIteratorMerger(const vector<shared_ptr<DeltaIterator> > &iters)
  : iters_(iters) {
}

Status DeltaIteratorMerger::Init(ScanSpec *spec) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->Init(spec));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::SeekToOrdinal(rowid_t idx) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->SeekToOrdinal(idx));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::PrepareBatch(size_t nrows, PrepareFlag flag) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->PrepareBatch(nrows, flag));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->ApplyUpdates(col_to_apply, dst));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::ApplyDeletes(SelectionVector *sel_vec) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->ApplyDeletes(sel_vec));
  }
  return Status::OK();
}

Status DeltaIteratorMerger::CollectMutations(vector<Mutation *> *dst, Arena *arena) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    RETURN_NOT_OK(iter->CollectMutations(dst, arena));
  }
  // TODO: do we need to do some kind of sorting here to deal with out-of-order
  // timestamps?
  return Status::OK();
}

struct DeltaKeyUpdateComparator {
  bool operator() (const DeltaKeyAndUpdate& a, const DeltaKeyAndUpdate &b) {
    return a.key.CompareTo<REDO>(b.key) < 0;
  }
};

Status DeltaIteratorMerger::FilterColumnIdsAndCollectDeltas(
    const vector<int>& col_ids,
    vector<DeltaKeyAndUpdate>* out,
    Arena* arena) {
  BOOST_FOREACH(const shared_ptr<DeltaIterator>& iter, iters_) {
    RETURN_NOT_OK(iter->FilterColumnIdsAndCollectDeltas(col_ids, out, arena));
  }
  std::sort(out->begin(), out->end(), DeltaKeyUpdateComparator());
  return Status::OK();
}

bool DeltaIteratorMerger::HasNext() {
  BOOST_FOREACH(const shared_ptr<DeltaIterator>& iter, iters_) {
    if (iter->HasNext()) {
      return true;
    }
  }

  return false;
}

string DeltaIteratorMerger::ToString() const {
  string ret;
  ret.append("DeltaIteratorMerger(");

  bool first = true;
  BOOST_FOREACH(const shared_ptr<DeltaIterator> &iter, iters_) {
    if (!first) {
      ret.append(", ");
    }
    first = false;

    ret.append(iter->ToString());
  }
  ret.append(")");
  return ret;
}


Status DeltaIteratorMerger::Create(
    const vector<shared_ptr<DeltaStore> > &stores,
    const Schema* projection,
    const MvccSnapshot &snapshot,
    shared_ptr<DeltaIterator>* out) {
  vector<shared_ptr<DeltaIterator> > delta_iters;

  BOOST_FOREACH(const shared_ptr<DeltaStore> &store, stores) {
    DeltaIterator* raw_iter;
    Status s = store->NewDeltaIterator(projection, snapshot, &raw_iter);
    if (s.IsNotFound()) {
      continue;
    }
    RETURN_NOT_OK_PREPEND(s, Substitute("Could not create iterator for store $0",
                                        store->ToString()));

    delta_iters.push_back(shared_ptr<DeltaIterator>(raw_iter));
  }

  if (delta_iters.size() == 1) {
    // If we only have one input to the "merge", we can just directly
    // return that iterator.
    *out = delta_iters[0];
  } else {
    *out = shared_ptr<DeltaIterator>(new DeltaIteratorMerger(delta_iters));
  }
  return Status::OK();
}

} // namespace tablet
} // namespace kudu
