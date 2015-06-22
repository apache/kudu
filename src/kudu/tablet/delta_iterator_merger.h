// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
// All rights reserved.
#ifndef KUDU_TABLET_DELTA_ITERATOR_MERGER_H
#define KUDU_TABLET_DELTA_ITERATOR_MERGER_H

#include <string>
#include <tr1/memory>
#include <vector>

#include "kudu/tablet/delta_store.h"

namespace kudu {

class ScanSpec;

namespace tablet {

// DeltaIterator that simply combines together other DeltaIterators,
// applying deltas from each in order.
class DeltaIteratorMerger : public DeltaIterator {
 public:
  // Create a new DeltaIterator which combines the deltas from
  // all of the input delta stores.
  //
  // If only one store is input, this will automatically return an unwrapped
  // iterator for greater efficiency.
  static Status Create(
      const std::vector<std::tr1::shared_ptr<DeltaStore> > &stores,
      const Schema* projection,
      const MvccSnapshot &snapshot,
      std::tr1::shared_ptr<DeltaIterator>* out);

  ////////////////////////////////////////////////////////////
  // Implementations of DeltaIterator
  ////////////////////////////////////////////////////////////
  virtual Status Init(ScanSpec *spec) OVERRIDE;
  virtual Status SeekToOrdinal(rowid_t idx) OVERRIDE;
  virtual Status PrepareBatch(size_t nrows, PrepareFlag flag) OVERRIDE;
  virtual Status ApplyUpdates(size_t col_to_apply, ColumnBlock *dst) OVERRIDE;
  virtual Status ApplyDeletes(SelectionVector *sel_vec) OVERRIDE;
  virtual Status CollectMutations(vector<Mutation *> *dst, Arena *arena) OVERRIDE;
  virtual Status FilterColumnIdsAndCollectDeltas(const std::vector<int>& col_ids,
                                                 vector<DeltaKeyAndUpdate>* out,
                                                 Arena* arena) OVERRIDE;
  virtual bool HasNext() OVERRIDE;
  virtual std::string ToString() const OVERRIDE;

 private:
  explicit DeltaIteratorMerger(const vector<std::tr1::shared_ptr<DeltaIterator> > &iters);

  std::vector<std::tr1::shared_ptr<DeltaIterator> > iters_;
};

} // namespace tablet
} // namespace kudu

#endif // KUDU_TABLET_DELTA_ITERATOR_MERGER_H
