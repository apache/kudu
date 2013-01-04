// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_DELTAMEMSTORE_H
#define KUDU_TABLET_DELTAMEMSTORE_H

#include <boost/noncopyable.hpp>

#include "common/columnblock.h"
#include "common/schema.h"
#include "tablet/concurrent_btree.h"
#include "tablet/rowdelta.h"
#include "tablet/layer-interfaces.h"
#include "util/memory/arena.h"

namespace kudu {
namespace tablet {


class DeltaFileWriter;

// In-memory storage for data which has been recently updated.
// This essentially tracks a 'diff' per row, which contains the
// modified columns.

class DeltaMemStore : public DeltaTrackerInterface, boost::noncopyable {
public:
  explicit DeltaMemStore(const Schema &schema);

  // Update the given row in the database.
  // Copies the data, as well as any referenced
  // values into this DMS's local arena.
  void Update(uint32_t row_idx, const RowDelta &update);

  // See DeltaTrackerInterface::ApplyUpdates()
  Status ApplyUpdates(size_t col_idx, uint32_t start_row,
                      ColumnBlock *dst) const /* override */;

  size_t Count() const {
    return tree_.count();
  }

  Status FlushToFile(DeltaFileWriter *dfw) const;

private:
  friend class RowDelta;

  const Schema &schema() const {
    return schema_;
  }

  RowDelta DecodeDelta(Slice *val) const;
  uint32_t DecodeKey(const Slice &key) const;

  const Schema schema_;

  typedef btree::CBTree<btree::BTreeTraits> DMSTree;
  typedef btree::CBTreeIterator<btree::BTreeTraits> DMSTreeIter;

  // Concurrent B-Tree storing <key index> -> RowDelta
  // TODO:
  // Performance could be improved by storing the row delta data inline
  // in the leaf nodes, rather than storing pointers 
  DMSTree tree_;

  Arena arena_;
};



} // namespace tablet
} // namespace kudu

#endif
