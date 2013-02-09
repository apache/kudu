// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_H
#define KUDU_TABLET_TABLET_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>
#include <deque>

#include "common/iterator.h"
#include "common/schema.h"
#include "tablet/memstore.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/percpu_rwlock.h"
#include "util/status.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

using boost::scoped_ptr;
using std::string;
using std::deque;
using std::tr1::shared_ptr;

class Tablet {
public:
  class RowIterator;

  Tablet(const Schema &schema,
         const string &dir);

  // Create a new tablet.
  // This will create the directory for this tablet.
  // After the call, the tablet may be opened with Open().
  // If the directory already exists, returns an IOError
  // Status.
  Status CreateNew();

  // Open an existing tablet.
  Status Open();

  // Insert a new row into the tablet.
  //
  // The provided 'data' slice should have length equivalent to this
  // tablet's Schema.byte_size().
  //
  // After insert, the row and any referred-to memory (eg for strings)
  // have been copied into internal memory, and thus the provided memory
  // buffer may safely be re-used or freed.
  //
  // Returns Status::AlreadyPresent() if an entry with the same key is already
  // present in the tablet.
  // Returns Status::OK unless allocation fails.
  Status Insert(const Slice &data);

  // Update a row in this tablet.
  //
  // If the row does not exist in this tablet, returns
  // Status::NotFound().
  Status UpdateRow(const void *key,
                   const RowDelta &update);

  template <class SmartPointer>
  Status NewRowIterator(const Schema &projection,
                        SmartPointer *iter) const;
  Status Flush();
  Status Compact();

  size_t MemStoreSize() const {
    return memstore_->memory_footprint();
  }

  // Return the current number of layers in the tablet.
  size_t num_layers() const;

  // Attempt to count the total number of rows in the tablet.
  // This is not super-efficient since it must iterate over the
  // memstore in the current implementation.
  Status CountRows(size_t *count) const;

  const Schema &schema() const { return schema_; }

  static string GetLayerPath(const string &tablet_dir, int layer_idx);

private:

  // Capture a set of iterators which, together, reflect all of the data in the tablet.
  //
  // TODO: these are not currently snapshot iterators - the only guarantee is that they
  // are all captured atomically (eg we don't miss an entire layer or something)
  Status CaptureConsistentIterators(const Schema &projection,
                                    deque<shared_ptr<RowIteratorInterface> > *iters) const;

  BloomFilterSizing bloom_sizing() const;

  Schema schema_;
  string dir_;
  shared_ptr<MemStore> memstore_;
  vector<shared_ptr<LayerInterface> > layers_;

  // Lock protecting write access to the components of the tablet (memstore and layers).
  // Shared mode:
  // - Inserters, updaters take this in shared mode during their mutation.
  // - Readers take this in shared mode while capturing their iterators.
  // Exclusive mode:
  // - Flushers take this lock in order to lock out concurrent updates when swapping in
  //   a new memstore.
  //
  // TODO: this could probably done more efficiently with a single atomic swap of a list
  // and an RCU-style quiesce phase, but not worth it for now.
  mutable percpu_rwlock component_lock_;

  size_t next_layer_idx_;

  Env *env_;

  bool open_;
};


// Iterator over materialized rows in the tablet (across memstore and layers)
class Tablet::RowIterator : public RowIteratorInterface, boost::noncopyable {
public:
  virtual Status Init();

  virtual Status SeekAtOrAfter(const Slice &key, bool *exact) {
    return Status::NotSupported("TODO: implement me");
  }

  virtual Status CopyNextRows(size_t *nrows, RowBlock *dst);

  virtual bool HasNext() const;

  string ToString() const {
    return "tablet iterator";
  }

  virtual const Schema &schema() const {
    return projection_;
  }

private:
  friend class Tablet;

  RowIterator(const Tablet &tablet,
              const Schema &projection);

  const Tablet *tablet_;
  const Schema projection_;

  deque<shared_ptr<RowIteratorInterface> > sub_iters_;

  vector<size_t> projection_mapping_;
};

template <class SmartPointer>
inline Status Tablet::NewRowIterator(const Schema &projection,
                                     SmartPointer *iter) const
{
  std::auto_ptr<RowIterator> it(new RowIterator(*this, projection));
  RETURN_NOT_OK(it->Init());
  iter->reset(it.release());
  return Status::OK();
}

} // namespace table
} // namespace kudu

#endif
