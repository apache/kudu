// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_H
#define KUDU_TABLET_TABLET_H

#include <boost/noncopyable.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>

#include "common/generic_iterators.h"
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
using std::tr1::shared_ptr;

class Tablet {
public:
  class CompactionFaultHooks;
  class FlushFaultHooks;

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
                   const RowChangeList &update);

  // Create a new row iterator.
  // The returned iterator is not initialized.
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

  void SetCompactionHooksForTests(const shared_ptr<CompactionFaultHooks> &hooks);
  void SetFlushHooksForTests(const shared_ptr<FlushFaultHooks> &hooks);

private:
  typedef vector<shared_ptr<boost::mutex::scoped_try_lock> > LockVector;

  // Capture a set of iterators which, together, reflect all of the data in the tablet.
  //
  // These iterators are not true snapshot iterators, but they are safe against
  // concurrent modification. They will include all data that was present at the time
  // of creation, and potentially newer data.
  //
  // The returned iterators are not Init()ed
  Status CaptureConsistentIterators(const Schema &projection,
                                    vector<shared_ptr<RowIteratorInterface> > *iters) const;

  Status PickLayersToCompact(
    LayerVector *out_layers,
    LockVector *out_locks) const;

  // Swap out a set of layers, atomically replacing them with the new layer
  // under the lock.
  void AtomicSwapLayers(const LayerVector old_layers,
                        const shared_ptr<LayerInterface> &new_layer);
    

  BloomFilterSizing bloom_sizing() const;

  Schema schema_;
  string dir_;
  shared_ptr<MemStore> memstore_;
  LayerVector layers_;

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

  // Fault hooks. In production code, these will always be NULL.
  shared_ptr<CompactionFaultHooks> compaction_hooks_;
  shared_ptr<FlushFaultHooks> flush_hooks_;
};


// Hooks used in test code to inject faults or other code into interesting
// parts of the compaction code.
class Tablet::CompactionFaultHooks {
public:
  virtual Status PreCompaction() { return Status::OK(); }
  virtual Status PostSelectIterators() { return Status::OK(); }
  virtual Status PostMergeKeys() { return Status::OK(); }
  virtual Status PostMergeNonKeys() { return Status::OK(); }
  virtual Status PostRenameFile() { return Status::OK(); }
  virtual Status PostSwapNewLayer() { return Status::OK(); }
  virtual Status PostCompaction() { return Status::OK(); }
};

// Hooks used in test code to inject faults or other code into interesting
// parts of the Flush() code.
class Tablet::FlushFaultHooks {
public:
  virtual Status PreFlush() { return Status::OK(); }
  virtual Status PostSwapNewMemStore() { return Status::OK(); }
  virtual Status PostFlushKeys() { return Status::OK(); }
  virtual Status PostFreezeOldMemStore() { return Status::OK(); }
  virtual Status PostOpenNewLayer() { return Status::OK(); }
};


template <class SmartPointer>
inline Status Tablet::NewRowIterator(const Schema &projection,
                                     SmartPointer *iter) const
{
  vector<shared_ptr<RowIteratorInterface> > iters;
  RETURN_NOT_OK(CaptureConsistentIterators(projection, &iters));

  iter->reset(new UnionIterator(iters));

  return Status::OK();
}

} // namespace table
} // namespace kudu

#endif
