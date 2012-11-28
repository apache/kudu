// Copyright (c) 2012, Cloudera, inc.
#ifndef KUDU_TABLET_TABLET_H
#define KUDU_TABLET_TABLET_H

#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_deque.hpp>
#include <boost/scoped_ptr.hpp>
#include <string>

#include "common/schema.h"
#include "tablet/memstore.h"
#include "tablet/layer.h"
#include "util/env.h"
#include "util/status.h"
#include "util/slice.h"

namespace kudu { namespace tablet {

using boost::ptr_deque;
using boost::scoped_ptr;
using std::string;

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

  template <class SmartPointer>
  Status NewRowIterator(const Schema &projection,
                        SmartPointer *iter) const;
  Status Flush();

  const Schema &schema() const { return schema_; }

private:

  // Capture a set of iterators which, together, reflect all of the data in the tablet.
  //
  // TODO: these are not currently snapshot iterators - the only guarantee is that they
  // are all captured atomically (eg we don't miss an entire layer or something)
  Status CaptureConsistentIterators(const Schema &projection,
                                    ptr_deque<MemStore::Iterator> *ms_iters_,
                                    ptr_deque<Layer::RowIterator> *layer_iters) const;

  Schema schema_;
  string dir_;
  scoped_ptr<MemStore> memstore_;
  ptr_vector<Layer> layers_;

  size_t next_layer_idx_;

  Env *env_;

  bool open_;
};


// Iterator over materialized rows in the tablet (across memstore and layers)
class Tablet::RowIterator : boost::noncopyable {
public:

  // TODO: this probably needs something like 'class RowBlock' to correspond to
  // class ColumnBlock
  Status CopyNextRows(size_t *nrows,
                      void *dst,
                      Arena *dst_arena);

private:
  friend class Tablet;

  RowIterator(const Tablet &tablet,
              const Schema &projection);

  Status Init();

  Status CopyNextFromMemStore(size_t *nrows,
                              void *dst,
                              Arena *dst_arena);
  Status CopyNextFromLayers(size_t *nrows,
                            void *dst,
                            Arena *dst_arena);

  const Tablet *tablet_;
  const Schema projection_;

  ptr_deque<MemStore::Iterator> memstore_iters_;
  ptr_deque<Layer::RowIterator> layer_iters_;

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
