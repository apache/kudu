// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef KUDU_TABLET_MUTATION_H
#define KUDU_TABLET_MUTATION_H

#include <cstdint>
#include <cstring>
#include <new>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include "kudu/common/row_changelist.h"
#include "kudu/common/timestamp.h"
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/util/slice.h"

namespace kudu {

class Schema;

namespace tablet {

// A single mutation associated with a row.
// This object also acts as a node in a linked list connected to other
// mutations in the row.
//
// This is a variable-length object.
class Mutation {
 public:
  Mutation() { }

  // Create a new Mutation object with a copy of the given changelist.
  // The object is allocated from the provided Arena.
  template<class ArenaType>
  static Mutation *CreateInArena(
    ArenaType *arena, Timestamp timestamp, const RowChangeList &rcl);

  RowChangeList changelist() const {
    return RowChangeList(Slice(changelist_data_, changelist_size_));
  }

  Timestamp timestamp() const { return timestamp_; }

  Mutation *next() { return next_; }
  const Mutation *next() const { return next_; }

  // Same as 'next()' but loads with 'Acquire' ordering semantics.
  // This must be used when traversing the mutation list associated with
  // an in-memory store.
  const Mutation* acquire_next() const {
    return reinterpret_cast<const Mutation*>(base::subtle::Acquire_Load(
        reinterpret_cast<const AtomicWord*>(&next_)));
  }

  void set_next(Mutation *next) {
    next_ = next;
  }

  // Return a stringified version of the given list of mutations.
  // This should only be used for debugging/logging.
  static std::string StringifyMutationList(const Schema &schema, const Mutation *head);

  // Appends this mutation to the list given by 'redo_head' and 'redo_tail'.
  //
  // This function is atomic provided that callers are externally synchronized
  // on a per mutation list (i.e. per row) basis. Without this synchronization,
  // the non-atomicity between 'redo_head' and 'redo_tail' writes may cause errors.
  //
  // This operation uses "Release" memory semantics (see atomicops.h). The
  // pointers as well as all of the mutations in the list must be word-aligned.
  void AppendToListAtomic(Mutation** redo_head, Mutation** redo_tail);

  void PrependToList(Mutation** list) {
    this->next_ = *list;
    *list = this;
  }

  // O(n) algorithm to reverse the order of a linked list of mutations.
  static void ReverseMutationList(Mutation** list);

 private:
  friend class MSRow;
  friend class MemRowSet;

  // The op timestamp which made this mutation. If this op is not committed in
  // the snapshot of the reader, this mutation should be ignored.
  Timestamp timestamp_;

  // Link to the next mutation on this row
  Mutation *next_;

  uint32_t changelist_size_;

  // The actual encoded RowChangeList
  char changelist_data_[0];

  DISALLOW_COPY_AND_ASSIGN(Mutation);
};

template<class ArenaType>
inline Mutation *Mutation::CreateInArena(
  ArenaType *arena, Timestamp timestamp, const RowChangeList &rcl) {
  DCHECK(!rcl.is_null());

  size_t size = sizeof(Mutation) + rcl.slice().size();
  void *storage = arena->AllocateBytesAligned(size, BASE_PORT_H_ALIGN_OF(Mutation));
  CHECK(storage) << "failed to allocate storage from arena";
  auto ret = new (storage) Mutation();
  ret->timestamp_ = timestamp;
  ret->next_ = NULL;
  ret->changelist_size_ = rcl.slice().size();
  memcpy(ret->changelist_data_, rcl.slice().data(), rcl.slice().size());
  return ret;
}

inline void Mutation::ReverseMutationList(Mutation** list) {
  Mutation* prev = nullptr;
  Mutation* cur = *list;
  while (cur != nullptr) {
    Mutation* next = cur->next_;
    cur->next_ = prev;
    prev = cur;
    cur = next;
  }
  *list = prev;
}

} // namespace tablet
} // namespace kudu

#endif
