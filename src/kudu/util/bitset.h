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
#pragma once

#include <bitset>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <utility>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"

// Utility template for working with a bitset to make it feel more like a
// container. E.g., this is useful for building containers for enum types,
// instead of using a hashed container.
//
// This supports operating on 'IntType' types with values ranging from [0,
// MaxVals). The underlying bitset is of size 'MaxVals', which must be known at
// compile time.
//
// Under the hood, std::bitset uses word-sized bitwise instructions on
// stack-allocated bytes, so the expectation is that 'MaxVals' is not many
// times larger than the word-size. A size limit of 64 bits is enforced at
// compile time.
template <typename IntType, size_t MaxVals>
class FixedBitSet {
 public:
  // These types are exposed to match those provided by STL containers, which
  // allows template instantiations to be used by map utility functions.
  class iterator;
  typedef IntType key_type;
  typedef IntType value_type;

  // Constructs an empty FixedBitSet.
  FixedBitSet() {}

  // Constructs a new FixedBitSet from an initializer list.
  FixedBitSet(std::initializer_list<IntType> list) {
    for (const IntType& val : list) {
      insert(val);
    }
  }

  // Constructs a new FixedBitSet from a container.
  template <typename Container>
  explicit FixedBitSet(const Container& c) {
    for (const IntType& val : c) {
      insert(val);
    }
  }

  // Inserts 'val' into the set.
  std::pair<iterator, bool> insert(const IntType& val) {
    DCHECK_LT(val, MaxVals);
    bool not_present = !contains(val);
    if (not_present) {
      bitset_.set(static_cast<size_t>(val));
    }
    return { iterator(this, static_cast<int>(val)), not_present };
  }

  // Removes 'val' from the set if it exists.
  size_t erase(const IntType val) {
    DCHECK_LT(val, MaxVals);
    bool not_present = !contains(val);
    if (not_present) {
      return 0;
    }
    bitset_.set(static_cast<size_t>(val), false);
    return 1;
  }

  // Returns whether 'val' exists in the set.
  bool contains(const IntType& val) const {
    DCHECK_LT(val, MaxVals);
    return bitset_.test(val);
  }

  // Returns whether the set is empty.
  bool empty() const {
    return bitset_.none();
  }

  // Clears the contents of the set.
  void clear() {
    bitset_.reset();
  }

  // Returns the number of set bits.
  size_t size() const {
    return bitset_.count();
  }

  // Resets the set to have the contents of the container of int-typed values.
  template <typename C>
  void reset(const C& container) {
    clear();
    for (const IntType& item : container) {
      insert(item);
    }
  }

  // Forward iterator that points to the start of the values in the bitset.
  // Points at the first set bit, or at end() if no bits are set.
  iterator begin() const {
    iterator iter(this, -1);
    iter.seek_forward();
    return iter;
  }

  // Forward iterator that points to the end of the values in the bitset.
  iterator end() const {
    return iterator(this, MaxVals);
  }

  // Forward iterator that points at the element 'val' if it exists, or at
  // end() if it doesn't exist.
  iterator find(const IntType& val) const {
    return contains(val) ? iterator(this, val) : end();
  }

 private:
  COMPILE_ASSERT(MaxVals < 64, bitset_size_too_large);
  std::bitset<MaxVals> bitset_;
};

// Forward iterator class for a FixedBitSet.
template <typename IntType, size_t MaxVals>
class FixedBitSet<IntType, MaxVals>::iterator :
    public std::iterator<std::forward_iterator_tag, IntType> {
 public:
  // Returns the value currently pointed at by this iterator.
  IntType operator*() {
    return static_cast<IntType>(idx_);
  }

  // Prefix increment operator. Advances the iterator to the next position and
  // returns it.
  iterator& operator++() {
    seek_forward();
    return *this;
  }

  // Postfix increment operator. Advances the iterator to the next position,
  // returning a non-iterated iterator.
  iterator operator++(int) {
    iterator iter_copy = *this;
    seek_forward();
    return iter_copy;
  }

  // Returns whether this iterator is the same as 'other'.
  bool operator==(const iterator& other) const {
    return (idx_ == other.idx_) && (fbs_ == other.fbs_);
  }

  // Returns whether this iterator is not the same as 'other'.
  bool operator!=(const iterator& other) const {
    return !(*this == other);
  }

 private:
  friend class FixedBitSet<IntType, MaxVals>;
  iterator(const FixedBitSet<IntType, MaxVals>* fbs, int idx)
      : fbs_(fbs),
        idx_(idx) {}

  // Seeks forward to the next set bit, or until at the end of the iterator.
  void seek_forward() {
    if (idx_ == MaxVals) {
      return;
    }
    while (++idx_ < MaxVals) {
      if (fbs_->contains(static_cast<IntType>(idx_))) {
        break;
      }
    }
  }

  // The underlying FixedBitSet that this iterator is iterating over.
  const FixedBitSet<IntType, MaxVals>* const fbs_;

  // This iterator's current position.
  int idx_;
};

