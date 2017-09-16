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
#ifndef KUDU_UTIL_MEM_TRACKER_H
#define KUDU_UTIL_MEM_TRACKER_H

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/util/high_water_mark.h"
#include "kudu/util/mutex.h"

namespace kudu {

// A MemTracker tracks memory consumption; it contains an optional limit and is
// arranged into a tree structure such that the consumption tracked by a
// MemTracker is also tracked by its ancestors.
//
// The MemTracker hierarchy is rooted in a single static MemTracker.
// The root MemTracker always exists, and it is the common
// ancestor to all MemTrackers. All operations that discover MemTrackers begin
// at the root and work their way down the tree, while operations that deal
// with adjusting memory consumption begin at a particular MemTracker and work
// their way up the tree to the root. All MemTrackers (except the root) must
// have a parent. As a rule, all children belonging to a parent should have
// unique ids, but this is only enforced during a Find() operation to allow for
// transient duplicates (e.g. the web UI grabbing very short-lived references
// to all MemTrackers while rendering a web page). This also means id
// uniqueness only exists where it's actually needed.
//
// When a MemTracker begins its life, it has a strong reference to its parent
// and the parent has a weak reference to it. Both remain for the lifetime of
// the MemTracker.
//
// Memory consumption is tracked via calls to Consume()/Release(), either to
// the tracker itself or to one of its descendants.
//
// This class is thread-safe.
class MemTracker : public std::enable_shared_from_this<MemTracker> {
 public:
  ~MemTracker();

  // Creates and adds the tracker to the tree so that it can be retrieved with
  // FindTracker/FindOrCreateTracker.
  //
  // byte_limit < 0 means no limit; 'id' is a used as a label to uniquely identify
  // the MemTracker for the below Find...() calls as well as the web UI.
  //
  // Use the two-argument form if there is no parent.
  static std::shared_ptr<MemTracker> CreateTracker(
      int64_t byte_limit,
      const std::string& id,
      const std::shared_ptr<MemTracker>& parent = std::shared_ptr<MemTracker>());

  // If a tracker with the specified 'id' and 'parent' exists in the tree, sets
  // 'tracker' to reference that instance. Returns false if no such tracker
  // exists.
  //
  // Use the two-argument form if there is no parent.
  //
  // Note: this function will enforce that 'id' is unique amongst the children
  // of 'parent'.
  static bool FindTracker(
      const std::string& id,
      std::shared_ptr<MemTracker>* tracker,
      const std::shared_ptr<MemTracker>& parent = std::shared_ptr<MemTracker>());

  // If a global tracker with the specified 'id' exists in the tree, returns a
  // shared_ptr to that instance. Otherwise, creates a new MemTracker with the
  // specified byte_limit and id, parented to the root MemTracker.
  //
  // Note: this function will enforce that 'id' is unique amongst the children
  // of the root MemTracker.
  static std::shared_ptr<MemTracker> FindOrCreateGlobalTracker(
      int64_t byte_limit, const std::string& id);

  // Returns a list of all the valid trackers.
  static void ListTrackers(std::vector<std::shared_ptr<MemTracker> >* trackers);

  // Gets a shared_ptr to the "root" tracker, creating it if necessary.
  static std::shared_ptr<MemTracker> GetRootTracker();

  // Increases consumption of this tracker and its ancestors by 'bytes'.
  void Consume(int64_t bytes);

  // Increases consumption of this tracker and its ancestors by 'bytes' only if
  // they can all consume 'bytes'. If this brings any of them over, none of them
  // are updated.
  // Returns true if the try succeeded.
  bool TryConsume(int64_t bytes);

  // Decreases consumption of this tracker and its ancestors by 'bytes'.
  //
  // This will also cause the process to periodically trigger tcmalloc "ReleaseMemory"
  // to ensure that memory is released to the OS.
  void Release(int64_t bytes);

  // Returns true if a valid limit of this tracker or one of its ancestors is
  // exceeded.
  bool AnyLimitExceeded();

  // If this tracker has a limit, checks the limit and attempts to free up some memory if
  // the limit is exceeded by calling any added GC functions. Returns true if the limit is
  // exceeded after calling the GC functions. Returns false if there is no limit.
  bool LimitExceeded() {
    return limit_ >= 0 && limit_ < consumption();
  }

  // Returns the maximum consumption that can be made without exceeding the limit on
  // this tracker or any of its parents. Returns int64_t::max() if there are no
  // limits and a negative value if any limit is already exceeded.
  int64_t SpareCapacity() const;


  int64_t limit() const { return limit_; }
  bool has_limit() const { return limit_ >= 0; }
  const std::string& id() const { return id_; }

  // Returns the memory consumed in bytes.
  int64_t consumption() const {
    return consumption_.current_value();
  }

  int64_t peak_consumption() const { return consumption_.max_value(); }

  // Retrieve the parent tracker, or NULL If one is not set.
  std::shared_ptr<MemTracker> parent() const { return parent_; }

  // Returns a textual representation of the tracker that is likely (but not
  // guaranteed) to be globally unique.
  std::string ToString() const;

 private:
  // byte_limit < 0 means no limit
  // 'id' is the label for LogUsage() and web UI.
  MemTracker(int64_t byte_limit, const std::string& id, std::shared_ptr<MemTracker> parent);

  // Further initializes the tracker.
  void Init();

  // Adds tracker to child_trackers_.
  void AddChildTracker(const std::shared_ptr<MemTracker>& tracker);

  // Variant of FindTracker() that must be called with a non-NULL parent.
  static bool FindTrackerInternal(
      const std::string& id,
      std::shared_ptr<MemTracker>* tracker,
      const std::shared_ptr<MemTracker>& parent);

  // Creates the root tracker.
  static void CreateRootTracker();

  int64_t limit_;
  const std::string id_;
  const std::string descr_;
  std::shared_ptr<MemTracker> parent_;

  HighWaterMark consumption_;

  // this tracker plus all of its ancestors
  std::vector<MemTracker*> all_trackers_;
  // all_trackers_ with valid limits
  std::vector<MemTracker*> limit_trackers_;

  // All the child trackers of this tracker. Used for error reporting and
  // listing only (i.e. updating the consumption of a parent tracker does not
  // update that of its children).
  mutable Mutex child_trackers_lock_;
  std::list<std::weak_ptr<MemTracker>> child_trackers_;

  // Iterator into parent_->child_trackers_ for this object. Stored to have O(1)
  // remove.
  std::list<std::weak_ptr<MemTracker>>::iterator child_tracker_it_;
};

// An std::allocator that manipulates a MemTracker during allocation
// and deallocation.
template<typename T, typename Alloc = std::allocator<T> >
class MemTrackerAllocator : public Alloc {
 public:
  typedef typename Alloc::pointer pointer;
  typedef typename Alloc::const_pointer const_pointer;
  typedef typename Alloc::size_type size_type;

  explicit MemTrackerAllocator(std::shared_ptr<MemTracker> mem_tracker)
      : mem_tracker_(std::move(mem_tracker)) {}

  // This constructor is used for rebinding.
  template <typename U>
  MemTrackerAllocator(const MemTrackerAllocator<U>& allocator)
      : Alloc(allocator),
        mem_tracker_(allocator.mem_tracker()) {
  }

  ~MemTrackerAllocator() {
  }

  pointer allocate(size_type n, const_pointer hint = 0) {
    // Ideally we'd use TryConsume() here to enforce the tracker's limit.
    // However, that means throwing bad_alloc if the limit is exceeded, and
    // it's not clear that the rest of Kudu can handle that.
    mem_tracker_->Consume(n * sizeof(T));
    return Alloc::allocate(n, hint);
  }

  void deallocate(pointer p, size_type n) {
    Alloc::deallocate(p, n);
    mem_tracker_->Release(n * sizeof(T));
  }

  // This allows an allocator<T> to be used for a different type.
  template <class U>
  struct rebind {
    typedef MemTrackerAllocator<U, typename Alloc::template rebind<U>::other> other;
  };

  const std::shared_ptr<MemTracker>& mem_tracker() const { return mem_tracker_; }

 private:
  std::shared_ptr<MemTracker> mem_tracker_;
};

// Convenience class that adds memory consumption to a tracker when declared,
// releasing it when the end of scope is reached.
class ScopedTrackedConsumption {
 public:
  ScopedTrackedConsumption(std::shared_ptr<MemTracker> tracker,
                           int64_t to_consume)
      : tracker_(std::move(tracker)), consumption_(to_consume) {
    DCHECK(tracker_);
    tracker_->Consume(consumption_);
  }

  void Reset(int64_t new_consumption) {
    // Consume(-x) is the same as Release(x).
    tracker_->Consume(new_consumption - consumption_);
    consumption_ = new_consumption;
  }

  ~ScopedTrackedConsumption() {
    tracker_->Release(consumption_);
  }

  int64_t consumption() const { return consumption_; }

 private:
  std::shared_ptr<MemTracker> tracker_;
  int64_t consumption_;
};

} // namespace kudu

#endif // KUDU_UTIL_MEM_TRACKER_H
