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

#include <algorithm> // IWYU pragma: keep
#include <map>
#include <memory>
#include <ostream>

#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/rwc_lock.h"

namespace kudu {

// An object which manages its state via copy-on-write.
//
// Access to this object can be done more conveniently using the
// CowLock template class defined below.
//
// The 'State' template parameter must be swappable using std::swap.
template<class State>
class CowObject {
 public:
  CowObject() {}
  ~CowObject() {}

  // Lock an object for read.
  //
  // While locked, a mutator will be blocked when trying to commit its mutation.
  void ReadLock() const {
    lock_.ReadLock();
  }

  // Return whether the object is locked for read.
  bool IsReadLocked() const {
    return lock_.HasReaders();
  }

  // Unlock an object previously locked for read, unblocking a mutator
  // actively trying to commit its mutation.
  void ReadUnlock() const {
    lock_.ReadUnlock();
  }

  // Lock the object for write (preventing concurrent mutators).
  //
  // We defer making a dirty copy of the state to mutable_dirty() so that the
  // copy can be avoided if no dirty changes are actually made.
  void StartMutation() {
    lock_.WriteLock();
  }

  // Return whether the object is locked for read and write.
  bool IsWriteLocked() const {
    return lock_.HasWriteLock();
  }

  // Abort the current mutation. This drops the write lock without applying any
  // changes made to the mutable copy.
  void AbortMutation() {
    DCHECK(lock_.HasWriteLock());
    dirty_state_.reset();
    lock_.WriteUnlock();
  }

  // Commit the current mutation. This escalates to the "Commit" lock, which
  // blocks any concurrent readers or writers, swaps in the new version of the
  // State, and then drops the commit lock.
  void CommitMutation() {
    DCHECK(lock_.HasWriteLock());
    if (!dirty_state_) {
      AbortMutation();
      return;
    }
    lock_.UpgradeToCommitLock();
    std::swap(state_, *dirty_state_);
    dirty_state_.reset();
    lock_.CommitUnlock();
  }

  // Return the current state, not reflecting any in-progress mutations.
  State& state() {
    DCHECK(lock_.HasReaders() || lock_.HasWriteLock());
    return state_;
  }

  const State& state() const {
    DCHECK(lock_.HasReaders() || lock_.HasWriteLock());
    return state_;
  }

  // Returns the current dirty state (i.e reflecting in-progress mutations).
  // Should only be called by a thread who previously called StartMutation().
  State* mutable_dirty() {
    DCHECK(lock_.HasWriteLock());
    if (!dirty_state_) {
      dirty_state_.reset(new State(state_));
    }
    return dirty_state_.get();
  }

  const State& dirty() const {
    DCHECK(lock_.HasWriteLock());
    if (!dirty_state_) {
      return state_;
    }
    return *dirty_state_.get();
  }

 private:
  mutable RWCLock lock_;

  State state_;
  std::unique_ptr<State> dirty_state_;

  DISALLOW_COPY_AND_ASSIGN(CowObject);
};

// Lock state for the following lock-guard-like classes.
enum class LockMode {
  // The lock is held for reading.
  READ,

  // The lock is held for reading and writing.
  WRITE,

  // The lock is not held.
  RELEASED
};

// Defined so LockMode is compatible with DCHECK and the like.
std::ostream& operator<<(std::ostream& o, LockMode m);

// A lock-guard-like scoped object to acquire the lock on a CowObject,
// and obtain a pointer to the correct copy to read/write.
//
// Example usage:
//
//   CowObject<Foo> my_obj;
//   {
//     CowLock<Foo> l(&my_obj, LockMode::READ);
//     l.data().get_foo();
//     ...
//   }
//   {
//     CowLock<Foo> l(&my_obj, LockMode::WRITE);
//     l->mutable_data()->set_foo(...);
//     ...
//     l.Commit();
//   }
template<class State>
class CowLock {
 public:

   // An unlocked CowLock. This is useful for default constructing a lock to be
   // moved in to.
   CowLock()
    : cow_(nullptr),
      mode_(LockMode::RELEASED) {
   }

  // Lock in either read or write mode.
  CowLock(CowObject<State>* cow,
          LockMode mode)
    : cow_(cow),
      mode_(mode) {
    switch (mode) {
      case LockMode::READ: cow_->ReadLock(); break;
      case LockMode::WRITE: cow_->StartMutation(); break;
      default: LOG(FATAL) << "Cannot lock in mode " << mode;
    }
  }

  // Lock in read mode.
  // A const object may not be locked in write mode.
  CowLock(const CowObject<State>* info,
          LockMode mode)
    : cow_(const_cast<CowObject<State>*>(info)),
      mode_(mode) {
    switch (mode) {
      case LockMode::READ: cow_->ReadLock(); break;
      case LockMode::WRITE: LOG(FATAL) << "Cannot write-lock a const pointer";
      default: LOG(FATAL) << "Cannot lock in mode " << mode;
    }
  }

  // Disable copying.
  CowLock(const CowLock&) = delete;
  CowLock& operator=(const CowLock&) = delete;

  // Allow moving.
  CowLock(CowLock&& other) noexcept
    : cow_(other.cow_),
      mode_(other.mode_) {
    other.cow_ = nullptr;
    other.mode_ = LockMode::RELEASED;
  }
  CowLock& operator=(CowLock&& other) noexcept {
    cow_ = other.cow_;
    mode_ = other.mode_;
    other.cow_ = nullptr;
    other.mode_ = LockMode::RELEASED;
    return *this;
  }

  // Commit the underlying object.
  // Requires that the caller hold the lock in write mode.
  void Commit() {
    DCHECK_EQ(LockMode::WRITE, mode_);
    cow_->CommitMutation();
    mode_ = LockMode::RELEASED;
  }

  void Unlock() {
    switch (mode_) {
      case LockMode::READ: cow_->ReadUnlock(); break;
      case LockMode::WRITE: cow_->AbortMutation(); break;
      default: DCHECK_EQ(LockMode::RELEASED, mode_); break;
    }
    mode_ = LockMode::RELEASED;
  }

  // Obtain the underlying data. In WRITE mode, this returns the
  // same data as mutable_data() (not the safe unchanging copy).
  const State& data() const {
    switch (mode_) {
      case LockMode::READ: return cow_->state();
      case LockMode::WRITE: return cow_->dirty();
      default: LOG(FATAL) << "Cannot access data after committing";
    }
  }

  // Obtain the mutable data. This may only be called in WRITE mode.
  State* mutable_data() {
    switch (mode_) {
      case LockMode::READ: LOG(FATAL) << "Cannot mutate data with READ lock";
      case LockMode::WRITE: return cow_->mutable_dirty();
      default: LOG(FATAL) << "Cannot access data after committing";
    }
  }

  bool is_write_locked() const {
    return mode_ == LockMode::WRITE;
  }

  // Drop the lock. If the lock is held in WRITE mode, and the
  // lock has not yet been released, aborts the mutation, restoring
  // the underlying object to its original data.
  ~CowLock() {
    Unlock();
  }

 private:
  CowObject<State>* cow_;
  LockMode mode_;
};

// Scoped object that locks multiple CowObjects for reading or for writing.
// When locked for writing and mutations are completed, can also commit those
// mutations, which releases the lock.
//
// CowObjects are stored in an std::map, which provides two important properties:
// 1. AddObject() can deduplicate CowObjects already inserted.
// 2. When locking for writing, the deterministic iteration order provided by
//    std::map prevents deadlocks.
//
// The use of std::map forces callers to provide a key for each CowObject. For
// a key implementation to be usable, an appropriate overload of operator<
// must be available.
//
// Unlike CowLock, does not mediate access to the CowObject data itself;
// callers should access the data out of band.
//
// Sample usage:
//
//   struct Foo {
//     string id_;
//     string data_;
//   };
//
//   vector<CowObject<Foo>> foos;
//
// 1. Locking a group of CowObjects for reading:
//
//   CowGroupLock<string, Foo> l(LockMode::RELEASED);
//   for (const auto& f : foos) {
//     l.AddObject(f.id_, f);
//   }
//   l.Lock(LockMode::READ);
//   for (const auto& f : foos) {
//     cout << f.state().data_ << endl;
//   }
//   l.Unlock();
//
// 2. Tracking already-write-locked CowObjects for group commit:
//
//   CowGroupLock<string, Foo> l(LockMode::WRITE);
//   for (const auto& f : foos) {
//     l.AddObject(f.id_, f);
//     f.mutable_dirty().data_ = "modified";
//   }
//   l.Commit();
//
// 3. Aggregating unlocked CowObjects, locking them safely, and committing them together:
//
//   CowGroupLock<string, Foo> l(LockMode::RELEASED);
//   for (const auto& f : foos) {
//     l.AddObject(f.id_, f);
//   }
//   l.Lock(LockMode::WRITE);
//   for (const auto& f : foos) {
//     f.mutable_dirty().data_ = "modified";
//   }
//   l.Commit();
template<class Key, class Value>
class CowGroupLock {
 public:
  explicit CowGroupLock(LockMode mode)
    : mode_(mode) {
  }

  ~CowGroupLock() {
    Unlock();
  }

  void Unlock() {
    switch (mode_) {
      case LockMode::READ:
        for (const auto& e : cows_) {
          e.second->ReadUnlock();
        }
        break;
      case LockMode::WRITE:
        for (const auto& e : cows_) {
          e.second->AbortMutation();
        }
        break;
      default:
        DCHECK_EQ(LockMode::RELEASED, mode_);
        break;
    }

    cows_.clear();
    mode_ = LockMode::RELEASED;
  }

  void Lock(LockMode new_mode) {
    DCHECK_EQ(LockMode::RELEASED, mode_);

    switch (new_mode) {
      case LockMode::READ:
        for (const auto& e : cows_) {
          e.second->ReadLock();
        }
        break;
      case LockMode::WRITE:
        for (const auto& e : cows_) {
          e.second->StartMutation();
        }
        break;
      default:
        LOG(FATAL) << "Cannot lock in mode " << new_mode;
    }
    mode_ = new_mode;
  }

  void Commit() {
    DCHECK_EQ(LockMode::WRITE, mode_);
    for (const auto& e : cows_) {
      e.second->CommitMutation();
    }
    cows_.clear();
    mode_ = LockMode::RELEASED;
  }

  // Adds a new CowObject to be tracked by the lock guard. Does nothing if a
  // CowObject with the same key was already added.
  //
  // It is the responsibility of the caller to ensure:
  // 1. That 'object' remains alive until the lock is released.
  // 2. That if 'object' was already added, both objects point to the same
  //    memory address.
  // 3. That if the CowGroupLock is already locked in a particular mode,
  //    'object' is also already locked in that mode.
  void AddObject(Key key, const CowObject<Value>* object) {
    AssertObjectLocked(object);
    auto r = cows_.emplace(std::move(key), const_cast<CowObject<Value>*>(object));
    DCHECK_EQ(r.first->second, object);
  }

  // Like the above, but for mutable objects.
  void AddMutableObject(Key key, CowObject<Value>* object) {
    AssertObjectLocked(object);
    auto r = cows_.emplace(std::move(key), object);
    DCHECK_EQ(r.first->second, object);
  }

 private:
  void AssertObjectLocked(const CowObject<Value>* object) const {
#ifndef NDEBUG
    switch (mode_) {
      case LockMode::READ:
        DCHECK(object->IsReadLocked());
        break;
      case LockMode::WRITE:
        DCHECK(object->IsWriteLocked());
        break;
      default:
        DCHECK_EQ(LockMode::RELEASED, mode_);
        break;
    }
#endif
  }

  std::map<Key, CowObject<Value>*> cows_;
  LockMode mode_;

  DISALLOW_COPY_AND_ASSIGN(CowGroupLock);
};

} // namespace kudu
