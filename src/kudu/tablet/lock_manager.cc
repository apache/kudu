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

#include "kudu/tablet/lock_manager.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/dynamic_annotations.h"
#include "kudu/gutil/hash/city.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/array_view.h"
#include "kudu/util/faststring.h"
#include "kudu/util/locks.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/semaphore.h"
#include "kudu/util/trace.h"

using std::string;
using std::unique_lock;
using std::unique_ptr;
using std::vector;

namespace kudu {
namespace tablet {

class OpState;

// ============================================================================
//  LockTable
// ============================================================================

// The entry returned to a thread which has taken a lock.
// Callers should generally use ScopedRowLock (see below).
class LockEntry {
 public:
  explicit LockEntry(const Slice& key)
  : sem(1),
    recursion_(0) {
    key_hash_ = util_hash::CityHash64(reinterpret_cast<const char *>(key.data()), key.size());
    key_ = key;
    refs_ = 1;
  }

  bool Equals(const Slice& key, uint64_t hash) const {
    return key_hash_ == hash && key_ == key;
  }

  string ToString() const {
    return KUDU_REDACT(key_.ToDebugString());
  }

  void Unlock();

  // Mutex used by the LockManager
  Semaphore sem;
  int recursion_;

 private:
  friend class LockTable;
  friend class LockManager;

  void CopyKey() {
    key_buf_.assign_copy(key_.data(), key_.size());
    key_ = Slice(key_buf_);
  }

  // Pointer to the next entry in the same hash table bucket
  LockEntry *ht_next_;

  // Hash of the key, used to lookup the hash table bucket
  uint64_t key_hash_;

  // key of the entry, used to compare the entries
  Slice key_;

  // number of users that are referencing this object
  uint64_t refs_;

  // buffer of the key, allocated on insertion by CopyKey()
  faststring key_buf_;

  // The op currently holding the lock
  const OpState* holder_;
};

class LockTable {
 private:
  struct Bucket {
    // First entry chained from this bucket, or NULL if the bucket is empty.
    LockEntry *chain_head;
    Bucket() : chain_head(nullptr) {}
  };

 public:
  LockTable() : mask_(0), size_(0), item_count_(0) {
    Resize();
  }

  ~LockTable() {
    // Sanity checks: The table shouldn't be destructed when there are any entries in it.
    DCHECK_EQ(0, item_count_) << "There are some unreleased locks";
    for (size_t i = 0; i < size_; ++i) {
      for (LockEntry *p = buckets_[i].chain_head; p != nullptr; p = p->ht_next_) {
        DCHECK(p == nullptr) << "The entry " << p->ToString() << " was not released";
      }
    }
  }

  vector<LockEntry*> GetLockEntries(ArrayView<Slice> keys);
  LockEntry* GetLockEntry(Slice key);

  void ReleaseLockEntries(ArrayView<LockEntry*> entries);

 private:
  Bucket *FindBucket(uint64_t hash) const {
    return &(buckets_[hash & mask_]);
  }

  // Return a pointer to slot that points to a lock entry that
  // matches key/hash. If there is no such lock entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LockEntry **FindSlot(Bucket *bucket, const Slice& key, uint64_t hash) const {
    LockEntry **node = &(bucket->chain_head);
    while (*node && !(*node)->Equals(key, hash)) {
      node = &((*node)->ht_next_);
    }
    return node;
  }

  // Return a pointer to slot that points to a lock entry that
  // matches the specified 'entry'.
  // If there is no such lock entry, NULL is returned.
  LockEntry **FindEntry(Bucket *bucket, LockEntry *entry) const {
    for (LockEntry **node = &(bucket->chain_head); *node != nullptr; node = &((*node)->ht_next_)) {
      if (*node == entry) {
        return node;
      }
    }
    return nullptr;
  }

  void Resize();

 private:
  simple_spinlock lock_;
  // size - 1 used to lookup the bucket (hash & mask_)
  uint64_t mask_;
  // number of buckets in the table
  uint64_t size_;
  // number of items in the table
  int64_t item_count_;
  // table buckets
  unique_ptr<Bucket[]> buckets_;
};

vector<LockEntry*> LockTable::GetLockEntries(ArrayView<Slice> keys) {
  vector<LockEntry*> entries;
  entries.resize(keys.size());
  for (int i = 0; i < keys.size(); i++) {
    entries[i] = new LockEntry(keys[i]);
  }

  vector<LockEntry*> to_delete;

  // TODO(todd) prefetch the hash buckets
  {
    unique_lock<simple_spinlock> l(lock_);
    for (int i = 0; i < entries.size(); i++) {
      LockEntry* new_entry = entries[i];
      Bucket* bucket = FindBucket(new_entry->key_hash_);
      LockEntry **node = FindSlot(bucket, new_entry->key_, new_entry->key_hash_);
      LockEntry* old_entry = *node;
      if (PREDICT_FALSE(old_entry != nullptr)) {
        old_entry->refs_++;
        to_delete.push_back(entries[i]);
        entries[i] = old_entry;
      } else {
        new_entry->ht_next_ = nullptr;
        new_entry->CopyKey();
        *node = new_entry;
        ++item_count_;

        if (PREDICT_FALSE(item_count_ > size_)) {
          Resize();
        }
      }
    }
  }

  for (auto* e : to_delete) delete e;

  return entries;
}

LockEntry* LockTable::GetLockEntry(Slice key) {
  vector<LockEntry*> entries = GetLockEntries({&key, 1});
  return entries[0];
}

void LockTable::ReleaseLockEntries(ArrayView<LockEntry*> entries) {
  // Construct a linked list co-opting the ht_next pointers of the entries
  // to keep track of which objects need to be deleted.
  LockEntry* removed_head = nullptr;

  const auto& RemoveEntryFromBucket = [&](Bucket* bucket, LockEntry* entry) {
    LockEntry** node = FindEntry(bucket, entry);
    if (PREDICT_TRUE(node != nullptr)) {
      if (--entry->refs_ > 0) return;

      *node = entry->ht_next_;
      entry->ht_next_ = removed_head;
      removed_head = entry;
      item_count_--;
    } else {
      LOG(DFATAL) << "Unable to find LockEntry on release";
    }
  };

  {
    unique_lock<simple_spinlock> l(lock_);

    auto it = entries.begin();
    int rem = entries.size();

    // Manually block the loop into a series of constant-sized batches
    // followed by one last variable-sized batch for the remainder.
    //
    // The batch size was experimentally determined.
    static constexpr int kBatchSize = 16;
    LockEntry* batch[kBatchSize];
    Bucket* buckets[kBatchSize];
    const auto& ProcessBatch = [&](int n) {
      for (int i = 0; i < n; i++) {
        batch[i] = *it++;
        buckets[i] = FindBucket(batch[i]->key_hash_);
        prefetch(reinterpret_cast<const char*>(buckets[i]), PREFETCH_HINT_T0);
      }
      for (int i = 0; i < n; i++) {
        RemoveEntryFromBucket(buckets[i], batch[i]);
      }
    };

    while (rem >= kBatchSize) {
      ProcessBatch(kBatchSize);
      rem -= kBatchSize;
    }
    ProcessBatch(rem);
  }

  // Actually free the memory outside the lock.
  while (removed_head) {
    auto* tmp = removed_head;
    removed_head = removed_head->ht_next_;
    delete tmp;
  }
}

void LockTable::Resize() {
  // Calculate a new table size
  size_t new_size = 16;
  while (new_size < item_count_) {
    new_size <<= 1;
  }

  if (PREDICT_FALSE(size_ >= new_size))
    return;

  // Allocate a new bucket list
  unique_ptr<Bucket[]> new_buckets(new Bucket[new_size]);
  size_t new_mask = new_size - 1;

  // Copy entries
  for (size_t i = 0; i < size_; ++i) {
    LockEntry *p = buckets_[i].chain_head;
    while (p != nullptr) {
      LockEntry *next = p->ht_next_;

      // Insert Entry
      Bucket *bucket = &(new_buckets[p->key_hash_ & new_mask]);
      p->ht_next_ = bucket->chain_head;
      bucket->chain_head = p;

      p = next;
    }
  }

  // Swap the bucket
  mask_ = new_mask;
  size_ = new_size;
  buckets_.swap(new_buckets);
}

// ============================================================================
//  ScopedRowLock
// ============================================================================

ScopedRowLock::ScopedRowLock(LockManager* manager,
                             const OpState* op,
                             ArrayView<Slice> keys,
                             LockManager::LockMode mode)
    : manager_(DCHECK_NOTNULL(manager)) {
  DCHECK_EQ(LockManager::LOCK_EXCLUSIVE, mode);
  entries_ = manager_->LockBatch(keys, op);
}

ScopedRowLock::ScopedRowLock(ScopedRowLock&& other) noexcept {
  TakeState(&other);
}

ScopedRowLock& ScopedRowLock::operator=(ScopedRowLock&& other) noexcept {
  TakeState(&other);
  return *this;
}

void ScopedRowLock::TakeState(ScopedRowLock* other) {
  manager_ = other->manager_;
  entries_ = std::move(other->entries_);
}

ScopedRowLock::~ScopedRowLock() {
  Release();
}

void ScopedRowLock::Release() {
  if (entries_.empty()) return;  // Already released.
  for (auto* entry : entries_) {
    DCHECK_NOTNULL(entry)->Unlock();
  }
  manager_->ReleaseBatch(entries_);
  entries_.clear();
}

// ============================================================================
//  LockManager
// ============================================================================

LockManager::LockManager()
  : locks_(new LockTable()) {
}

LockManager::~LockManager() {
  delete locks_;
}

std::vector<LockEntry*> LockManager::LockBatch(ArrayView<Slice> keys, const OpState* op) {
  vector<LockEntry*> entries = locks_->GetLockEntries(keys);

  for (auto* e : entries) {
    AcquireLockOnEntry(e, op);
  }
  return entries;
}

void LockManager::ReleaseBatch(ArrayView<LockEntry*> locks) { locks_->ReleaseLockEntries(locks); }

void LockManager::AcquireLockOnEntry(LockEntry* entry, const OpState* op) {
  // We expect low contention, so just try to try_lock first. This is faster
  // than a timed_lock, since we don't have to do a syscall to get the current
  // time.
  if (!entry->sem.TryAcquire()) {
    // If the current holder of this lock is the same op just increment
    // the recursion count without acquiring the mutex.
    //
    //
    // NOTE: This is not a problem for the current way locks are managed since
    // they are obtained and released in bulk (all locks for an op are
    // obtained and released at the same time). If at any time in the future
    // we opt to perform more fine grained locking, possibly letting ops
    // release a portion of the locks they no longer need, this no longer is OK.
    if (ANNOTATE_UNPROTECTED_READ(entry->holder_) == op) {
      entry->recursion_++;
      return;
    }

    // If we couldn't immediately acquire the lock, do a timed lock so we can
    // warn if it takes a long time.
    // TODO: would be nice to hook in some histogram metric about lock acquisition
    // time. For now we just associate with per-request metrics.
    TRACE_COUNTER_INCREMENT("row_lock_wait_count", 1);
    MicrosecondsInt64 start_wait_us = GetMonoTimeMicros();
    int waited_seconds = 0;
    while (!entry->sem.TimedAcquire(MonoDelta::FromSeconds(1))) {
      const OpState* cur_holder = ANNOTATE_UNPROTECTED_READ(entry->holder_);
      LOG(WARNING) << "Waited " << (++waited_seconds) << " seconds to obtain row lock on key "
                   << entry->ToString() << " cur holder: " << cur_holder;
      // TODO(unknown): would be nice to also include some info about the blocking op,
      // but it's a bit tricky to do in a non-racy fashion (the other op may
      // complete at any point)
    }
    MicrosecondsInt64 wait_us = GetMonoTimeMicros() - start_wait_us;
    TRACE_COUNTER_INCREMENT("row_lock_wait_us", wait_us);
    if (wait_us > 100 * 1000) {
      TRACE("Waited $0us for lock on $1", wait_us, KUDU_REDACT(entry->ToString()));
    }
  }

  entry->holder_ = op;
}

bool LockManager::TryLock(const Slice& key, const OpState* op, LockEntry** entry) {
  *entry = locks_->GetLockEntry(key);
  bool locked = (*entry)->sem.TryAcquire();
  if (!locked) {
    Release(*entry);
    return false;
  }
  (*entry)->holder_ = op;
  return true;
}

void LockEntry::Unlock() {
  DCHECK(holder_);
  if (recursion_ > 0) {
    recursion_--;
  } else {
    holder_ = nullptr;
    sem.Release();
  }
}

void LockManager::Release(LockEntry* lock) { locks_->ReleaseLockEntries({&lock, 1}); }

} // namespace tablet
} // namespace kudu
