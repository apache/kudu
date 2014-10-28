// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Confidential Cloudera Information: Covered by NDA.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// This is taken from LevelDB and evolved to fit the kudu codebase.
//
// TODO: this is pretty lock-heavy. Would be good to sub out something
// a little more concurrent.

#ifndef KUDU_UTIL_CACHE_H_
#define KUDU_UTIL_CACHE_H_

#include <stdint.h>

#include "kudu/gutil/macros.h"
#include "kudu/util/slice.h"

namespace kudu {

class Cache;
struct CacheMetrics;
class MetricContext;

// Create a new cache with a fixed size capacity.  This implementation
// of Cache uses a least-recently-used eviction policy.
extern Cache* NewLRUCache(size_t capacity);

class Cache {
 public:
  Cache() { }

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache();

  // Opaque handle to an entry stored in the cache.
  struct Handle { };

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // Note that the 'key' Slice is copied into the internal storage of
  // the cache. The caller may free or mutate the key data freely
  // after this method returns.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter". The key is only passed
  // to the deleter for convenience -- the cache itself is responsible
  // for managing the key's memory.
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // If the cache has no mapping for "key", returns NULL.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle* Lookup(const Slice& key) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // Pass a metric context in order to start recoding metrics.
  virtual void SetMetrics(const MetricContext& metric_ctx) = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Cache);

  void LRU_Remove(Handle* e);
  void LRU_Append(Handle* e);
  void Unref(Handle* e);

  struct Rep;
  Rep* rep_;
};

}  // namespace kudu

#endif
