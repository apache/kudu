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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/cache.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/locks.h"
#include "kudu/util/status.h"

namespace kudu {

namespace internal {

template <class FileType>
class BaseDescriptor;
template <class FileType>
class Descriptor;

} // namespace internal

class MetricEntity;
class Thread;

// Cache of open files.
//
// The purpose of this cache is to enforce an upper bound on the maximum number
// of files open at a time. Files opened through the cache may be closed at any
// time, only to be reopened upon next use.
//
// The file cache can be viewed as having two logical parts: the client-facing
// API and the LRU cache.
//
// Client-facing API
// -----------------
// The core of the client-facing API is the cache descriptor. A descriptor
// uniquely identifies an opened file. To a client, a descriptor is just an
// open file interface of the variety defined in util/env.h. Clients open
// descriptors via the OpenFile() cache methods.
//
// Descriptors are shared objects; an existing descriptor is handed back to a
// client if a file with the same name is already opened. To facilitate
// descriptor sharing, the file cache maintains by-file-name descriptor maps
// (one per file type). The values are weak references to the descriptors so
// that map entries don't affect the descriptor lifecycle.
//
// LRU cache
// ---------
// The lower half of the file cache is a standard LRU cache whose keys are file
// names and whose values are pointers to opened file objects allocated on the
// heap. Unlike the descriptor maps, this cache has an upper bound on capacity,
// and handles are evicted (and closed) according to an LRU algorithm.
//
// Whenever a descriptor is used by a client in file I/O, its file name is used
// in an LRU cache lookup. If found, the underlying file is still open and the
// file access is performed. Otherwise, the file must have been evicted and
// closed, so it is reopened and reinserted (possibly evicting a different open
// file) before the file access is performed.
//
// Other notes
// -----------
// In a world where files are opened and closed transparently, file deletion
// demands special care if UNIX semantics are to be preserved. When a call to
// DeleteFile() is made to a file with an opened descriptor, the descriptor is
// simply "marked" as to-be-deleted-later. Only when all references to the
// descriptor are dropped is the file actually deleted. If there is no open
// descriptor, the file is deleted immediately.
//
// Every public method in the file cache is thread safe.
class FileCache {
 public:
  // Creates a new file cache.
  //
  // The 'cache_name' is used to disambiguate amongst other file cache
  // instances. The cache will use 'max_open_files' as a soft upper bound on
  // the number of files open at any given time.
  FileCache(const std::string& cache_name,
            Env* env,
            int max_open_files,
            const scoped_refptr<MetricEntity>& entity);

  // Destroys the file cache.
  ~FileCache();

  // Initializes the file cache. Initialization done here may fail.
  Status Init();

  // Opens an existing file by name through the cache.
  //
  // The returned 'file' is actually an object called a descriptor. It adheres
  // to a file-like interface but interfaces with the cache under the hood to
  // reopen a file as needed during file operations.
  //
  // The underlying file is opened immediately to respect 'Mode', but may be
  // closed later if the cache reaches its upper bound on the number of open
  // files. It is also closed when the descriptor's last reference is dropped.
  //
  // All file types honor a 'Mode' of MUST_EXIST. Some may honor other modes as
  // well, although transparently reopening evicted files will always use
  // MUST_EXIST. Different combinations of modes and file types are expressed as
  // template specializations; if a file type doesn't support a particular mode,
  // there will be a linker error.
  //
  // TODO(adar): The file cache tries to behave as if users were accessing the
  // underlying POSIX filesystem directly, but its semantics aren't 100% correct
  // when using modes other than MUST_EXIST. For example, the behavior of
  // MUST_CREATE and CREATE_OR_OPEN isn't quite right for open files marked for
  // deletion. In theory we should "unmark" such a file to indicate that it was
  // recreated, and truncate it so it's empty for the second client, but the
  // truncation would corrupt the file for the first client. In short, take
  // great care when using any mode apart from MUST_EXIST.
  template <Env::OpenMode Mode, class FileType>
  Status OpenFile(const std::string& file_name,
                  std::shared_ptr<FileType>* file);

  // Deletes a file by name through the cache.
  //
  // If there is an outstanding descriptor for the file, the deletion will be
  // deferred until the last reference is dropped. Otherwise, the file is
  // deleted immediately.
  Status DeleteFile(const std::string& file_name);

  // Invalidate the given path in the cache if present. This removes the
  // path from the cache, and invalidates any previously-opened descriptors
  // associated with this file.
  //
  // If a file with the same path is opened again, the actual path will be opened from
  // disk.
  //
  // This operation should be used during 'rename-to-replace' patterns, eg:
  //
  //    WriteNewDataTo(tmp_path);
  //    env->RenameFile(tmp_path, p);
  //    file_cache->Invalidate(p);
  //
  // NOTE: if any reader of 'p' holds an open descriptor from the cache
  // prior to this operation, that descriptor is invalidated and any
  // further operations on that descriptor will result in a CHECK failure.
  // Hence this is not safe to use without some external synchronization
  // which prevents concurrent access to the same file.
  //
  // NOTE: this function must not be called concurrently on the same file name
  // from multiple threads.
  void Invalidate(const std::string& file_name);

  // Returns the number of entries in the descriptor maps.
  //
  // Only intended for unit tests.
  size_t NumDescriptorsForTests() const;

  // Dumps the contents of the file cache. Intended for debugging.
  std::string ToDebugString() const;

 private:
  friend class internal::BaseDescriptor<RWFile>;
  friend class internal::BaseDescriptor<RandomAccessFile>;

  template <class FileType>
  using DescriptorMap = std::unordered_map<std::string,
                                           std::weak_ptr<internal::Descriptor<FileType>>>;

  template <class FileType>
  FRIEND_TEST(FileCacheTest, TestBasicOperations);

  // Dumps a descriptor map in 'descriptors'. All output will be prefixed by 'prefix'.
  template <class FileType>
  static std::string MapToDebugString(const DescriptorMap<FileType>& descs,
                                      const std::string& prefix);

  // Removes all expired descriptors from 'descs'.
  template <class FileType>
  static void ExpireDescriptorsFromMap(DescriptorMap<FileType>* descs);

  // Looks up a descriptor by file name or creates a new one (if requested).
  //
  // The value of 'created_desc' will be set in accordance with whether a new
  // descriptor was created.
  //
  // Must be called with 'lock_' held.
  enum class FindMode {
    // Only return an existing descriptor from the map; don't create a new one.
    DONT_CREATE,

    // Create a new descriptor if one did not exist in the map.
    CREATE_IF_NOT_EXIST,
  };
  template <class FileType>
  std::shared_ptr<internal::Descriptor<FileType>> FindDescriptorUnlocked(
      const std::string& file_name,
      FindMode mode,
      DescriptorMap<FileType>* descs,
      bool* created_desc);

  // Periodically removes expired descriptors from the descriptor maps.
  void RunDescriptorExpiry();

  // Actually opens the file as per OpenFile. Used to encapsulate the bulk of
  // OpenFile because C++ prohibits partial specialization of template functions.
  template <class FileType>
  Status DoOpenFile(const std::string& file_name,
                    std::shared_ptr<FileType>* file,
                    bool* created_desc);

  // Status message prefix for files that have already been marked as deleted.
  static const char* const kAlreadyDeleted;

  // Interface to the underlying filesystem.
  Env* env_;

  // Name of the cache.
  const std::string cache_name_;

  // Invoked whenever a cached file reaches zero references (i.e. it was
  // removed from the cache and is no longer in use by any file operations).
  std::unique_ptr<Cache::EvictionCallback> eviction_cb_;

  // Underlying cache instance. Caches opened files.
  std::unique_ptr<Cache> cache_;

  // Protects the descriptor map.
  mutable simple_spinlock lock_;

  // Maps filenames to descriptors.
  DescriptorMap<RWFile> rwf_descs_;
  DescriptorMap<RandomAccessFile> raf_descs_;

  // Calls RunDescriptorExpiry() in a loop until 'running_' isn't set.
  scoped_refptr<Thread> descriptor_expiry_thread_;

  // Tracks whether or not 'descriptor_expiry_thread_' should be running.
  CountDownLatch running_;

  DISALLOW_COPY_AND_ASSIGN(FileCache);
};

} // namespace kudu
