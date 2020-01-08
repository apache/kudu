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

#include "kudu/util/file_cache.h"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/cache.h"
#include "kudu/util/cache_metrics.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/env.h"
#include "kudu/util/file_cache_metrics.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/locks.h"
#include "kudu/util/monotime.h"
#include "kudu/util/once.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "kudu/util/thread.h"

DEFINE_int32(file_cache_expiry_period_ms, 60 * 1000,
             "Period of time (in ms) between removing expired file cache descriptors");
TAG_FLAG(file_cache_expiry_period_ms, advanced);

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;

namespace kudu {

namespace {

template <class FileType>
FileType* CacheValueToFileType(Slice s) {
  return reinterpret_cast<FileType*>(*reinterpret_cast<void**>(
      s.mutable_data()));
}

class EvictionCallback : public Cache::EvictionCallback {
 public:
  EvictionCallback() {}

  void EvictedEntry(Slice key, Slice value) override {
    VLOG(2) << "Evicted fd belonging to " << key.ToString();
    delete CacheValueToFileType<File>(value);
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(EvictionCallback);
};

} // anonymous namespace

namespace internal {

template <class FileType>
class ScopedOpenedDescriptor;

// Encapsulates common descriptor fields and methods.
template <class FileType>
class BaseDescriptor {
 public:
  BaseDescriptor(FileCache* file_cache,
                 string filename)
      : file_cache_(file_cache),
        file_name_(std::move(filename)) {}

  ~BaseDescriptor() {
    VLOG(2) << "Out of scope descriptor with file name: " << filename();

    // The (now expired) weak_ptr remains in 'descriptors_', to be removed by
    // the next call to RunDescriptorExpiry(). Removing it here would risk a
    // deadlock on recursive acquisition of 'lock_'.

    if (deleted()) {
      cache()->Erase(filename());

      VLOG(1) << "Deleting file: " << filename();
      WARN_NOT_OK(env()->DeleteFile(filename()), "");
    }
  }

  // Insert a pointer to an open file object into the file cache with the
  // filename as the cache key.
  //
  // Returns a handle to the inserted entry. The handle always contains an open
  // file.
  ScopedOpenedDescriptor<FileType> InsertIntoCache(void* file_ptr) const {
    // The allocated charge is always one byte. This is incorrect with respect
    // to memory tracking, but it's necessary if the cache capacity is to be
    // equivalent to the max number of fds.
    auto pending(cache()->Allocate(filename(), sizeof(file_ptr), 1));
    CHECK(pending);
    memcpy(cache()->MutableValue(&pending), &file_ptr, sizeof(file_ptr));
    return ScopedOpenedDescriptor<FileType>(
        this, cache()->Insert(std::move(pending),
                              file_cache_->eviction_cb_.get()));
  }

  // Retrieves a pointer to an open file object from the file cache with the
  // filename as the cache key.
  //
  // Returns a handle to the looked up entry. The handle may or may not contain
  // an open file, depending on whether the cache hit or missed.
  ScopedOpenedDescriptor<FileType> LookupFromCache() const {
    return ScopedOpenedDescriptor<FileType>(
        this, cache()->Lookup(filename(), Cache::EXPECT_IN_CACHE));
  }

  // Mark this descriptor as to-be-deleted later.
  void MarkDeleted() {
    DCHECK(!deleted());
    while (true) {
      auto v = flags_.load();
      if (flags_.compare_exchange_weak(v, v | FILE_DELETED)) return;
    }
  }

  // Mark this descriptor as invalidated. No further access is allowed
  // to this file.
  void MarkInvalidated() {
    DCHECK(!invalidated());
    while (true) {
      auto v = flags_.load();
      if (flags_.compare_exchange_weak(v, v | INVALIDATED)) return;
    }
  }

  Cache* cache() const { return file_cache_->cache_.get(); }

  Env* env() const { return file_cache_->env_; }

  const string& filename() const { return file_name_; }

  bool deleted() const { return flags_.load() & FILE_DELETED; }
  bool invalidated() const { return flags_.load() & INVALIDATED; }

 private:
  FileCache* file_cache_;
  const string file_name_;
  enum Flags {
    FILE_DELETED = 1 << 0,
    INVALIDATED = 1 << 1
  };
  std::atomic<uint8_t> flags_ {0};

  DISALLOW_COPY_AND_ASSIGN(BaseDescriptor);
};

// A "smart" retrieved LRU cache handle.
//
// The cache handle is released when this object goes out of scope, possibly
// closing the opened file if it is no longer in the cache.
template <class FileType>
class ScopedOpenedDescriptor {
 public:
  // A not-yet-but-soon-to-be opened descriptor.
  explicit ScopedOpenedDescriptor(const BaseDescriptor<FileType>* desc)
      : desc_(desc),
        handle_(nullptr, Cache::HandleDeleter(desc_->cache())) {
  }

  // An opened descriptor. Its handle may or may not contain an open file.
  ScopedOpenedDescriptor(const BaseDescriptor<FileType>* desc,
                         Cache::UniqueHandle handle)
      : desc_(desc),
        handle_(std::move(handle)) {
  }

  bool opened() const { return handle_.get(); }

  FileType* file() const {
    DCHECK(opened());
    return CacheValueToFileType<FileType>(desc_->cache()->Value(handle_));
  }

 private:
  const BaseDescriptor<FileType>* desc_;
  Cache::UniqueHandle handle_;
};

// Reference to an on-disk file that may or may not be opened (and thus
// cached) in the file cache.
//
// This empty template is just a specification; actual descriptor classes must
// be fully specialized.
template <class FileType>
class Descriptor : public FileType {
};

// A descriptor adhering to the RWFile interface (i.e. when opened, provides
// a read-write interface to the underlying file).
template <>
class Descriptor<RWFile> : public RWFile {
 public:
  Descriptor(FileCache* file_cache, const string& filename)
      : base_(file_cache, filename) {}

  ~Descriptor() = default;

  Status Read(uint64_t offset, Slice result) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Read(offset, result);
  }

  Status ReadV(uint64_t offset, ArrayView<Slice> results) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->ReadV(offset, results);
  }

  Status Write(uint64_t offset, const Slice& data) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Write(offset, data);
  }

  Status WriteV(uint64_t offset, ArrayView<const Slice> data) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->WriteV(offset, data);
  }

  Status PreAllocate(uint64_t offset, size_t length, PreAllocateMode mode) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->PreAllocate(offset, length, mode);
  }

  Status Truncate(uint64_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Truncate(length);
  }

  Status PunchHole(uint64_t offset, size_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->PunchHole(offset, length);
  }

  Status Flush(FlushMode mode, uint64_t offset, size_t length) override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Flush(mode, offset, length);
  }

  Status Sync() override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Sync();
  }

  Status Close() override {
    // Intentional no-op; actual closing is deferred to LRU cache eviction.
    return Status::OK();
  }

  Status Size(uint64_t* size) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Size(size);
  }

  Status GetExtentMap(ExtentMap* out) const override {
    ScopedOpenedDescriptor<RWFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->GetExtentMap(out);
  }

  const string& filename() const override {
    return base_.filename();
  }

 private:
  friend class ::kudu::FileCache;

  Status Init() {
    return once_.Init(&Descriptor<RWFile>::InitOnce, this);
  }

  Status InitOnce() {
    return ReopenFileIfNecessary(nullptr);
  }

  Status ReopenFileIfNecessary(ScopedOpenedDescriptor<RWFile>* out) const {
    ScopedOpenedDescriptor<RWFile> found(base_.LookupFromCache());
    CHECK(!base_.invalidated());
    if (found.opened()) {
      // The file is already open in the cache, return it.
      if (out) {
        *out = std::move(found);
      }
      return Status::OK();
    }

    // The file was evicted, reopen it.
    RWFileOptions opts;
    opts.mode = Env::MUST_EXIST;
    unique_ptr<RWFile> f;
    RETURN_NOT_OK(base_.env()->NewRWFile(opts, base_.filename(), &f));

    // The cache will take ownership of the newly opened file.
    ScopedOpenedDescriptor<RWFile> opened(base_.InsertIntoCache(f.release()));
    if (out) {
      *out = std::move(opened);
    }
    return Status::OK();
  }

  BaseDescriptor<RWFile> base_;
  KuduOnceDynamic once_;

  DISALLOW_COPY_AND_ASSIGN(Descriptor);
};

// A descriptor adhering to the RandomAccessFile interface (i.e. when opened,
// provides a read-only interface to the underlying file).
template <>
class Descriptor<RandomAccessFile> : public RandomAccessFile {
 public:
  Descriptor(FileCache* file_cache, const string& filename)
      : base_(file_cache, filename) {}

  ~Descriptor() = default;

  Status Read(uint64_t offset, Slice result) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Read(offset, result);
  }

  Status ReadV(uint64_t offset, ArrayView<Slice> results) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->ReadV(offset, results);
  }

  Status Size(uint64_t *size) const override {
    ScopedOpenedDescriptor<RandomAccessFile> opened(&base_);
    RETURN_NOT_OK(ReopenFileIfNecessary(&opened));
    return opened.file()->Size(size);
  }

  const string& filename() const override {
    return base_.filename();
  }

  size_t memory_footprint() const override {
    // Normally we would use kudu_malloc_usable_size(this). However, that's
    // not safe because 'this' was allocated via std::make_shared(), which
    // means it isn't necessarily the base of the memory allocation; it may be
    // preceded by the shared_ptr control block.
    //
    // It doesn't appear possible to get the base of the allocation via any
    // shared_ptr APIs, so we'll use sizeof(*this) + 16 instead. The 16 bytes
    // represent the shared_ptr control block. Overall the object size is still
    // undercounted as it doesn't account for any internal heap fragmentation,
    // but at least it's safe.
    //
    // Some anecdotal memory measurements taken inside gdb:
    // - glibc 2.23 malloc_usable_size() on make_shared<FileType>: 88 bytes.
    // - tcmalloc malloc_usable_size() on make_shared<FileType>: 96 bytes.
    // - sizeof(std::_Sp_counted_base<>) with libstdc++ 5.4: 16 bytes.
    // - sizeof(std::__1::__shared_ptr_emplace<>) with libc++ 3.9: 16 bytes.
    // - sizeof(*this): 72 bytes.
    return sizeof(*this) +
        16 + // shared_ptr control block
        once_.memory_footprint_excluding_this() +
        base_.filename().capacity();
  }

 private:
  friend class ::kudu::FileCache;

  Status Init() {
    return once_.Init(&Descriptor<RandomAccessFile>::InitOnce, this);
  }

  Status InitOnce() {
    return ReopenFileIfNecessary(nullptr);
  }

  Status ReopenFileIfNecessary(
      ScopedOpenedDescriptor<RandomAccessFile>* out) const {
    ScopedOpenedDescriptor<RandomAccessFile> found(base_.LookupFromCache());
    CHECK(!base_.invalidated());
    if (found.opened()) {
      // The file is already open in the cache, return it.
      if (out) {
        *out = std::move(found);
      }
      return Status::OK();
    }

    // The file was evicted, reopen it.
    unique_ptr<RandomAccessFile> f;
    RETURN_NOT_OK(base_.env()->NewRandomAccessFile(base_.filename(), &f));

    // The cache will take ownership of the newly opened file.
    ScopedOpenedDescriptor<RandomAccessFile> opened(
        base_.InsertIntoCache(f.release()));
    if (out) {
      *out = std::move(opened);
    }
    return Status::OK();
  }

  BaseDescriptor<RandomAccessFile> base_;
  KuduOnceDynamic once_;

  DISALLOW_COPY_AND_ASSIGN(Descriptor);
};

} // namespace internal

const char* const FileCache::kAlreadyDeleted = "File already marked as deleted";

FileCache::FileCache(const string& cache_name,
                     Env* env,
                     int max_open_files,
                     const scoped_refptr<MetricEntity>& entity)
    : env_(env),
      cache_name_(cache_name),
      eviction_cb_(new EvictionCallback()),
      cache_(NewCache(max_open_files, cache_name)),
      running_(1) {
  if (entity) {
    unique_ptr<FileCacheMetrics> metrics(new FileCacheMetrics(entity));
    cache_->SetMetrics(std::move(metrics));
  }
  LOG(INFO) << Substitute("Constructed file cache $0 with capacity $1",
                          cache_name, max_open_files);
}

FileCache::~FileCache() {
  running_.CountDown();
  if (descriptor_expiry_thread_) {
    descriptor_expiry_thread_->Join();
  }
}

Status FileCache::Init() {
  return Thread::Create("cache", Substitute("$0-evict", cache_name_),
                        &FileCache::RunDescriptorExpiry, this,
                        &descriptor_expiry_thread_);
}

template <>
Status FileCache::OpenExistingFile(const string& file_name,
                                   shared_ptr<RWFile>* file) {
  shared_ptr<internal::Descriptor<RWFile>> d;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    d = FindDescriptorUnlocked(file_name, FindMode::CREATE_IF_NOT_EXIST, &rwf_descs_);
    DCHECK(d);

    // Enforce the invariant that a particular file name may only be used by one
    // descriptor at a time. This is expensive so it's only done in DEBUG mode.
    DCHECK(!FindDescriptorUnlocked(file_name, FindMode::DONT_CREATE, &raf_descs_));
  }
  if (d->base_.deleted()) {
    return Status::NotFound(kAlreadyDeleted, file_name);
  }

  // Check that the underlying file can be opened (no-op for found descriptors).
  //
  // Done outside the lock.
  RETURN_NOT_OK(d->Init());
  *file = std::move(d);
  return Status::OK();
}

template <>
Status FileCache::OpenExistingFile(const string& file_name,
                                   shared_ptr<RandomAccessFile>* file) {
  shared_ptr<internal::Descriptor<RandomAccessFile>> d;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    d = FindDescriptorUnlocked(file_name, FindMode::CREATE_IF_NOT_EXIST, &raf_descs_);
    DCHECK(d);

    // Enforce the invariant that a particular file name may only be used by one
    // descriptor at a time. This is expensive so it's only done in DEBUG mode.
    DCHECK(!FindDescriptorUnlocked(file_name, FindMode::DONT_CREATE, &rwf_descs_));
  }
  if (d->base_.deleted()) {
    return Status::NotFound(kAlreadyDeleted, file_name);
  }

  // Check that the underlying file can be opened (no-op for found descriptors).
  //
  // Done outside the lock.
  RETURN_NOT_OK(d->Init());
  *file = std::move(d);
  return Status::OK();
}

Status FileCache::DeleteFile(const string& file_name) {
  // Mark any outstanding descriptor as deleted. Because there may only be one
  // descriptor per file name, we can short circuit the search if we find a
  // descriptor in the first map.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    {
      auto d = FindDescriptorUnlocked(file_name, FindMode::DONT_CREATE, &rwf_descs_);
      if (d) {
        if (d->base_.deleted()) {
          return Status::NotFound(kAlreadyDeleted, file_name);
        }
        d->base_.MarkDeleted();
        return Status::OK();
      }
    }
    {
      auto d = FindDescriptorUnlocked(file_name, FindMode::DONT_CREATE, &raf_descs_);
      if (d) {
        if (d->base_.deleted()) {
          return Status::NotFound(kAlreadyDeleted, file_name);
        }
        d->base_.MarkDeleted();
        return Status::OK();
      }
    }
  }

  // There are no outstanding descriptors. Delete the file now.
  //
  // Make sure it's been fully evicted from the cache (perhaps it was opened
  // previously?) so that the filesystem can reclaim the file data instantly.
  cache_->Erase(file_name);
  return env_->DeleteFile(file_name);
}

void FileCache::Invalidate(const string& file_name) {
  // Ensure that there are invalidated descriptors in both maps for this
  // filename. This ensures that any concurrent opens during this method will
  // see the invalidation and result in a CHECK failure.
  //
  // Note: this temporarily violates the invariant that no two descriptors may
  // share the same file name. That's OK because the invalidation CHECK failure
  // occurs before the client trips on the broken invariant.
  shared_ptr<internal::Descriptor<RWFile>> rwf_desc;
  shared_ptr<internal::Descriptor<RandomAccessFile>> raf_desc;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    rwf_desc = FindDescriptorUnlocked(file_name, FindMode::CREATE_IF_NOT_EXIST,
                                      &rwf_descs_);
    DCHECK(rwf_desc);
    rwf_desc->base_.MarkInvalidated();

    raf_desc = FindDescriptorUnlocked(file_name, FindMode::CREATE_IF_NOT_EXIST,
                                      &raf_descs_);
    DCHECK(raf_desc);
    raf_desc->base_.MarkInvalidated();
  }

  // Remove it from the cache so that if the same path is opened again, we
  // will re-open a new FD rather than retrieving one that might have been
  // cached prior to invalidation.
  cache_->Erase(file_name);

  // Remove the invalidated descriptors from the maps. We are guaranteed they
  // are still there because we've held a strong references to them for the
  // duration of this method, and no other methods erase strong references from
  // the maps.
  {
    std::lock_guard<simple_spinlock> l(lock_);
    CHECK_EQ(1, rwf_descs_.erase(file_name));
    CHECK_EQ(1, raf_descs_.erase(file_name));
  }
}

size_t FileCache::NumDescriptorsForTests() const {
  std::lock_guard<simple_spinlock> l(lock_);
  return rwf_descs_.size() + raf_descs_.size();
}

string FileCache::ToDebugString() const {
  string ret;

  // We need to iterate through the descriptor maps, so make temporary copies
  // of them.
  DescriptorMap<RWFile> rwfs_copy;
  DescriptorMap<RandomAccessFile> rafs_copy;
  {
    std::lock_guard<simple_spinlock> l(lock_);
    rwfs_copy = rwf_descs_;
    rafs_copy = raf_descs_;
  }

  // Dump the contents of the copies.
  ret += MapToDebugString(rwfs_copy, "rwf");
  ret += MapToDebugString(rafs_copy, "raf");
  return ret;
}

template <class FileType>
string FileCache::MapToDebugString(const DescriptorMap<FileType>& descs,
                                   const string& prefix) {
  string ret;
  for (const auto& e : descs) {
    bool strong = false;
    bool deleted = false;
    bool opened = false;
    shared_ptr<internal::Descriptor<FileType>> d = e.second.lock();
    if (d) {
      strong = true;
      if (d->base_.deleted()) {
        deleted = true;
      }
      internal::ScopedOpenedDescriptor<FileType> sod(d->base_.LookupFromCache());
      if (sod.opened()) {
        opened = true;
      }
    }
    if (strong) {
      ret += Substitute("$0: $1 (S$2$3)\n", prefix, e.first,
                        deleted ? "D" : "", opened ? "O" : "");
    } else {
      ret += Substitute("$0: $1\n", prefix, e.first);
    }
  }
  return ret;
}

template <class FileType>
shared_ptr<internal::Descriptor<FileType>> FileCache::FindDescriptorUnlocked(
    const string& file_name,
    FindMode mode,
    DescriptorMap<FileType>* descs) {
  DCHECK(lock_.is_locked());

  shared_ptr<internal::Descriptor<FileType>> d;
  auto it = descs->find(file_name);
  if (it != descs->end()) {
    // Found the descriptor. Has it expired?
    d = it->second.lock();
    if (d) {
      CHECK(!d->base_.invalidated());

      // Descriptor is still valid, return it.
      VLOG(2) << "Found existing descriptor: " << file_name;
      return d;
    }
    // Descriptor has expired; erase it and pretend we found nothing.
    descs->erase(it);
  }

  if (mode == FindMode::CREATE_IF_NOT_EXIST) {
    d = std::make_shared<internal::Descriptor<FileType>>(this, file_name);
    EmplaceOrDie(descs, file_name, d);
    VLOG(2) << "Created new descriptor: " << file_name;
  }
  return d;
}

template <class FileType>
void FileCache::ExpireDescriptorsFromMap(DescriptorMap<FileType>* descs) {
  for (auto it = descs->begin(); it != descs->end();) {
    if (it->second.expired()) {
      it = descs->erase(it);
    } else {
      it++;
    }
  }
}

void FileCache::RunDescriptorExpiry() {
  while (!running_.WaitFor(MonoDelta::FromMilliseconds(
      FLAGS_file_cache_expiry_period_ms))) {
    std::lock_guard<simple_spinlock> l(lock_);
    ExpireDescriptorsFromMap(&rwf_descs_);
    ExpireDescriptorsFromMap(&raf_descs_);
  }
}

} // namespace kudu
