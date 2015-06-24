// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Confidential Cloudera Information: Covered by NDA.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Modified for kudu:
// - use boost mutexes instead of port mutexes

#include <string.h>
#include <boost/foreach.hpp>
#include <glog/logging.h>
#include <map>
#include <string>
#include <vector>

#include "kudu/gutil/map-util.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/strings/strip.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/env.h"
#include "kudu/util/malloc.h"
#include "kudu/util/mutex.h"
#include "kudu/util/memenv/memenv.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

namespace {

using std::string;
using std::vector;
using strings::Substitute;

class FileState {
 public:
  // FileStates are reference counted. The initial reference count is zero
  // and the caller must call Ref() at least once.
  explicit FileState(const string& filename)
  : refs_(0),
    size_(0),
    filename_(filename) {
  }

  // Increase the reference count.
  void Ref() {
    MutexLock lock(refs_mutex_);
    ++refs_;
  }

  // Decrease the reference count. Delete if this is the last reference.
  void Unref() {
    bool do_delete = false;

    {
      MutexLock lock(refs_mutex_);
      --refs_;
      assert(refs_ >= 0);
      if (refs_ <= 0) {
        do_delete = true;
      }
    }

    if (do_delete) {
      delete this;
    }
  }

  uint64_t Size() const { return size_; }

  Status Read(uint64_t offset, size_t n, Slice* result, uint8_t* scratch) const {
    if (offset > size_) {
      return Status::IOError("Offset greater than file size.");
    }
    const uint64_t available = size_ - offset;
    if (n > available) {
      n = available;
    }
    if (n == 0) {
      *result = Slice();
      return Status::OK();
    }

    size_t block = offset / kBlockSize;
    size_t block_offset = offset % kBlockSize;

    if (n <= kBlockSize - block_offset) {
      // The requested bytes are all in the first block.
      *result = Slice(blocks_[block] + block_offset, n);
      return Status::OK();
    }

    size_t bytes_to_copy = n;
    uint8_t* dst = scratch;

    while (bytes_to_copy > 0) {
      size_t avail = kBlockSize - block_offset;
      if (avail > bytes_to_copy) {
        avail = bytes_to_copy;
      }
      memcpy(dst, blocks_[block] + block_offset, avail);

      bytes_to_copy -= avail;
      dst += avail;
      block++;
      block_offset = 0;
    }

    *result = Slice(scratch, n);
    return Status::OK();
  }

  Status PreAllocate(uint64_t size) {
    uint8_t *padding = new uint8_t[size];
    // TODO optimize me
    memset(&padding, 0, sizeof(uint8_t));
    Status s = AppendRaw(padding, size);
    delete [] padding;
    size_ -= size;
    return s;
  }

  Status Append(const Slice& data) {
    return AppendRaw(data.data(), data.size());
  }

  Status AppendRaw(const uint8_t *src, size_t src_len) {
    while (src_len > 0) {
      size_t avail;
      size_t offset = size_ % kBlockSize;

      if (offset != 0) {
        // There is some room in the last block.
        avail = kBlockSize - offset;
      } else {
        // No room in the last block; push new one.
        blocks_.push_back(new uint8_t[kBlockSize]);
        avail = kBlockSize;
      }

      if (avail > src_len) {
        avail = src_len;
      }
      memcpy(blocks_.back() + offset, src, avail);
      src_len -= avail;
      src += avail;
      size_ += avail;
    }

    return Status::OK();
  }

  const string& filename() const { return filename_; }

  size_t memory_footprint() const {
    size_t size = kudu_malloc_usable_size(this);
    size += kudu_malloc_usable_size(blocks_.data());
    BOOST_FOREACH(uint8_t* block, blocks_) {
      size += kudu_malloc_usable_size(block);
    }
    size += filename_.capacity();
    return size;
  }

 private:
  // Private since only Unref() should be used to delete it.
  ~FileState() {
    for (vector<uint8_t*>::iterator i = blocks_.begin(); i != blocks_.end();
         ++i) {
      delete [] *i;
    }
  }

  // No copying allowed.
  FileState(const FileState&);
  void operator=(const FileState&);

  Mutex refs_mutex_;
  int refs_;  // Protected by refs_mutex_;

  // The following fields are not protected by any mutex. They are only mutable
  // while the file is being written, and concurrent access is not allowed
  // to writable files.
  vector<uint8_t*> blocks_;
  uint64_t size_;

  string filename_;

  enum { kBlockSize = 8 * 1024 };
};

class SequentialFileImpl : public SequentialFile {
 public:
  explicit SequentialFileImpl(FileState* file) : file_(file), pos_(0) {
    file_->Ref();
  }

  ~SequentialFileImpl() {
    file_->Unref();
  }

  virtual Status Read(size_t n, Slice* result, uint8_t* scratch) OVERRIDE {
    Status s = file_->Read(pos_, n, result, scratch);
    if (s.ok()) {
      pos_ += result->size();
    }
    return s;
  }

  virtual Status Skip(uint64_t n) OVERRIDE {
    if (pos_ > file_->Size()) {
      return Status::IOError("pos_ > file_->Size()");
    }
    const size_t available = file_->Size() - pos_;
    if (n > available) {
      n = available;
    }
    pos_ += n;
    return Status::OK();
  }

  virtual const string& filename() const OVERRIDE {
    return file_->filename();
  }

 private:
  FileState* file_;
  size_t pos_;
};

class RandomAccessFileImpl : public RandomAccessFile {
 public:
  explicit RandomAccessFileImpl(FileState* file) : file_(file) {
    file_->Ref();
  }

  ~RandomAccessFileImpl() {
    file_->Unref();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      uint8_t* scratch) const OVERRIDE {
    return file_->Read(offset, n, result, scratch);
  }

  virtual Status Size(uint64_t *size) const OVERRIDE {
    *size = file_->Size();
    return Status::OK();
  }

  virtual const string& filename() const OVERRIDE {
    return file_->filename();
  }

  virtual size_t memory_footprint() const OVERRIDE {
    // The FileState is actually shared between multiple files, but the double
    // counting doesn't matter much since MemEnv is only used in tests.
    return kudu_malloc_usable_size(this) + file_->memory_footprint();
  }

 private:
  FileState* file_;
};

class WritableFileImpl : public WritableFile {
 public:
  explicit WritableFileImpl(FileState* file) : file_(file) {
    file_->Ref();
  }

  ~WritableFileImpl() {
    file_->Unref();
  }

  virtual Status PreAllocate(uint64_t size) OVERRIDE {
    return file_->PreAllocate(size);
  }

  virtual Status Append(const Slice& data) OVERRIDE {
    return file_->Append(data);
  }

  // This is a dummy implementation that simply serially appends all
  // slices using regular I/O.
  virtual Status AppendVector(const vector<Slice>& data_vector) OVERRIDE {
    BOOST_FOREACH(const Slice& data, data_vector) {
      RETURN_NOT_OK(file_->Append(data));
    }
    return Status::OK();
  }

  virtual Status Close() OVERRIDE { return Status::OK(); }

  virtual Status Flush(FlushMode mode) OVERRIDE { return Status::OK(); }

  virtual Status Sync() OVERRIDE { return Status::OK(); }

  virtual uint64_t Size() const OVERRIDE { return file_->Size(); }

  virtual const string& filename() const OVERRIDE {
    return file_->filename();
  }

 private:
  FileState* file_;
};

class RWFileImpl : public RWFile {
 public:
  explicit RWFileImpl(FileState* file) : file_(file) {
    file_->Ref();
  }

  ~RWFileImpl() {
    file_->Unref();
  }

  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const OVERRIDE {
    return file_->Read(offset, length, result, scratch);
  }

  virtual Status Write(uint64_t offset, const Slice& data) OVERRIDE {
    uint64_t file_size = file_->Size();
    // TODO: Modify FileState to allow rewriting.
    if (offset < file_size) {
      return Status::NotSupported(
          "In-memory RW file does not support random writing");
    } else if (offset > file_size) {
      // Fill in the space between with zeroes.
      uint8_t zeroes[offset - file_size];
      memset(zeroes, 0, sizeof(zeroes));
      Slice s(zeroes, sizeof(zeroes));
      RETURN_NOT_OK(file_->Append(s));
    }
    return file_->Append(data);
  }

  virtual Status PreAllocate(uint64_t offset, size_t length) OVERRIDE {
    return Status::OK();
  }

  virtual Status PunchHole(uint64_t offset, size_t length) OVERRIDE {
    return Status::OK();
  }

  virtual Status Flush(FlushMode mode, uint64_t offset, size_t length) OVERRIDE {
    return Status::OK();
  }

  virtual Status Sync() OVERRIDE {
    return Status::OK();
  }

  virtual Status Close() OVERRIDE {
    return Status::OK();
  }

  virtual Status Size(uint64_t* size) const OVERRIDE {
    *size = file_->Size();
    return Status::OK();
  }

  virtual const string& filename() const OVERRIDE {
    return file_->filename();
  }

 private:
  FileState* file_;
};

class InMemoryEnv : public EnvWrapper {
 public:
  explicit InMemoryEnv(Env* base_env) : EnvWrapper(base_env) { }

  virtual ~InMemoryEnv() {
    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i) {
      i->second->Unref();
    }
  }

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   gscoped_ptr<SequentialFile>* result) OVERRIDE {
    MutexLock lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    result->reset(new SequentialFileImpl(file_map_[fname]));
    return Status::OK();
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     gscoped_ptr<RandomAccessFile>* result) OVERRIDE {
    return NewRandomAccessFile(RandomAccessFileOptions(), fname, result);
  }

  virtual Status NewRandomAccessFile(const RandomAccessFileOptions& opts,
                                     const std::string& fname,
                                     gscoped_ptr<RandomAccessFile>* result) OVERRIDE {
    MutexLock lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    result->reset(new RandomAccessFileImpl(file_map_[fname]));
    return Status::OK();
  }

  virtual Status NewWritableFile(const WritableFileOptions& opts,
                                 const std::string& fname,
                                 gscoped_ptr<WritableFile>* result) OVERRIDE {
    gscoped_ptr<WritableFileImpl> wf;
    RETURN_NOT_OK(CreateAndRegisterNewFile(fname, opts.mode, &wf));
    result->reset(wf.release());
    return Status::OK();
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 gscoped_ptr<WritableFile>* result) OVERRIDE {
    return NewWritableFile(WritableFileOptions(), fname, result);
  }

  virtual Status NewRWFile(const RWFileOptions& opts,
                           const string& fname,
                           gscoped_ptr<RWFile>* result) OVERRIDE {
    gscoped_ptr<RWFileImpl> rwf;
    RETURN_NOT_OK(CreateAndRegisterNewFile(fname, opts.mode, &rwf));
    result->reset(rwf.release());
    return Status::OK();
  }

  virtual Status NewRWFile(const string& fname,
                           gscoped_ptr<RWFile>* result) OVERRIDE {
    return NewRWFile(RWFileOptions(), fname, result);
  }

  virtual Status NewTempWritableFile(const WritableFileOptions& opts,
                                     const std::string& name_template,
                                     std::string* created_filename,
                                     gscoped_ptr<WritableFile>* result) OVERRIDE {
    // Not very random, but InMemoryEnv is basically a test env.
    Random random(GetCurrentTimeMicros());
    while (true) {
      string stripped;
      if (!TryStripSuffixString(name_template, "XXXXXX", &stripped)) {
        return Status::InvalidArgument("Name template must end with the string XXXXXX",
                                       name_template);
      }
      uint32_t num = random.Next() % 999999; // Ensure it's <= 6 digits long.
      string path = StringPrintf("%s%06u", stripped.c_str(), num);

      MutexLock lock(mutex_);
      if (!ContainsKey(file_map_, path)) {
        CreateAndRegisterNewWritableFileUnlocked(path, result);
        *created_filename = path;
        return Status::OK();
      }
    }
    // Unreachable.
  }

  virtual bool FileExists(const std::string& fname) OVERRIDE {
    MutexLock lock(mutex_);
    return file_map_.find(fname) != file_map_.end();
  }

  virtual Status GetChildren(const std::string& dir,
                             vector<std::string>* result) OVERRIDE {
    MutexLock lock(mutex_);
    result->clear();

    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ++i) {
      const std::string& filename = i->first;

      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' &&
          Slice(filename).starts_with(Slice(dir))) {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }

    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) OVERRIDE {
    MutexLock lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    DeleteFileInternal(fname);
    return Status::OK();
  }

  virtual Status CreateDir(const std::string& dirname) OVERRIDE {
    gscoped_ptr<WritableFile> file;
    return NewWritableFile(dirname, &file);
  }

  virtual Status DeleteDir(const std::string& dirname) OVERRIDE {
    return DeleteFile(dirname);
  }

  virtual Status SyncDir(const std::string& dirname) OVERRIDE {
    return Status::OK();
  }

  virtual Status DeleteRecursively(const std::string& dirname) OVERRIDE {
    CHECK(!dirname.empty());
    string dir(dirname);
    if (dir[dir.size() - 1] != '/') {
      dir.push_back('/');
    }

    MutexLock lock(mutex_);

    for (FileSystem::iterator i = file_map_.begin(); i != file_map_.end(); ) {
      const std::string& filename = i->first;

      if (filename.size() >= dir.size() && Slice(filename).starts_with(Slice(dir))) {
        file_map_.erase(i++);
      } else {
        ++i;
      }
    }

    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) OVERRIDE {
    MutexLock lock(mutex_);
    if (file_map_.find(fname) == file_map_.end()) {
      return Status::IOError(fname, "File not found");
    }

    *file_size = file_map_[fname]->Size();
    return Status::OK();
  }

  virtual Status GetFileSizeOnDisk(const std::string& fname, uint64_t* file_size) OVERRIDE {
    return GetFileSize(fname, file_size);
  }

  virtual Status GetBlockSize(const string& fname, uint64_t* block_size) OVERRIDE {
    // The default for ext3/ext4 filesystems.
    *block_size = 4096;
    return Status::OK();
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) OVERRIDE {
    MutexLock lock(mutex_);
    if (file_map_.find(src) == file_map_.end()) {
      return Status::IOError(src, "File not found");
    }

    DeleteFileInternal(target);
    file_map_[target] = file_map_[src];
    file_map_.erase(src);
    return Status::OK();
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) OVERRIDE {
    *lock = new FileLock;
    return Status::OK();
  }

  virtual Status UnlockFile(FileLock* lock) OVERRIDE {
    delete lock;
    return Status::OK();
  }

  virtual Status GetTestDirectory(std::string* path) OVERRIDE {
    *path = "/test";
    return Status::OK();
  }

  virtual Status Walk(const std::string& root,
                      DirectoryOrder order,
                      const WalkCallback& cb) OVERRIDE {
    LOG(FATAL) << "Not implemented";
  }

  virtual Status Canonicalize(const string& path, string* result) OVERRIDE {
    *result = path;
    return Status::OK();
  }

  virtual Status GetTotalRAMBytes(int64_t* ram) OVERRIDE {
    LOG(FATAL) << "Not implemented";
  }

 private:
  void DeleteFileInternal(const std::string& fname) {
    if (!ContainsKey(file_map_, fname)) {
      return;
    }

    file_map_[fname]->Unref();
    file_map_.erase(fname);
  }

  // Create new internal representation of a writable file.
  void CreateAndRegisterNewWritableFileUnlocked(const string& path,
                                                gscoped_ptr<WritableFile>* result) {
    FileState* file = new FileState(path);
    file->Ref();
    file_map_[path] = file;
    result->reset(new WritableFileImpl(file));
  }

  // Create new internal representation of a file.
  template <typename T>
  Status CreateAndRegisterNewFile(const string& fname,
                                  CreateMode mode,
                                  gscoped_ptr<T>* result) {
    MutexLock lock(mutex_);
    if (ContainsKey(file_map_, fname)) {
      switch (mode) {
        case CREATE_IF_NON_EXISTING_TRUNCATE:
          DeleteFileInternal(fname);
          break; // creates a new file below
        case CREATE_NON_EXISTING:
          return Status::AlreadyPresent(fname, "File already exists");
        case OPEN_EXISTING:
          result->reset(new T(file_map_[fname]));
          return Status::OK();
        default:
          return Status::NotSupported(Substitute("Unknown create mode $0",
                                                 mode));
      }
    } else if (mode == OPEN_EXISTING) {
      return Status::IOError(fname, "File not found");
    }

    FileState* file = new FileState(fname);
    file->Ref();
    file_map_[fname] = file;
    result->reset(new T(file));
    return Status::OK();
  }

  // Map from filenames to FileState objects, representing a simple file system.
  typedef std::map<std::string, FileState*> FileSystem;
  Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_.
};

}  // namespace

Env* NewMemEnv(Env* base_env) {
  return new InMemoryEnv(base_env);
}

} // namespace kudu
