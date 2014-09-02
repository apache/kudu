// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fts.h>
#include <glog/logging.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

#include <deque>

#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/errno.h"
#include "kudu/util/logging.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"

using base::subtle::Atomic64;
using base::subtle::Barrier_AtomicIncrement;
using strings::Substitute;

static __thread uint64_t thread_local_id;
static Atomic64 cur_thread_local_id_;

namespace kudu {

namespace {

// Close file descriptor when object goes out of scope.
class ScopedFdCloser {
 public:
  explicit ScopedFdCloser(int fd)
    : fd_(fd) {
  }

  ~ScopedFdCloser() {
    int err = ::close(fd_);
    if (PREDICT_FALSE(err != 0)) {
      PLOG(WARNING) << "Failed to close fd " << fd_;
    }
  }

 private:
  int fd_;
};

static Status IOError(const std::string& context, int err_number) {
  switch (err_number) {
    case ENOENT:
      return Status::NotFound(context, ErrnoToString(err_number), err_number);
    case EEXIST:
      return Status::AlreadyPresent(context, ErrnoToString(err_number), err_number);
  }
  return Status::IOError(context, ErrnoToString(err_number), err_number);
}

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }

  virtual Status Read(size_t n, Slice* result, uint8_t* scratch) OVERRIDE {
    Status s;
    size_t r = fread_unlocked(scratch, 1, n, file_);
    *result = Slice(scratch, r);
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) OVERRIDE {
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual string ToString() const OVERRIDE { return filename_; }
};

// pread() based random-access
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      uint8_t *scratch) const OVERRIDE {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }

  virtual Status Size(uint64_t *size) const OVERRIDE {
    struct stat st;
    if (fstat(fd_, &st) == -1) {
      return IOError(filename_, errno);
    }
    *size = st.st_size;
    return Status::OK();
  }

  virtual string ToString() const OVERRIDE { return filename_; }
};

// mmap() based random-access
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length)
      : filename_(fname), mmapped_region_(base), length_(length) { }
  virtual ~PosixMmapReadableFile() { munmap(mmapped_region_, length_); }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      uint8_t *scratch) const OVERRIDE {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = Status::IOError(
        Substitute("cannot read $0 bytes at offset $1 (file $2 has length $3)",
                   n, offset, filename_, length_), "", EINVAL);
    } else {
      *result = Slice(reinterpret_cast<uint8_t *>(mmapped_region_) + offset, n);
    }
    return s;
  }

  virtual Status Size(uint64_t *size) const OVERRIDE {
    *size = length_;
    return Status::OK();
  }

  virtual string ToString() const OVERRIDE { return filename_; }
};

// We preallocate up to an extra megabyte and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
//
// TODO since PosixWritableFile brings significant performance
// improvements, consider getting rid of this class and instead
// (perhaps) adding optional buffering to PosixWritableFile.
class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  bool sync_on_close_;
  size_t map_size_;       // How much extra memory to map at a time
  uint8_t *base_;            // The mapped region
  uint8_t *limit_;           // Limit of the mapped region
  uint8_t *dst_;             // Where to write next  (in range [base_,limit_])
  uint8_t *last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
  uint64_t pre_allocated_size_;

  // Have we done an munmap of unsynced data?
  bool pending_sync_;

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) {
    return ((x + y - 1) / y) * y;
  }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  bool UnmapCurrentRegion() {
    bool result = true;
    if (base_ != NULL) {
      if (last_sync_ < limit_) {
        // Defer syncing this data until next Sync() call, if any
        pending_sync_ = true;
      }
      if (munmap(base_, limit_ - base_) != 0) {
        result = false;
      }
      file_offset_ += limit_ - base_;
      base_ = NULL;
      limit_ = NULL;
      last_sync_ = NULL;
      dst_ = NULL;

      // Increase the amount we map the next time, but capped at 2MB
      if (map_size_ < (1<<20)) {
        map_size_ *= 2;
      }
    }
    return result;
  }

  bool MapNewRegion() {
    assert(base_ == NULL);
    uint64_t required_space = file_offset_ + map_size_;
    if (required_space >= pre_allocated_size_) {
      if (ftruncate(fd_, required_space) < 0) {
        return false;
      }
    }
    void* ptr = mmap(NULL, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED,
                     fd_, file_offset_);
    if (ptr == MAP_FAILED) {
      return false;
    }
    base_ = reinterpret_cast<uint8_t *>(ptr);
    limit_ = base_ + map_size_;
    dst_ = base_;
    last_sync_ = base_;
    return true;
  }

  Status DoWritev(const vector<Slice>& data_vector,
                  size_t offset, size_t n) {
    DCHECK_LE(n, IOV_MAX);

    struct iovec iov[n];
    size_t j = 0;
    size_t nbytes = 0;

    for (size_t i = offset; i < offset + n; i++) {
      const Slice& data = data_vector[i];
      iov[j].iov_base = const_cast<uint8_t*>(data.data());
      iov[j].iov_len = data.size();
      nbytes += data.size();
      ++j;
    }

    size_t mem_offset = dst_ - base_;
    size_t actual_offset = file_offset_ + mem_offset;

    size_t left = nbytes;
    while (left > 0) {
      DCHECK_LE(base_, dst_);
      DCHECK_LE(dst_, limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (!UnmapCurrentRegion() ||
            !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }
      size_t n = (left <= avail) ? left : avail;
      dst_ += n;
      left -= n;
    }

    ssize_t written = pwritev(fd_, iov, n, actual_offset);

    if (PREDICT_FALSE(written == -1)) {
      int err = errno;
      return IOError("writev error", err);
    }

    if (PREDICT_FALSE(written != nbytes)) {
      return Status::IOError(
          strings::Substitute("writev error: expected to write $0 bytes, wrote $1 bytes instead",
                              nbytes, written));
    }

    return Status::OK();
  }

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size, bool sync_on_close)
      : filename_(fname),
        fd_(fd),
        page_size_(page_size),
        sync_on_close_(sync_on_close),
        map_size_(Roundup(65536, page_size)),
        base_(NULL),
        limit_(NULL),
        dst_(NULL),
        last_sync_(NULL),
        file_offset_(0),
        pre_allocated_size_(0),
        pending_sync_(false) {
    assert((page_size & (page_size - 1)) == 0);
  }


  ~PosixMmapFile() {
    if (fd_ >= 0) {
      WARN_NOT_OK(PosixMmapFile::Close(),
                  "Failed to close mmapped file");
    }
  }

  virtual Status PreAllocate(uint64_t size) OVERRIDE {
    uint64_t offset = std::max(file_offset_, pre_allocated_size_);
    if (fallocate(fd_, 0, offset, size) < 0) {
      return IOError(filename_, errno);
    }
    // Make sure that we set pre_allocated_size so that the file
    // doesn't get truncated on MapNewRegion().
    pre_allocated_size_ = offset + size;
    return Status::OK();
  }

  virtual Status Append(const Slice& data) OVERRIDE {
    const uint8_t *src = data.data();
    size_t left = data.size();
    while (left > 0) {
      assert(base_ <= dst_);
      assert(dst_ <= limit_);
      size_t avail = limit_ - dst_;
      if (avail == 0) {
        if (!UnmapCurrentRegion() ||
            !MapNewRegion()) {
          return IOError(filename_, errno);
        }
      }

      size_t n = (left <= avail) ? left : avail;
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
    }
    return Status::OK();
  }

  // Uses pwritev to perform scatter-gather I/O. Note that on systems
  // other than Linux, it may be neccessary to call Sync() after each
  // AppendVector() if we also plan to read from this file.
  virtual Status AppendVector(const vector<Slice>& data_vector) OVERRIDE {
    // TODO (perf) : investigate what the optimal number of vectors to
    //               pass to writev at one time is or if it matters.
    static const size_t kIovMaxElements = IOV_MAX;

    for (size_t i = 0; i < data_vector.size(); i += kIovMaxElements) {
      size_t n = std::min(data_vector.size() - i, kIovMaxElements);
      RETURN_NOT_OK(DoWritev(data_vector, i, n));
    }

    pending_sync_ = true;

    return Status::OK();
  }

  virtual Status Close() OVERRIDE {
    Status s;
    size_t unused = limit_ - dst_;
    if (!UnmapCurrentRegion()) {
      s = IOError(filename_, errno);
    } else if (unused > 0) {
      // Trim the extra space at the end of the file
      if (ftruncate(fd_, file_offset_ - unused) < 0) {
        s = IOError(filename_, errno);
      }
      pending_sync_ = true;
    }

     if (sync_on_close_) {
      Status sync_status = Sync();
      if (!sync_status.ok()) {
        LOG(ERROR) << "Unable to Sync " << filename_ << ": " << sync_status.ToString();
        if (s.ok()) {
          s = sync_status;
        }
      }
     }

    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    fd_ = -1;
    base_ = NULL;
    limit_ = NULL;
    return s;
  }

  virtual Status Flush() OVERRIDE {
    return Status::OK();
  }

  virtual Status Sync() OVERRIDE {
    Status s;

    if (pending_sync_) {
      // Some unmapped data was not synced
      pending_sync_ = false;
      if (fdatasync(fd_) < 0) {
        s = IOError(filename_, errno);
      }
    }

    if (dst_ > last_sync_) {
      // Find the beginnings of the pages that contain the first and last
      // bytes to be synced.
      size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
      size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
      last_sync_ = dst_;
      if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
        s = IOError(filename_, errno);
      }
    }

    return s;
  }

  virtual Status SyncParentDir() OVERRIDE {
    return Env::Default()->SyncDir(DirName(filename_));
  }

  virtual uint64_t Size() const OVERRIDE {
    return file_offset_ + (dst_ - base_);
  }

  virtual string ToString() const OVERRIDE { return filename_; }
};

// Use non-memory mapped POSIX files to write data to a file.
//
// TODO (perf) investigate zeroing a pre-allocated allocated area in
// order to further improve Sync() performance.
class PosixWritableFile : public WritableFile {
 public:
  PosixWritableFile(const std::string& fname,
                    int fd,
                    bool sync_on_close)
      : filename_(fname),
        fd_(fd),
        sync_on_close_(sync_on_close),
        filesize_(0),
        pre_allocated_size_(0),
        pending_sync_type_(NONE) {
  }

  ~PosixWritableFile() {
    if (fd_ >= 0) {
      WARN_NOT_OK(Close(), "Failed to close " + filename_);
    }
  }

  virtual Status Append(const Slice& data) OVERRIDE {
    const uint8_t* src = data.data();
    size_t left = data.size();

    // If we're writing beyond the pre-allocated portion of the file,
    // make sure fsync() is executed on the next Sync(). Otherwise,
    // the next call to Sync() will invoke fdatasync().
    if (PREDICT_FALSE(filesize_ + left > pre_allocated_size_)) {
      pending_sync_type_ = FSYNC;
    } else {
      pending_sync_type_ = FDATASYNC;
    }

    while (left != 0) {
      ssize_t done = write(fd_, src, left);
      if (done < 0) {
        return IOError(filename_, errno);
      }

      left -= done;
      src += done;
    }

    filesize_ += data.size();

    return Status::OK();
  }

  virtual Status AppendVector(const vector<Slice>& data_vector) OVERRIDE {
    static const size_t kIovMaxElements = IOV_MAX;

    for (size_t i = 0; i < data_vector.size(); i += kIovMaxElements) {
      size_t n = std::min(data_vector.size() - i, kIovMaxElements);
      RETURN_NOT_OK(DoWritev(data_vector, i, n));
    }

    if (PREDICT_FALSE(filesize_ > pre_allocated_size_)) {
      pending_sync_type_ = FSYNC;
    } else {
      pending_sync_type_ = FDATASYNC;
    }

    return Status::OK();
  }

  virtual Status PreAllocate(uint64_t size) OVERRIDE {
    uint64_t offset = std::max(filesize_, pre_allocated_size_);
    if (fallocate(fd_, 0, offset, size) < 0) {
      if (errno == EOPNOTSUPP) {
        KLOG_FIRST_N(WARNING, 1) << "The filesystem does not support fallocate().";
      } else if (errno == ENOSYS) {
        KLOG_FIRST_N(WARNING, 1) << "The kernel does not implement fallocate().";
      } else {
        return IOError(filename_, errno);
      }
    } else {
      pre_allocated_size_ = filesize_ + size;
    }
    return Status::OK();
  }

  virtual Status Close() OVERRIDE {
    Status s;

    // If we've allocated more space than we used, truncate to the
    // actual size of the file and perform fsync().
    if (filesize_ < pre_allocated_size_) {
      if (ftruncate(fd_, filesize_) < 0) {
        s = IOError(filename_, errno);
        pending_sync_type_ = FSYNC;
      }
    }

    if (sync_on_close_) {
      Status sync_status = Sync();
      if (!sync_status.ok()) {
        LOG(ERROR) << "Unable to Sync " << filename_ << ": " << sync_status.ToString();
        if (s.ok()) {
          s = sync_status;
        }
      }
    }

    if (close(fd_) < 0) {
      if (s.ok()) {
        s = IOError(filename_, errno);
      }
    }

    fd_ = -1;
    return s;
  }

  virtual Status Flush() OVERRIDE {
    return Status::OK();
  }

  virtual Status Sync() OVERRIDE {
    LOG_SLOW_EXECUTION(WARNING, 1000, Substitute("sync call for $0", filename_)) {
      if (pending_sync_type_ == FSYNC) {
        if (fsync(fd_) < 0) {
          return IOError(filename_, errno);
        }
      } else if (pending_sync_type_ == FDATASYNC) {
        if (fdatasync(fd_) <  0) {
          return IOError(filename_, errno);
        }
      }
    }
    pending_sync_type_ = NONE;
    return Status::OK();
  }

  virtual Status SyncParentDir() OVERRIDE {
    return Env::Default()->SyncDir(DirName(filename_));
  }

  virtual uint64_t Size() const OVERRIDE {
    return filesize_;
  }

  virtual string ToString() const OVERRIDE { return filename_; }

 private:

  Status DoWritev(const vector<Slice>& data_vector,
                  size_t offset, size_t n) {
    DCHECK_LE(n, IOV_MAX);

    struct iovec iov[n];
    size_t j = 0;
    size_t nbytes = 0;

    for (size_t i = offset; i < offset + n; i++) {
      const Slice& data = data_vector[i];
      iov[j].iov_base = const_cast<uint8_t*>(data.data());
      iov[j].iov_len = data.size();
      nbytes += data.size();
      ++j;
    }

    ssize_t written = writev(fd_, iov, n);

    if (PREDICT_FALSE(written == -1)) {
      int err = errno;
      return IOError("writev error", err);
    }

    filesize_ += written;

    if (PREDICT_FALSE(written != nbytes)) {
      return Status::IOError(
          strings::Substitute("writev error: expected to write $0 bytes, wrote $1 bytes instead",
                              nbytes, written));
    }

    return Status::OK();
  }

  const std::string filename_;
  int fd_;
  bool sync_on_close_;
  uint64_t filesize_;
  uint64_t pre_allocated_size_;

  enum SyncType {
    NONE,
    FSYNC,
    FDATASYNC
  };
  SyncType pending_sync_type_;
};

static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK);
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    fprintf(stderr, "Destroying Env::Default()\n");
    exit(1);
  }

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) OVERRIDE {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) OVERRIDE {
    *result = NULL;
    uint64_t file_size;
    RETURN_NOT_OK_PREPEND(GetFileSize(fname, &file_size),
                          Substitute("Unable to get size of file $0", fname));
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      return IOError(fname, errno);
    }
    Status s = Status::OK();
    if (file_size > 0 && sizeof(void*) >= 8) {
      // Use mmap when virtual address-space is plentiful.
      void* base = mmap(NULL, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (base != MAP_FAILED) {
        *result = new PosixMmapReadableFile(fname, base, file_size);
      } else {
        s = IOError(Substitute("mmap() failed on file $0", fname), errno);
      }
      close(fd);
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) OVERRIDE {
    return NewWritableFile(WritableFileOptions(), fname, result);
  }

  virtual Status NewWritableFile(const WritableFileOptions& opts,
                                 const std::string& fname,
                                 WritableFile** result) OVERRIDE {
    Status s;
    const int fd = open(fname.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (fd < 0) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      gscoped_ptr<WritableFile> writable_file;
      InstantiateNewWritableFile(fname, fd, opts, &writable_file);
      *result = writable_file.release();
    }
    return s;
  }

  virtual Status NewTempWritableFile(const WritableFileOptions& opts,
                                     const std::string& name_template,
                                     std::string* created_filename,
                                     gscoped_ptr<WritableFile>* result) OVERRIDE {
    gscoped_ptr<char[]> fname(new char[name_template.size() + 1]);
    ::snprintf(fname.get(), name_template.size() + 1, "%s", name_template.c_str());
    const int fd = ::mkstemp(fname.get());
    if (fd < 0) {
      return IOError(Substitute("Call to mkstemp() failed on name template $0", name_template),
                     errno);
    }
    *created_filename = fname.get();
    InstantiateNewWritableFile(*created_filename, fd, opts, result);
    return Status::OK();
  }

  virtual bool FileExists(const std::string& fname) OVERRIDE {
    return access(fname.c_str(), F_OK) == 0;
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) OVERRIDE {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    // TODO: lint: Consider using readdir_r(...) instead of readdir(...) for improved thread safety.
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  virtual Status DeleteFile(const std::string& fname) OVERRIDE {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  };

  virtual Status CreateDir(const std::string& name) OVERRIDE {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status DeleteDir(const std::string& name) OVERRIDE {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  };

  virtual Status SyncDir(const std::string& dirname) OVERRIDE {
    int dir_fd;
    if ((dir_fd = open(dirname.c_str(), O_DIRECTORY|O_RDONLY)) == -1) {
      return IOError(dirname, errno);
    }
    ScopedFdCloser fd_closer(dir_fd);
    if (fsync(dir_fd) != 0) {
      return IOError(dirname, errno);
    }
    return Status::OK();
  }


  virtual Status DeleteRecursively(const std::string &name) OVERRIDE {
    // Some sanity checks
    CHECK_NE(name, "/");
    CHECK_NE(name, "./");
    CHECK_NE(name, ".");
    CHECK_NE(name, "");

    // FTS requires a non-const copy of the name. strdup it and free() when
    // we leave scope.
    gscoped_ptr<char, FreeDeleter> name_dup(strdup(name.c_str()));
    char *(paths[]) = { name_dup.get(), NULL };

    // FTS_NOCHDIR is important here to make this thread-safe.
    gscoped_ptr<FTS, FtsCloser> tree(
      fts_open(paths, FTS_PHYSICAL | FTS_XDEV | FTS_NOCHDIR, NULL));
    if (!tree.get()) {
      return IOError(name, errno);
    }

    FTSENT *ent = NULL;
    bool had_errors = false;
    while ((ent = fts_read(tree.get())) != NULL) {
      Status s;
      switch (ent->fts_info) {
        case FTS_D: // Directory in pre-order
          break;
        case FTS_DP: // Directory in post-order
          s = DeleteDir(ent->fts_accpath);
          if (!s.ok()) {
            LOG(WARNING) << "Couldn't delete " << ent->fts_path << ": " << s.ToString();
            had_errors = true;
          }
          break;
        case FTS_F:         // A regular file
        case FTS_SL:        // A symbolic link
        case FTS_SLNONE:    // A broken symbolic link
        case FTS_DEFAULT:   // Unknown type of file
          s = DeleteFile(ent->fts_accpath);
          if (!s.ok()) {
            LOG(WARNING) << "Couldn't delete file " << ent->fts_path << ": " << s.ToString();
            had_errors = true;
          }
          break;
        case FTS_ERR:
          LOG(WARNING) << "Unable to access file " << ent->fts_path << " for deletion: "
                       << strerror(ent->fts_errno);
          had_errors = true;
          break;

        default:
          LOG(WARNING) << "Unable to access file " << ent->fts_path << " for deletion (code "
                       << ent->fts_info << ")";
          break;
      }
    }

    if (had_errors) {
      return Status::IOError(name, "One or more errors occurred");
    }
    return Status::OK();
  }

  virtual Status GetFileSize(const std::string& fname, uint64_t* size) OVERRIDE {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

  virtual Status RenameFile(const std::string& src, const std::string& target) OVERRIDE {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) {
      result = IOError(src, errno);
    }
    return result;
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) OVERRIDE {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (LockOrUnlock(fd, true) == -1) {
      result = IOError("lock " + fname, errno);
      close(fd);
    } else {
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      *lock = my_lock;
    }
    return result;
  }

  virtual Status UnlockFile(FileLock* lock) OVERRIDE {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

  virtual Status GetTestDirectory(std::string* result) OVERRIDE {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/kudutest-%d", static_cast<int>(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    ignore_result(CreateDir(*result));
    return Status::OK();
  }

  virtual uint64_t gettid() OVERRIDE {
    // Platform-independent thread ID.  We can't use pthread_self here,
    // because that function returns a totally opaque ID, which can't be
    // compared via normal means.
    if (thread_local_id == 0) {
      thread_local_id = Barrier_AtomicIncrement(&cur_thread_local_id_, 1);
    }
    return thread_local_id;
  }

  virtual uint64_t NowMicros() OVERRIDE {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) OVERRIDE {
    usleep(micros);
  }

  virtual Status GetExecutablePath(string* path) OVERRIDE {
    int size = 64;
    while (true) {
      gscoped_ptr<char[]> buf(new char[size]);
      ssize_t rc = readlink("/proc/self/exe", buf.get(), size);
      if (rc == -1) {
        return Status::IOError("Unable to determine own executable path", "",
                               errno);
      }
      if (rc < size) {
        path->assign(&buf[0], rc);
        break;
      }
      // Buffer wasn't large enough
      size *= 2;
    }
    return Status::OK();
  }

 private:
  // gscoped_ptr Deleter implementation for fts_close
  struct FtsCloser {
    void operator()(FTS *fts) const {
      if (fts) { fts_close(fts); }
    }
  };

  void InstantiateNewWritableFile(const std::string& fname,
                                  int fd,
                                  const WritableFileOptions& opts,
                                  gscoped_ptr<WritableFile>* result) {
    if (opts.mmap_file) {
      result->reset(new PosixMmapFile(fname, fd, page_size_, opts.sync_on_close));
    } else {
      result->reset(new PosixWritableFile(fname, fd, opts.sync_on_close));
    }
  }

  size_t page_size_;
};

PosixEnv::PosixEnv() : page_size_(getpagesize()) {
}

}  // namespace

static pthread_once_t once = PTHREAD_ONCE_INIT;
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);
  return default_env;
}

}  // namespace kudu
