// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the kudu implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kudu/gutil/callback_forward.h"
#include "kudu/gutil/macros.h"
#include "kudu/util/status.h"

namespace kudu {

class faststring;
class FileLock;
class RandomAccessFile;
class RWFile;
class SequentialFile;
class Slice;
class WritableFile;

struct RandomAccessFileOptions;
struct RWFileOptions;
struct WritableFileOptions;

template <typename T>
class ArrayView;

// Returned by Env::GetSpaceInfo().
struct SpaceInfo {
  int64_t capacity_bytes; // Capacity of a filesystem, in bytes.
  int64_t free_bytes;     // Bytes available to non-privileged processes.
};

class Env {
 public:
  // Governs if/how the file is created.
  //
  // enum value                      | file exists       | file does not exist
  // --------------------------------+-------------------+--------------------
  // CREATE_IF_NON_EXISTING_TRUNCATE | opens + truncates | creates
  // CREATE_NON_EXISTING             | fails             | creates
  // OPEN_EXISTING                   | opens             | fails
  enum CreateMode {
    CREATE_IF_NON_EXISTING_TRUNCATE,
    CREATE_NON_EXISTING,
    OPEN_EXISTING
  };

  Env() { }
  virtual ~Env();

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to kudu and must never be deleted.
  static Env* Default();

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores NULL in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores NULL in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result) = 0;

  // Like the previous NewRandomAccessFile, but allows options to be specified.
  virtual Status NewRandomAccessFile(const RandomAccessFileOptions& opts,
                                     const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result) = 0;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores NULL in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result) = 0;


  // Like the previous NewWritableFile, but allows options to be
  // specified.
  virtual Status NewWritableFile(const WritableFileOptions& opts,
                                 const std::string& fname,
                                 std::unique_ptr<WritableFile>* result) = 0;

  // Creates a new WritableFile provided the name_template parameter.
  // The last six characters of name_template must be "XXXXXX" and these are
  // replaced with a string that makes the filename unique.
  // The resulting created filename, if successful, will be stored in the
  // created_filename out parameter.
  // The file is created with permissions 0600, that is, read plus write for
  // owner only. The implementation will create the file in a secure manner,
  // and will return an error Status if it is unable to open the file.
  virtual Status NewTempWritableFile(const WritableFileOptions& opts,
                                     const std::string& name_template,
                                     std::string* created_filename,
                                     std::unique_ptr<WritableFile>* result) = 0;

  // Creates a new readable and writable file. If a file with the same name
  // already exists on disk, it is deleted.
  //
  // Some of the methods of the new file may be accessed concurrently,
  // while others are only safe for access by one thread at a time.
  virtual Status NewRWFile(const std::string& fname,
                           std::unique_ptr<RWFile>* result) = 0;

  // Like the previous NewRWFile, but allows options to be specified.
  virtual Status NewRWFile(const RWFileOptions& opts,
                           const std::string& fname,
                           std::unique_ptr<RWFile>* result) = 0;

  // Same as abovoe for NewTempWritableFile(), but for an RWFile.
  virtual Status NewTempRWFile(const RWFileOptions& opts,
                               const std::string& name_template,
                               std::string* created_filename,
                               std::unique_ptr<RWFile>* res) = 0;

  // Returns true iff the named file exists.
  virtual bool FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;

  // Delete the named file.
  virtual Status DeleteFile(const std::string& fname) = 0;

  // Create the specified directory.
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Delete the specified directory.
  virtual Status DeleteDir(const std::string& dirname) = 0;

  // Return the current working directory.
  virtual Status GetCurrentWorkingDir(std::string* cwd) const = 0;

  // Change the current working directory.
  virtual Status ChangeDir(const std::string& dest) = 0;

  // Synchronize the entry for a specific directory.
  virtual Status SyncDir(const std::string& dirname) = 0;

  // Recursively delete the specified directory.
  // This should operate safely, not following any symlinks, etc.
  virtual Status DeleteRecursively(const std::string &dirname) = 0;

  // Store the logical size of fname in *file_size.
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Store the physical size of fname in *file_size.
  //
  // This differs from GetFileSize() in that it returns the actual amount
  // of space consumed by the file, not the user-facing file size.
  virtual Status GetFileSizeOnDisk(const std::string& fname, uint64_t* file_size) = 0;

  // Walk 'root' recursively, looking up the amount of space used by each file
  // as reported by GetFileSizeOnDisk(), storing the grand total in 'bytes_used'.
  virtual Status GetFileSizeOnDiskRecursively(const std::string& root, uint64_t* bytes_used) = 0;

  // Returns the modified time of the file in microseconds.
  //
  // The timestamp is a 'system' timestamp, and is not guaranteed to be
  // monotonic, or have any other consistency properties. The granularity of the
  // timestamp is not guaranteed, and may be as high as 1 second on some
  // platforms. The timestamp is not guaranteed to be anchored to any particular
  // epoch.
  virtual Status GetFileModifiedTime(const std::string& fname, int64_t* timestamp) = 0;

  // Store the block size of the filesystem where fname resides in
  // *block_size. fname must exist but it may be a file or a directory.
  virtual Status GetBlockSize(const std::string& fname, uint64_t* block_size) = 0;

  // Determine the capacity and number of bytes free on the filesystem
  // specified by 'path'. "Free space" accounting on the underlying filesystem
  // may be more coarse than single bytes.
  virtual Status GetSpaceInfo(const std::string& path, SpaceInfo* space_info) = 0;

  // Rename file src to target.
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores NULL in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) = 0;

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Returns the number of micro-seconds since some fixed point in time. Only
  // useful for computing deltas of time.
  virtual uint64_t NowMicros() = 0;

  // Sleep/delay the thread for the perscribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;

  // Get caller's thread id.
  virtual uint64_t gettid() = 0;

  // Return the full path of the currently running executable.
  virtual Status GetExecutablePath(std::string* path) = 0;

  // Checks if the file is a directory. Returns an error if it doesn't
  // exist, otherwise writes true or false into 'is_dir' appropriately.
  virtual Status IsDirectory(const std::string& path, bool* is_dir) = 0;

  // The kind of file found during a walk. Note that symbolic links are
  // reported as FILE_TYPE.
  enum FileType {
    DIRECTORY_TYPE,
    FILE_TYPE,
  };

  // Called for each file/directory in the walk.
  //
  // The first argument is the type of file.
  // The second is the dirname of the file.
  // The third is the basename of the file.
  //
  // Returning an error won't halt the walk, but it will cause it to return
  // with an error status when it's done.
  typedef Callback<Status(FileType, const std::string&, const std::string&)> WalkCallback;

  // Whether to walk directories in pre-order or post-order.
  enum DirectoryOrder {
    PRE_ORDER,
    POST_ORDER,
  };

  // Walk the filesystem subtree from 'root' down, invoking 'cb' for each
  // file or directory found, including 'root'.
  //
  // The walk will not cross filesystem boundaries. It won't change the
  // working directory, nor will it follow symbolic links.
  virtual Status Walk(const std::string& root,
                      DirectoryOrder order,
                      const WalkCallback& cb) = 0;

  // Finds paths on the filesystem matching a pattern.
  //
  // The found pathnames are added to the 'paths' vector. If no pathnames are
  // found matching the pattern, no paths are added to the vector and an OK
  // status is returned.
  virtual Status Glob(const std::string& path_pattern, std::vector<std::string>* paths) = 0;

  // Canonicalize 'path' by applying the following conversions:
  // - Converts a relative path into an absolute one using the cwd.
  // - Converts '.' and '..' references.
  // - Resolves all symbolic links.
  //
  // All directory entries in 'path' must exist on the filesystem.
  virtual Status Canonicalize(const std::string& path, std::string* result) = 0;

  // Gets the total amount of RAM installed on this machine.
  virtual Status GetTotalRAMBytes(int64_t* ram) = 0;

  enum class ResourceLimitType {
    // The maximum number of file descriptors that this process can have open
    // at any given time.
    //
    // Corresponds to RLIMIT_NOFILE on UNIX platforms.
    OPEN_FILES_PER_PROCESS,

    // The maximum number of threads (or processes) that this process's
    // effective user ID may have spawned and running at any given time.
    //
    // Corresponds to RLIMIT_NPROC on UNIX platforms.
    RUNNING_THREADS_PER_EUID,
  };

  // Gets the process' current limit for the given resource type. If there is
  // no limit, returns kuint64max.
  //
  // On UNIX platforms, this is equivalent to the resource's soft limit.
  virtual uint64_t GetResourceLimit(ResourceLimitType t) = 0;

  // Increases the resource limit by as much as possible.
  //
  // On UNIX platforms, this means increasing the resource's soft limit (the
  // limit actually enforced by the kernel) to be equal to the hard limit.
  virtual void IncreaseResourceLimit(ResourceLimitType t) = 0;

  // Checks whether the given path resides on an ext2, ext3, or ext4
  // filesystem.
  //
  // On success, 'result' contains the answer. On failure, 'result' is unset.
  virtual Status IsOnExtFilesystem(const std::string& path, bool* result) = 0;

  // Checks whether the given path resides on an xfs filesystem.
  //
  // On success, 'result' contains the answer. On failure, 'result' is unset.
  virtual Status IsOnXfsFilesystem(const std::string& path, bool* result) = 0;

  // Gets the kernel release string for this machine.
  virtual std::string GetKernelRelease() = 0;

  // Ensure that the file with the given path has permissions which adhere
  // to the current configured umask (from flags.h). If the permissions are
  // wider than the current umask, then a warning is logged and the permissions
  // are fixed.
  //
  // Returns a bad Status if the file does not exist or the permissions cannot
  // be changed.
  virtual Status EnsureFileModeAdheresToUmask(const std::string& path) = 0;

  // Checks whether the given path has world-readable permissions.
  //
  // On success, 'result' contains the answer. On failure, 'result' is unset.
  virtual Status IsFileWorldReadable(const std::string& path, bool* result) = 0;

  // Special string injected into file-growing operations' random failures
  // (if enabled).
  //
  // Only useful for tests.
  static const char* const kInjectedFailureStatusMsg;

 private:
  // No copying allowed
  Env(const Env&);
  void operator=(const Env&);
};

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
  SequentialFile() { }
  virtual ~SequentialFile();

  // Read up to "result.size" bytes from the file.
  // Sets "result.data" to the data that was read.
  //
  // If an error was encountered, returns a non-OK status
  // and the contents of "result" are invalid.
  //
  // REQUIRES: External synchronization
  virtual Status Read(Slice* result) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual Status Skip(uint64_t n) = 0;

  // Returns the filename provided when the SequentialFile was constructed.
  virtual const std::string& filename() const = 0;
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() { }
  virtual ~RandomAccessFile();

  // Read "result.size" bytes from the file starting at "offset".
  // Copies the resulting data into "result.data".
  //
  // If an error was encountered, returns a non-OK status.
  //
  // This method will internally retry on EINTR and "short reads" in order to
  // fully read the requested number of bytes. In the event that it is not
  // possible to read exactly 'length' bytes, an IOError is returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, Slice result) const = 0;

  // Reads up to the "results" aggregate size, based on each Slice's "size",
  // from the file starting at 'offset'. The Slices must point to already-allocated
  // buffers for the data to be written to.
  //
  // If an error was encountered, returns a non-OK status.
  //
  // This method will internally retry on EINTR and "short reads" in order to
  // fully read the requested number of bytes. In the event that it is not
  // possible to read exactly 'length' bytes, an IOError is returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status ReadV(uint64_t offset, ArrayView<Slice> results) const = 0;

  // Returns the size of the file
  virtual Status Size(uint64_t *size) const = 0;

  // Returns the filename provided when the RandomAccessFile was constructed.
  virtual const std::string& filename() const = 0;

  // Returns the approximate memory usage of this RandomAccessFile including
  // the object itself.
  virtual size_t memory_footprint() const = 0;
};

// Creation-time options for WritableFile
struct WritableFileOptions {
  // Call Sync() during Close().
  bool sync_on_close;

  // See CreateMode for details.
  Env::CreateMode mode;

  WritableFileOptions()
    : sync_on_close(false),
      mode(Env::CREATE_IF_NON_EXISTING_TRUNCATE) { }
};

// Options specified when a file is opened for random access.
struct RandomAccessFileOptions {
  RandomAccessFileOptions() {}
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile {
 public:
  enum FlushMode {
    FLUSH_SYNC,
    FLUSH_ASYNC
  };

  WritableFile() { }
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;

  // If possible, uses scatter-gather I/O to efficiently append
  // multiple buffers to a file. Otherwise, falls back to regular I/O.
  //
  // For implementation specific quirks and details, see comments in
  // implementation source code (e.g., env_posix.cc)
  virtual Status AppendV(ArrayView<const Slice> data) = 0;

  // Pre-allocates 'size' bytes for the file in the underlying filesystem.
  // size bytes are added to the current pre-allocated size or to the current
  // offset, whichever is bigger. In no case is the file truncated by this
  // operation.
  //
  // On some implementations, preallocation is done without initializing the
  // contents of the data blocks (as opposed to writing zeroes), requiring no
  // IO to the data blocks.
  //
  // In no case is the file truncated by this operation.
  virtual Status PreAllocate(uint64_t size) = 0;

  virtual Status Close() = 0;

  // Flush all dirty data (not metadata) to disk.
  //
  // If the flush mode is synchronous, will wait for flush to finish and
  // return a meaningful status.
  virtual Status Flush(FlushMode mode) = 0;

  virtual Status Sync() = 0;

  virtual uint64_t Size() const = 0;

  // Returns the filename provided when the WritableFile was constructed.
  virtual const std::string& filename() const = 0;

 private:
  // No copying allowed
  WritableFile(const WritableFile&);
  void operator=(const WritableFile&);
};

// Creation-time options for RWFile
struct RWFileOptions {
  // Call Sync() during Close().
  bool sync_on_close;

  // See CreateMode for details.
  Env::CreateMode mode;

  RWFileOptions()
    : sync_on_close(false),
      mode(Env::CREATE_IF_NON_EXISTING_TRUNCATE) { }
};

// A file abstraction for both reading and writing. No notion of a built-in
// file offset is ever used; instead, all operations must provide an
// explicit offset.
//
// All operations are safe for concurrent use by multiple threads (unless
// noted otherwise) bearing in mind the usual filesystem coherency guarantees
// (e.g. two threads that write concurrently to the same file offset will
// probably yield garbage).
class RWFile {
 public:
  enum FlushMode {
    FLUSH_SYNC,
    FLUSH_ASYNC
  };

  RWFile() {
  }

  virtual ~RWFile();

  // Read "result.size" bytes from the file starting at "offset".
  // Copies the resulting data into "result.data".
  // If an error was encountered, returns a non-OK status.
  //
  // This method will internally retry on EINTR and "short reads" in order to
  // fully read the requested number of bytes. In the event that it is not
  // possible to read exactly 'length' bytes, an IOError is returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, Slice result) const = 0;

  // Reads up to the "results" aggregate size, based on each Slice's "size",
  // from the file starting at 'offset'. The Slices must point to already-allocated
  // buffers for the data to be written to.
  //
  // If an error was encountered, returns a non-OK status.
  //
  // This method will internally retry on EINTR and "short reads" in order to
  // fully read the requested number of bytes. In the event that it is not
  // possible to read exactly 'length' bytes, an IOError is returned.
  //
  // Safe for concurrent use by multiple threads.
  virtual Status ReadV(uint64_t offset, ArrayView<Slice> results) const = 0;

  // Writes 'data' to the file position given by 'offset'.
  virtual Status Write(uint64_t offset, const Slice& data) = 0;

  // Writes the 'data' slices to the file position given by 'offset'.
  virtual Status WriteV(uint64_t offset, ArrayView<const Slice> data) = 0;

  // Preallocates 'length' bytes for the file in the underlying filesystem
  // beginning at 'offset'. It is safe to preallocate the same range
  // repeatedly; this is an idempotent operation.
  //
  // On some implementations, preallocation is done without initializing the
  // contents of the data blocks (as opposed to writing zeroes), requiring no
  // IO to the data blocks. On such implementations, this is much faster than
  // using Truncate() to increase the file size.
  //
  // In no case is the file truncated by this operation.
  //
  // 'mode' controls whether the file's logical size grows to include the
  // preallocated space, or whether it remains the same.
  enum PreAllocateMode {
    CHANGE_FILE_SIZE,
    DONT_CHANGE_FILE_SIZE
  };
  virtual Status PreAllocate(uint64_t offset,
                             size_t length,
                             PreAllocateMode mode) = 0;

  // Truncate the file. If 'new_size' is less than the previous file size, the
  // extra data will be lost. If 'new_size' is greater than the previous file
  // size, the file length is extended, and the extended portion will contain
  // null bytes ('\0').
  virtual Status Truncate(uint64_t length) = 0;

  // Deallocates space given by 'offset' and length' from the file,
  // effectively "punching a hole" in it. The space will be reclaimed by
  // the filesystem and reads to that range will return zeroes. Useful
  // for making whole files sparse.
  //
  // Filesystems that don't implement this will return an error.
  virtual Status PunchHole(uint64_t offset, size_t length) = 0;

  // Flushes the range of dirty data (not metadata) given by 'offset' and
  // 'length' to disk. If length is 0, all bytes from 'offset' to the end
  // of the file are flushed.
  //
  // If the flush mode is synchronous, will wait for flush to finish and
  // return a meaningful status.
  virtual Status Flush(FlushMode mode, uint64_t offset, size_t length) = 0;

  // Synchronously flushes all dirty file data and metadata to disk. Upon
  // returning successfully, all previously issued file changes have been
  // made durable.
  virtual Status Sync() = 0;

  // Closes the file, optionally calling Sync() on it if the file was
  // created with the sync_on_close option enabled.
  //
  // Not thread-safe.
  virtual Status Close() = 0;

  // Retrieves the file's size.
  virtual Status Size(uint64_t* size) const = 0;

  // Retrieve a map of the file's live extents.
  //
  // Each map entry is an offset and size representing a section of live file
  // data. Any byte offset not contained in a map entry implicitly belongs to a
  // "hole" in the (sparse) file.
  //
  // If the underlying filesystem does not support extents, map entries
  // represent runs of adjacent fixed-size filesystem blocks instead. If the
  // platform doesn't support fetching extents at all, a NotSupported status
  // will be returned.
  typedef std::map<uint64_t, uint64_t> ExtentMap;
  virtual Status GetExtentMap(ExtentMap* out) const = 0;

  // Returns the filename provided when the RWFile was constructed.
  virtual const std::string& filename() const = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(RWFile);
};

// Identifies a locked file.
class FileLock {
 public:
  FileLock() { }
  virtual ~FileLock();
 private:
  // No copying allowed
  FileLock(const FileLock&);
  void operator=(const FileLock&);
};

// A utility routine: write "data" to the named file.
extern Status WriteStringToFile(Env* env, const Slice& data,
                                const std::string& fname);

// A utility routine: read contents of named file into *data
extern Status ReadFileToString(Env* env, const std::string& fname,
                               faststring* data);

// Overloaded operator for printing Env::ResourceLimitType.
std::ostream& operator<<(std::ostream& o, Env::ResourceLimitType t);

}  // namespace kudu

#endif  // STORAGE_LEVELDB_INCLUDE_ENV_H_
