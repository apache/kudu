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

#include <stdint.h>

#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "kudu/gutil/callback.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/monotime.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class ThreadPool;

// We pass around the results of canonicalization to indicate to the
// directory manager which, if any, failed to canonicalize.
//
// TODO(awong): move the canonicalization of directories into the
// directory manager so we can avoid this extra plumbing.
struct CanonicalizedRootAndStatus {
  std::string path;
  Status status;
};
typedef std::vector<CanonicalizedRootAndStatus> CanonicalizedRootsList;

namespace fs {

typedef std::unordered_map<int, std::string> UuidByUuidIndexMap;
typedef std::unordered_map<std::string, int> UuidIndexByUuidMap;
class DirInstanceMetadataFile;

// Defines the behavior when opening a directory manager that has an
// inconsistent or incomplete set of instance files.
enum UpdateInstanceBehavior {
  // If the directories don't match the on-disk dir sets, update the on-disk
  // data to match if not in read-only mode.
  UPDATE_AND_IGNORE_FAILURES,

  // Like UPDATE_AND_IGNORE_FAILURES, but will return an error if any of the updates to the
  // on-disk files fail.
  UPDATE_AND_ERROR_ON_FAILURE,

  // If the directories don't match the on-disk dir sets, continue without
  // updating the on-disk data.
  DONT_UPDATE
};

struct DirMetrics {
  scoped_refptr<AtomicGauge<uint64_t>> dirs_failed;
  scoped_refptr<AtomicGauge<uint64_t>> dirs_full;
};

// Detected type of filesystem.
enum class FsType {
  // ext2, ext3, or ext4.
  EXT,

  // SGI xfs.
  XFS,

  // None of the above.
  OTHER
};

// Representation of a directory (e.g. a data directory).
class Dir {
 public:
  Dir(Env* env,
      DirMetrics* metrics,
      FsType fs_type,
      std::string dir,
      std::unique_ptr<DirInstanceMetadataFile> metadata_file,
      std::unique_ptr<ThreadPool> pool);
  ~Dir();

  // Shuts down this dir's thread pool, waiting for any closures submitted via
  // ExecClosure() to finish first.
  void Shutdown();

  // Run a task on this dir's thread pool.
  //
  // Normally the task is performed asynchronously. However, if submission to
  // the pool fails, it runs synchronously on the current thread.
  void ExecClosure(const Closure& task);

  // Waits for any outstanding closures submitted via ExecClosure() to finish.
  void WaitOnClosures();

  // Tests whether the directory is full by comparing the free space of its
  // underlying filesystem with a predefined "reserved" space value.
  //
  // If 'mode' is EXPIRED_ONLY, performs the test only if the dir was last
  // determined to be full some time ago. If 'mode' is ALWAYS, the test is
  // performed regardless.
  //
  // Only returns a bad Status in the event of a real error; fullness is
  // reflected via is_full().
  enum class RefreshMode {
    EXPIRED_ONLY,
    ALWAYS,
  };
  Status RefreshAvailableSpace(RefreshMode mode);

  FsType fs_type() const { return fs_type_; }

  // Return the full path of this directory.
  const std::string& dir() const { return dir_; }

  const DirInstanceMetadataFile* instance() const {
    return metadata_file_.get();
  }

  bool is_full() const {
    std::lock_guard<simple_spinlock> l(lock_);
    return is_full_;
  }

  int64_t available_bytes() {
    std::lock_guard<simple_spinlock> l(lock_);
    return available_bytes_;
  }

  // The amount of time to cache the amount of available space in this
  // directory.
  virtual int available_space_cache_secs() const = 0;

  // The number of bytes to reserve in each directory for non-Kudu usage. A
  // value of -1 means 1% of the disk space in a directory will be reserved.
  virtual int reserved_bytes() const = 0;

 private:
  Env* env_;
  DirMetrics* metrics_;
  const FsType fs_type_;
  const std::string dir_;
  const std::unique_ptr<DirInstanceMetadataFile> metadata_file_;
  const std::unique_ptr<ThreadPool> pool_;

  bool is_shutdown_;

  // Protects 'last_space_check_', 'is_full_' and available_bytes_.
  mutable simple_spinlock lock_;
  MonoTime last_space_check_;
  bool is_full_;

  // The available bytes of this dir, updated by RefreshAvailableSpace.
  int64_t available_bytes_;

  DISALLOW_COPY_AND_ASSIGN(Dir);
};

struct DirManagerOptions {
 public:
  // The type of directory this directory manager should support.
  //
  // Must not be empty.
  std::string dir_type;

  // The entity under which all metrics should be grouped. If null, metrics
  // will not be produced.
  //
  // Defaults to null.
  scoped_refptr<MetricEntity> metric_entity;

  // Whether the directory manager should only allow reading.
  //
  // Defaults to false.
  bool read_only;

  // Whether to update the on-disk instances when opening directories if
  // inconsistencies are detected.
  //
  // Defaults to UPDATE_AND_IGNORE_FAILURES.
  UpdateInstanceBehavior update_instances;

 protected:
  explicit DirManagerOptions(const std::string& dir_type);
};

class DirManager {
 public:
  enum class LockMode {
    MANDATORY,
    OPTIONAL,
    NONE,
  };

  // Returns the root names from the input 'root_list'.
  static std::vector<std::string> GetRootNames(const CanonicalizedRootsList& root_list);

  ~DirManager();

  // Shuts down all directories' thread pools.
  void Shutdown();

  // Waits on all directories' thread pools.
  void WaitOnClosures();

  // Returns a list of all dirs.
  const std::vector<std::unique_ptr<Dir>>& dirs() const {
    return dirs_;
  }

  // Adds 'uuid_idx' to the set of failed directories. This directory will no
  // longer be used. Logs an error message prefixed with 'error_message'
  // describing what directories are affected.
  //
  // Returns an error if all directories have failed.
  Status MarkDirFailed(int uuid_idx, const std::string& error_message = "");

  // Fails the directory specified by 'uuid' and logs a warning if all
  // directories have failed.
  void MarkDirFailedByUuid(const std::string& uuid);

  // Returns whether or not the 'uuid_idx' refers to a failed directory.
  bool IsDirFailed(int uuid_idx) const;

  // Returns whether the given tablet exists in a failed directory.
  bool IsTabletInFailedDir(const std::string& tablet_id) const;

  std::set<int> GetFailedDirs() const {
    shared_lock<rw_spinlock> group_lock(dir_group_lock_.get_lock());
    return failed_dirs_;
  }

  // Return a list of the canonicalized root directory names.
  std::vector<std::string> GetRoots() const;

  // Return a list of the canonicalized directory names.
  std::vector<std::string> GetDirs() const;

  // Finds a directory by uuid index, returning null if it can't be found.
  //
  // More information on uuid indexes and their relation to directories
  // can be found next to DirSetPB in fs.proto.
  Dir* FindDirByUuidIndex(int uuid_idx) const;

  // Finds a uuid index by directory, returning false if it can't be found.
  bool FindUuidIndexByDir(Dir* dir, int* uuid_idx) const;

  // Finds a uuid index by root path, returning false if it can't be found.
  bool FindUuidIndexByRoot(const std::string& root, int* uuid_idx) const;

  // Finds a uuid index by UUID, returning false if it can't be found.
  bool FindUuidIndexByUuid(const std::string& uuid, int* uuid_idx) const;

  // Finds a UUID by canonicalized root name, returning false if it can't be found.
  bool FindUuidByRoot(const std::string& root, std::string* uuid) const;

  // Finds the set of tablet IDs that are registered to use the directory with
  // the given UUID index.
  std::set<std::string> FindTabletsByDirUuidIdx(int uuid_idx) const;

  // Create a new directory using the appropriate directory implementation.
  virtual std::unique_ptr<Dir> CreateNewDir(Env* env,
                                            DirMetrics* metrics,
                                            FsType fs_type,
                                            std::string dir,
                                            std::unique_ptr<DirInstanceMetadataFile>,
                                            std::unique_ptr<ThreadPool> pool) = 0;

 protected:
  // The name to be used by this directory manager for each sub-directory of
  // each directory root.
  virtual const char* dir_name() const = 0;

  // The name to be used by this directory manager for each instance file
  // corresponding to this directory manager.
  virtual const char* instance_metadata_filename() const = 0;

  // Whether to sync the directories when updating this manager's directories.
  virtual bool sync_dirs() const = 0;

  // Whether to lock the directories to prevent concurrent usage. Note:
  // read-only concurrent usage is still allowed.
  virtual bool lock_dirs() const = 0;

  // The max number of directories to be managed.
  virtual int max_dirs() const = 0;

  DirManager(Env* env,
             std::unique_ptr<DirMetrics> dir_metrics,
             int num_threads_per_dir,
             const DirManagerOptions& opts,
             CanonicalizedRootsList canonicalized_data_roots);

  // Initializes the data directories on disk. Returns an error if initialized
  // directories already exist.
  //
  // Note: this doesn't initialize any in-memory state for the directory
  // manager.
  virtual Status Create();

  // Opens existing instance files from disk and indexes the files found.
  //
  // Returns an error if the number of on-disk directories found exceeds the
  // max allowed, if locks need to be acquired and cannot be, or if there are
  // no healthy directories.
  //
  // If appropriate, this will create any missing directories and rewrite
  // existing instance files to be consistent with each other.
  virtual Status Open();

  // Populates the maps to index the given directories.
  virtual Status PopulateDirectoryMaps(const std::vector<std::unique_ptr<Dir>>& dirs);

  // Helper function to add a directory to the internal maps. Assumes that the
  // UUID, UUID index, and directory name have not already been inserted.
  void InsertToMaps(const std::string& uuid, int idx, Dir* dir);

  // Loads the instance files for each directory root.
  //
  // On success, 'instance_files' contains instance objects, including those
  // that failed to load because they were missing or because of a disk
  // error; they are still considered "loaded" and are labeled unhealthy
  // internally. 'has_existing_instances' is set to true if any of the instance
  // files are healthy.
  //
  // Returns an error if an instance file fails in an irreconcileable way (e.g.
  // the file is locked).
  Status LoadInstances(
      std::vector<std::unique_ptr<DirInstanceMetadataFile>>* instance_files,
      bool* has_existing_instances);

  // Takes the set of instance files, does some basic verification on them,
  // creates any that don't exist on disk, and updates any that have a
  // different set of UUIDs stored than the expected set.
  //
  // Returns an error if there is a configuration error, e.g. if the existing
  // instances believe there should be a different block size.
  //
  // If in UPDATE_AND_IGNORE_FAILURES mode, an error is not returned in the event of a disk
  // error. Instead, it is up to the caller to reload the instance files and
  // proceed if healthy enough.
  //
  // If in UPDATE_AND_ERROR_ON_FAILURE mode, a failure to update instances will
  // surface as an error.
  Status CreateNewDirectoriesAndUpdateInstances(
      std::vector<std::unique_ptr<DirInstanceMetadataFile>> instances);

  // Updates the on-disk instance files specified by 'instances_to_update'
  // (presumably those whose 'all_uuids' field doesn't match 'new_all_uuids')
  // using the contents of 'new_all_uuids', skipping any unhealthy instance
  // files.
  //
  // If in UPDATE_AND_IGNORE_FAILURES mode, this is best effort. If any of the instance
  // updates fail (e.g. due to a disk error) in this mode, this will log a
  // warning about the failed updates and return OK.
  //
  // If in UPDATE_AND_ERROR_ON_FAILURE mode, any failure will immediately attempt
  // to clean up any altered state and return with an error.
  Status UpdateHealthyInstances(
      const std::vector<std::unique_ptr<DirInstanceMetadataFile>>& instances_to_update,
      const std::set<std::string>& new_all_uuids);

  // The environment to be used for all directory operations.
  Env* env_;

  // The number of threads to allocate per directory threadpool.
  const int num_threads_per_dir_;

  // The options that the Dirmanager was created with.
  const DirManagerOptions opts_;

  // The canonicalized roots provided to the constructor, taken verbatim.
  // Common roots in the collections have been deduplicated.
  const CanonicalizedRootsList canonicalized_fs_roots_;

  // Directories tracked by this manager.
  std::vector<std::unique_ptr<Dir>> dirs_;

  // Set of metrics relating to the health of the directories that this manager
  // is tracking.
  std::unique_ptr<DirMetrics> metrics_;

  // Lock protecting access to the directory group maps and to failed_dirs_. A
  // percpu_rwlock is used so threads attempting to read (e.g. to get the next
  // directory for an operation) do not block each other, while threads
  // attempting to write (e.g. to create a new tablet, thereby registering
  // directories per tablet) block all threads.
  mutable percpu_rwlock dir_group_lock_;

  // RNG used to select directories.
  mutable ThreadSafeRandom rng_;

  typedef std::unordered_map<std::string, std::string> UuidByRootMap;
  UuidByRootMap uuid_by_root_;

  typedef std::unordered_map<int, Dir*> UuidIndexMap;
  UuidIndexMap dir_by_uuid_idx_;

  typedef std::unordered_map<Dir*, int> ReverseUuidIndexMap;
  ReverseUuidIndexMap uuid_idx_by_dir_;

  typedef std::unordered_map<int, std::set<std::string>> TabletsByUuidIndexMap;
  TabletsByUuidIndexMap tablets_by_uuid_idx_map_;

  UuidByUuidIndexMap uuid_by_idx_;
  UuidIndexByUuidMap idx_by_uuid_;

  typedef std::set<int> FailedDirSet;
  FailedDirSet failed_dirs_;

  DISALLOW_COPY_AND_ASSIGN(DirManager);
};

} // namespace fs
} // namespace kudu
