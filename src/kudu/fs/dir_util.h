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

#include <memory>
#include <set>
#include <string>

#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

namespace kudu {

class DirInstanceMetadataPB;
class Env;
class FileLock;

namespace fs {

// Reads and writes block manager instance metadata files.
//
// Thread-unsafe; access to this object must be externally synchronized.
class DirInstanceMetadataFile {
 public:
  // 'env' must remain valid for the lifetime of this class.
  //
  // 'uuid' is the UUID used for this instance file, though the UUID may be
  // changed if we read the instance file from disk and find a different UUID.
  DirInstanceMetadataFile(Env* env, std::string uuid, std::string dir_type,
                          std::string filename);

  ~DirInstanceMetadataFile();

  // Creates, writes, synchronizes, and closes a new instance metadata file.
  // Fails the PIMF in the case of a failed directory.
  //
  // 'all_uuids' is all of the unique UUIDs in this instance's dir set.
  // 'uuid_' must exist in this set. 'created_dir' is set to true if the parent
  // directory of the file was also created during this process.
  //
  // This should be called if we've already tried to load the instance file but
  // found it didn't exist.
  Status Create(const std::set<std::string>& all_uuids, bool* created_dir = nullptr);

  // Opens, reads, verifies, and closes an existing instance metadata file.
  //
  // On success, either 'metadata_' is overwritten with the contents of the
  // file, or, in the case of disk failure, returns OK and sets
  // 'health_status_' to a non-OK value.
  Status LoadFromDisk();

  // Locks the instance metadata file, which must exist on-disk. Returns an
  // error if it's already locked. The lock is released when Unlock() is
  // called, when this object is destroyed, or when the process exits.
  //
  // Note: the lock is also released if any fd of the instance metadata file
  // in this process is closed. Thus, it is an error to call Create() or
  // LoadFromDisk() on a locked file.
  Status Lock();

  // Unlocks the instance metadata file. Must have been locked to begin with.
  Status Unlock();

  // Sets that the instance failed (e.g. due to a disk failure).
  //
  // If failed, there is no guarantee that the instance will have a 'metadata_'.
  void SetInstanceFailed(const Status& s = Status::IOError("directory instance failed")) {
    health_status_ = s;
  }

  // Whether or not the instance is healthy. If the instance file lives on a
  // disk that has failed, this should return false.
  bool healthy() const {
    return health_status_.ok();
  }

  const Status& health_status() const {
    return health_status_;
  }

  std::string uuid() const { return uuid_; }
  std::string dir() const { return DirName(filename_); }
  const std::string& path() const { return filename_; }
  DirInstanceMetadataPB* metadata() const { return metadata_.get(); }

 private:
  Env* env_;

  // The UUID of this instance file. This is initialized in the constructor so
  // it can be used when creating a new instance file. However, it may be
  // overwritten when loading an existing instance from disk.
  //
  // This helps provide the invariant that, healthy or otherwise, every
  // instance has a valid UUID, which is useful for failed-directory-tracking,
  // which generally uses UUIDs.
  std::string uuid_;

  // The type of this instance file.
  const std::string dir_type_;

  // The name of the file associated with this instance.
  const std::string filename_;

  std::unique_ptr<DirInstanceMetadataPB> metadata_;

  // In order to prevent multiple Kudu processes from starting up using the
  // same directories, we lock the instance files when starting up.
  std::unique_ptr<FileLock> lock_;

  // The health of the instance file.
  Status health_status_;
};

} // namespace fs
} // namespace kudu
