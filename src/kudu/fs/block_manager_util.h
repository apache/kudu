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
#include <string>
#include <vector>

#include "kudu/util/path_util.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class FileLock;
class PathInstanceMetadataPB;

namespace fs {

// Reads and writes block manager instance metadata files.
//
// Thread-unsafe; access to this object must be externally synchronized.
class PathInstanceMetadataFile {
 public:
  // 'env' must remain valid for the lifetime of this class.
  PathInstanceMetadataFile(Env* env, std::string block_manager_type,
                           std::string filename);

  ~PathInstanceMetadataFile();

  // Creates, writes, synchronizes, and closes a new instance metadata file.
  //
  // 'uuid' is this instance's UUID, and 'all_uuids' is all of the UUIDs in
  // this instance's path set.
  Status Create(const std::string& uuid,
                const std::vector<std::string>& all_uuids);

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

  // Sets the metadata file.
  void SetMetadataForTests(std::unique_ptr<PathInstanceMetadataPB> metadata);

  // Sets that the instance failed (e.g. due to a disk failure).
  //
  // If failed, there is no guarantee that the instance will have a 'metadata_'.
  void SetInstanceFailed(const Status& s = Status::IOError("Path instance failed")) {
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

  std::string dir() const { return DirName(filename_); }
  const std::string& path() const { return filename_; }
  PathInstanceMetadataPB* const metadata() const { return metadata_.get(); }

  // Check the integrity of the provided instances' path sets, ignoring any
  // unhealthy instances.
  static Status CheckIntegrity(
      const std::vector<std::unique_ptr<PathInstanceMetadataFile>>& instances);

 private:
  Env* env_;
  const std::string block_manager_type_;
  const std::string filename_;
  std::unique_ptr<PathInstanceMetadataPB> metadata_;
  std::unique_ptr<FileLock> lock_;
  Status health_status_;
};

} // namespace fs
} // namespace kudu
