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
#include "kudu/fs/block_manager_util.h"

#include <cstdint>
#include <ostream>
#include <set>
#include <unordered_map>
#include <utility>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/join.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/test_util_prod.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {

using pb_util::CreateMode;
using std::set;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;
using strings::Substitute;

// Evaluates 'status_expr' and if it results in a disk failure, logs a message
// and fails the instance, returning with no error.
//
// Note: if a non-disk-failure error is produced, the instance will remain
// healthy. These errors should be handled externally.
#define RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(status_expr, msg) do { \
  const Status& _s = (status_expr); \
  if (PREDICT_FALSE(!_s.ok())) { \
    const Status _s_prepended = _s.CloneAndPrepend(msg); \
    if (_s_prepended.IsDiskFailure()) { \
      health_status_ = _s_prepended; \
      LOG(ERROR) << "Instance failed: " << _s_prepended.ToString(); \
      return Status::OK(); \
    } \
    return _s_prepended; \
  } \
} while (0)

PathInstanceMetadataFile::PathInstanceMetadataFile(Env* env,
                                                   string block_manager_type,
                                                   string filename)
    : env_(env),
      block_manager_type_(std::move(block_manager_type)),
      filename_(std::move(filename)) {}

PathInstanceMetadataFile::~PathInstanceMetadataFile() {
  if (lock_) {
    WARN_NOT_OK(Unlock(), Substitute("Failed to unlock file $0", filename_));
  }
}

Status PathInstanceMetadataFile::Create(const string& uuid, const vector<string>& all_uuids) {
  DCHECK(!lock_) <<
      "Creating a metadata file that's already locked would release the lock";
  DCHECK(ContainsKey(set<string>(all_uuids.begin(), all_uuids.end()), uuid));

  // Create a temporary file with which to fetch the filesystem's block size.
  //
  // This is a safer bet than using the parent directory as some filesystems
  // advertise different block sizes for directories than for files. On top of
  // that, the value may inform intra-file layout decisions made by Kudu, so
  // it's more correct to derive it from a file in any case.
  string created_filename;
  string tmp_template = JoinPathSegments(
      DirName(filename_), Substitute("getblocksize$0.XXXXXX", kTmpInfix));
  unique_ptr<WritableFile> tmp_file;
  RETURN_NOT_OK(env_->NewTempWritableFile(WritableFileOptions(),
                                          tmp_template,
                                          &created_filename, &tmp_file));
  SCOPED_CLEANUP({
    WARN_NOT_OK(env_->DeleteFile(created_filename),
                "could not delete temporary file");
  });
  uint64_t block_size;
  RETURN_NOT_OK(env_->GetBlockSize(created_filename, &block_size));

  PathInstanceMetadataPB new_instance;

  // Set up the path set.
  PathSetPB* new_path_set = new_instance.mutable_path_set();
  new_path_set->set_uuid(uuid);
  new_path_set->mutable_all_uuids()->Reserve(all_uuids.size());
  for (const string& u : all_uuids) {
    new_path_set->add_all_uuids(u);
  }

  // And the rest of the metadata.
  new_instance.set_block_manager_type(block_manager_type_);
  new_instance.set_filesystem_block_size_bytes(block_size);

  return pb_util::WritePBContainerToPath(
      env_, filename_, new_instance,
      pb_util::NO_OVERWRITE,
      FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
}

Status PathInstanceMetadataFile::LoadFromDisk() {
  DCHECK(!lock_) <<
      "Opening a metadata file that's already locked would release the lock";

  unique_ptr<PathInstanceMetadataPB> pb(new PathInstanceMetadataPB());
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(pb_util::ReadPBContainerFromPath(env_, filename_, pb.get()),
      Substitute("Failed to read metadata file from $0", filename_));

  if (pb->block_manager_type() != block_manager_type_) {
    return Status::IOError(Substitute(
      "existing data was written using the '$0' block manager; cannot restart "
      "with a different block manager '$1' without reformatting",
      pb->block_manager_type(), block_manager_type_));
  }

  uint64_t block_size;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->GetBlockSize(filename_, &block_size),
      Substitute("Failed to load metadata file. Could not get block size of $0", filename_));
  if (pb->filesystem_block_size_bytes() != block_size) {
    return Status::IOError("Wrong filesystem block size", Substitute(
        "Expected $0 but was $1", pb->filesystem_block_size_bytes(), block_size));
  }

  metadata_.swap(pb);
  return Status::OK();
}

Status PathInstanceMetadataFile::Lock() {
  DCHECK(!lock_);

  FileLock* lock;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->LockFile(filename_, &lock),
                                      Substitute("Could not lock $0", filename_));
  lock_.reset(lock);
  return Status::OK();
}

Status PathInstanceMetadataFile::Unlock() {
  DCHECK(lock_);

  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->UnlockFile(lock_.release()),
                                      Substitute("Could not unlock $0", filename_));
  return Status::OK();
}

void PathInstanceMetadataFile::SetMetadataForTests(
    unique_ptr<PathInstanceMetadataPB> metadata) {
  DCHECK(IsGTest());
  metadata_ = std::move(metadata);
}

Status PathInstanceMetadataFile::CheckIntegrity(
    const vector<unique_ptr<PathInstanceMetadataFile>>& instances) {
  CHECK(!instances.empty());

  // Note: although this verification works at the level of UUIDs and instance
  // files, the (user-facing) error messages are reported in terms of data
  // directories, because UUIDs and instance files are internal details.

  int first_healthy = -1;
  for (int i = 0; i < instances.size(); i++) {
    if (instances[i]->healthy()) {
      first_healthy = i;
      break;
    }
  }
  if (first_healthy == -1) {
    return Status::IOError("All data directories are unhealthy");
  }

  // Map of instance UUID to path instance structure. Tracks duplicate UUIDs.
  unordered_map<string, PathInstanceMetadataFile*> uuids;

  // Set of UUIDs specified in the path set of the first healthy instance. All
  // instances will be compared against it to make sure all path sets match.
  set<string> all_uuids(instances[first_healthy]->metadata()->path_set().all_uuids().begin(),
                        instances[first_healthy]->metadata()->path_set().all_uuids().end());

  if (all_uuids.size() != instances.size()) {
    return Status::IOError(
        Substitute("$0 data directories provided, but expected $1",
                   instances.size(), all_uuids.size()));
  }

  for (const auto& instance : instances) {
    // If the instance has failed (e.g. due to disk failure), there's no
    // telling what its metadata looks like. Ignore it, and continue checking
    // integrity across the healthy instances.
    if (!instance->healthy()) {
      continue;
    }
    const PathSetPB& path_set = instance->metadata()->path_set();

    // Check that the instance's UUID has not been claimed by another instance.
    PathInstanceMetadataFile** other = InsertOrReturnExisting(
        &uuids, path_set.uuid(), instance.get());
    if (other) {
      return Status::IOError(
          Substitute("Data directories $0 and $1 have duplicate instance metadata UUIDs",
                     (*other)->dir(), instance->dir()),
          path_set.uuid());
    }

    // Check that the instance's UUID is a member of all_uuids.
    if (!ContainsKey(all_uuids, path_set.uuid())) {
      return Status::IOError(
          Substitute("Data directory $0 instance metadata contains unexpected UUID",
                     instance->dir()),
          path_set.uuid());
    }

    // Check that the instance's UUID set does not contain duplicates.
    set<string> deduplicated_uuids(path_set.all_uuids().begin(),
                                   path_set.all_uuids().end());
    string all_uuids_str = JoinStrings(path_set.all_uuids(), ",");
    if (deduplicated_uuids.size() != path_set.all_uuids_size()) {
      return Status::IOError(
          Substitute("Data directory $0 instance metadata path set contains duplicate UUIDs",
                     instance->dir()),
          JoinStrings(path_set.all_uuids(), ","));
    }

    // Check that the instance's UUID set matches the expected set.
    if (deduplicated_uuids != all_uuids) {
      return Status::IOError(
          Substitute("Data directories $0 and $1 have different instance metadata UUID sets",
                     instances[0]->dir(), instance->dir()),
          Substitute("$0 vs $1", JoinStrings(all_uuids, ","), all_uuids_str));
    }
  }

  return Status::OK();
}

} // namespace fs
} // namespace kudu
