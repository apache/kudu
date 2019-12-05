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
#include "kudu/fs/dir_util.h"

#include <cstdint>
#include <ostream>
#include <set>
#include <utility>

#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/scoped_cleanup.h"

DECLARE_bool(enable_data_block_fsync);

using kudu::pb_util::CreateMode;
using std::set;
using std::string;
using std::unique_ptr;
using strings::Substitute;

namespace kudu {
namespace fs {

namespace {

const char kHolePunchErrorMsg[] =
    "Error during hole punch test. The log block manager requires a "
    "filesystem with hole punching support such as ext4 or xfs. On el6, "
    "kernel version 2.6.32-358 or newer is required. To run without hole "
    "punching (at the cost of some efficiency and scalability), reconfigure "
    "Kudu to use the file block manager. Refer to the Kudu documentation for "
    "more details. WARNING: the file block manager is not suitable for "
    "production use and should be used only for small-scale evaluation and "
    "development on systems where hole-punching is not available. It's "
    "impossible to switch between block managers after data is written to the "
    "server. Raw error message follows";

Status CheckHolePunch(Env* env, const string& path) {
  // Arbitrary constants.
  static uint64_t kFileSize = 4096 * 4;
  static uint64_t kHoleOffset = 4096;
  static uint64_t kHoleSize = 8192;
  static uint64_t kPunchedFileSize = kFileSize - kHoleSize;

  // Open the test file.
  string filename = JoinPathSegments(path, "hole_punch_test_file");
  unique_ptr<RWFile> file;
  RWFileOptions opts;
  RETURN_NOT_OK(env->NewRWFile(opts, filename, &file));

  // The file has been created; delete it on exit no matter what happens.
  auto file_deleter = MakeScopedCleanup([&]() {
    WARN_NOT_OK(env->DeleteFile(filename),
                "Could not delete file " + filename);
  });

  // Preallocate it, making sure the file's size is what we'd expect.
  uint64_t sz;
  RETURN_NOT_OK(file->PreAllocate(0, kFileSize, RWFile::CHANGE_FILE_SIZE));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kFileSize) {
    return Status::IOError(Substitute(
        "Unexpected pre-punch file size for $0: expected $1 but got $2",
        filename, kFileSize, sz));
  }

  // Punch the hole, testing the file's size again.
  RETURN_NOT_OK(file->PunchHole(kHoleOffset, kHoleSize));
  RETURN_NOT_OK(env->GetFileSizeOnDisk(filename, &sz));
  if (sz != kPunchedFileSize) {
    return Status::IOError(Substitute(
        "Unexpected post-punch file size for $0: expected $1 but got $2",
        filename, kPunchedFileSize, sz));
  }

  return Status::OK();
}

} // anonymous namespace

// Evaluates 'status_expr' and if it results in a disk-failure error, logs a
// message and marks the instance as unhealthy, returning with no error.
//
// Note: A disk failure may thwart attempts to read directory entries at the OS
// level, leading to NotFound errors when reading the instance files. As such,
// we treat missing instances the same way we treat those that yield more
// blatant disk failure POSIX codes.
//
// Note: if a non-disk-failure error is produced, the instance will remain
// healthy. These errors should be handled externally.
#define RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(status_expr, msg) do { \
  const Status& _s = (status_expr); \
  if (PREDICT_FALSE(!_s.ok())) { \
    const Status _s_prepended = _s.CloneAndPrepend(msg); \
    if (_s.IsNotFound() || _s.IsDiskFailure()) { \
      health_status_ = _s_prepended; \
      LOG(INFO) << "Instance is unhealthy: " << _s_prepended.ToString(); \
      return Status::OK(); \
    } \
    return _s_prepended; \
  } \
} while (0)

DirInstanceMetadataFile::DirInstanceMetadataFile(Env* env,
                                                   string uuid,
                                                   string dir_type,
                                                   string filename)
    : env_(env),
      uuid_(std::move(uuid)),
      dir_type_(std::move(dir_type)),
      filename_(std::move(filename)) {}

DirInstanceMetadataFile::~DirInstanceMetadataFile() {
  if (lock_) {
    WARN_NOT_OK(Unlock(), Substitute("Failed to unlock file $0", filename_));
  }
}

Status DirInstanceMetadataFile::Create(const set<string>& all_uuids,
                                        bool* created_dir) {
  DCHECK(!lock_);
  DCHECK(ContainsKey(all_uuids, uuid_));
  const string dir_name = dir();

  bool created;
  // Create the directory if needed.
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(
      env_util::CreateDirIfMissing(env_, dir_name, &created),
      Substitute("Could not create directory $0", dir_name));
  auto cleanup_dir_on_failure = MakeScopedCleanup([&] {
    if (created) {
      WARN_NOT_OK(env_->DeleteDir(dir_name), "Could not remove newly-created directory");
    }
  });

  // If we're initializing the log block manager, check that we support
  // hole-punching.
  if (dir_type_ == "log") {
    RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(CheckHolePunch(env_, dir_name),
                                        kHolePunchErrorMsg);
  }

  // Create a temporary file with which to fetch the filesystem's block size.
  //
  // This is a safer bet than using the parent directory as some filesystems
  // advertise different block sizes for directories than for files. On top of
  // that, the value may inform intra-file layout decisions made by Kudu, so
  // it's more correct to derive it from a file in any case.
  string created_filename;
  string tmp_template = JoinPathSegments(
      dir_name, Substitute("getblocksize$0.XXXXXX", kTmpInfix));
  unique_ptr<WritableFile> tmp_file;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(
      env_->NewTempWritableFile(WritableFileOptions(),
                                tmp_template,
                                &created_filename, &tmp_file),
      "failed to create temp file while checking block size");
  SCOPED_CLEANUP({
    WARN_NOT_OK(env_->DeleteFile(created_filename),
                "could not delete temporary file");
  });
  uint64_t block_size;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->GetBlockSize(created_filename, &block_size),
                                      "failed to check block size");

  // Set up the directory set.
  DirInstanceMetadataPB new_instance;
  DirSetPB* new_dir_set = new_instance.mutable_dir_set();
  new_dir_set->set_uuid(uuid_);
  new_dir_set->mutable_all_uuids()->Reserve(all_uuids.size());
  for (const string& u : all_uuids) {
    new_dir_set->add_all_uuids(u);
  }

  // And the rest of the metadata.
  new_instance.set_dir_type(dir_type_);
  new_instance.set_filesystem_block_size_bytes(block_size);

  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(pb_util::WritePBContainerToPath(
      env_, filename_, new_instance,
      pb_util::NO_OVERWRITE,
      FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC),
      "failed to write PB");

  // Now that we're returning success, we don't need to clean anything up, and
  // we can indicate to callers there is a new directory to clean up (if
  // appropriate).
  cleanup_dir_on_failure.cancel();
  if (created_dir) {
    *created_dir = created;
  }
  return Status::OK();
}

Status DirInstanceMetadataFile::LoadFromDisk() {
  DCHECK(!lock_) <<
      "Opening a metadata file that's already locked would release the lock";

  unique_ptr<DirInstanceMetadataPB> pb(new DirInstanceMetadataPB());
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(pb_util::ReadPBContainerFromPath(env_, filename_, pb.get()),
      Substitute("Failed to read metadata file from $0", filename_));

  if (pb->dir_type() != dir_type_) {
    return Status::IOError(Substitute(
      "existing instance was written using the '$0' format; cannot restart "
      "with a different format type '$1'",
      pb->dir_type(), dir_type_));
  }

  uint64_t block_size;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->GetBlockSize(filename_, &block_size),
      Substitute("Failed to load metadata file. Could not get block size of $0", filename_));
  if (pb->filesystem_block_size_bytes() != block_size) {
    return Status::IOError("Wrong filesystem block size", Substitute(
        "Expected $0 but was $1", pb->filesystem_block_size_bytes(), block_size));
  }

  uuid_ = pb->dir_set().uuid();
  metadata_ = std::move(pb);
  return Status::OK();
}

Status DirInstanceMetadataFile::Lock() {
  DCHECK(!lock_);

  FileLock* lock;
  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->LockFile(filename_, &lock),
                                      "Could not lock instance file. Make sure that "
                                      "Kudu is not already running and you are not trying to run "
                                      "Kudu with a different user than before");
  lock_.reset(lock);
  return Status::OK();
}

Status DirInstanceMetadataFile::Unlock() {
  DCHECK(lock_);

  RETURN_NOT_OK_FAIL_INSTANCE_PREPEND(env_->UnlockFile(lock_.release()),
                                      Substitute("Could not unlock $0", filename_));
  return Status::OK();
}

} // namespace fs
} // namespace kudu
