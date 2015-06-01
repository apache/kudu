// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/fs/block_manager_util.h"

#include <gflags/gflags.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {

using std::string;
using strings::Substitute;

PathInstanceMetadataFile::PathInstanceMetadataFile(Env* env,
                                                   const string& block_manager_type,
                                                   const string& filename)
  : env_(env),
    block_manager_type_(block_manager_type),
    filename_(filename) {
}

PathInstanceMetadataFile::~PathInstanceMetadataFile() {
  if (lock_) {
    WARN_NOT_OK(Unlock(), Substitute("Failed to unlock file $0", filename_));
  }
}

Status PathInstanceMetadataFile::Create() {
  DCHECK(!lock_) <<
      "Creating a metadata file that's already locked would release the lock";

  uint64_t block_size;
  RETURN_NOT_OK(env_->GetBlockSize(DirName(filename_), &block_size));

  PathInstanceMetadataPB new_instance;
  new_instance.set_uuid(oid_generator_.Next());
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

  gscoped_ptr<PathInstanceMetadataPB> pb(new PathInstanceMetadataPB());
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(env_, filename_, pb.get()));

  if (pb->block_manager_type() != block_manager_type_) {
    return Status::IOError("Wrong block manager type", pb->block_manager_type());
  }

  uint64_t block_size;
  RETURN_NOT_OK(env_->GetBlockSize(filename_, &block_size));
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
  RETURN_NOT_OK_PREPEND(env_->LockFile(filename_, &lock),
                        Substitute("Could not lock $0", filename_));
  lock_.reset(lock);
  return Status::OK();
}

Status PathInstanceMetadataFile::Unlock() {
  DCHECK(lock_);

  RETURN_NOT_OK_PREPEND(env_->UnlockFile(lock_.release()),
                        Substitute("Could not unlock $0", filename_));
  return Status::OK();
}

} // namespace fs
} // namespace kudu
