// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/fs/block_manager_util.h"

#include <gflags/gflags.h>

#include "kudu/fs/fs.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"

DECLARE_bool(enable_data_block_fsync);

namespace kudu {
namespace fs {

using std::string;

PathInstanceMetadataFile::PathInstanceMetadataFile(Env* env,
                                                   const string& block_manager_type,
                                                   const string& filename)
  : env_(env),
    block_manager_type_(block_manager_type),
    filename_(filename) {
}

Status PathInstanceMetadataFile::Create() {
  if (env_->FileExists(filename_)) {
    return Status::AlreadyPresent("Block manager instance already exists",
                                  filename_);
  }
  PathInstanceMetadataPB new_instance;
  new_instance.set_uuid(oid_generator_.Next());
  new_instance.set_block_manager_type(block_manager_type_);

  return pb_util::WritePBContainerToPath(
      env_, filename_, new_instance,
      FLAGS_enable_data_block_fsync ? pb_util::SYNC : pb_util::NO_SYNC);
}

Status PathInstanceMetadataFile::Open(PathInstanceMetadataPB* metadata) const {
  PathInstanceMetadataPB pb;
  RETURN_NOT_OK(pb_util::ReadPBContainerFromPath(env_, filename_, &pb));

  if (pb.block_manager_type() != block_manager_type_) {
    return Status::IOError("Wrong block manager type", pb.block_manager_type());
  }

  if (metadata) {
    pb.Swap(metadata);
  }
  return Status::OK();
}

} // namespace fs
} // namespace kudu
