// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_FS_BLOCK_MANAGER_UTIL_H
#define KUDU_FS_BLOCK_MANAGER_UTIL_H

#include <string>

#include "kudu/util/oid_generator.h"
#include "kudu/util/status.h"

namespace kudu {

class Env;
class PathInstanceMetadataPB;

namespace fs {

// Reads and writes block manager instance metadata files.
class PathInstanceMetadataFile {
 public:
  // 'env' must remain valid for the lifetime of this class.
  PathInstanceMetadataFile(Env* env,
                           const std::string& block_manager_type,
                           const std::string& filename);

  // Creates, writes, and synchronizes a new instance metadata file.
  Status Create();

  // Opens and verifies an existing instance metadata file.
  //
  // On success, 'metadata' is overwritten with the contents of the file
  // (if not NULL).
  Status Open(PathInstanceMetadataPB* metadata) const;

 private:
  Env* env_;
  const std::string block_manager_type_;
  const std::string filename_;
  ObjectIdGenerator oid_generator_;
};

} // namespace fs
} // namespace kudu
#endif
