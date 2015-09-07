// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/block_manager.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/metrics.h"

// The default value is optimized for the case where:
// 1. the cfile blocks are colocated with the WALs.
// 2. The underlying hardware is a spinning disk.
// 3. The underlying filesystem is either XFS or EXT4.
// 4. cfile_do_on_finish is 'close' (see cfile/cfile_writer.cc).
//
// When all conditions hold, this value ensures low latency for WAL writes.
DEFINE_bool(block_coalesce_close, false,
            "Coalesce synchronization of data during CloseBlocks()");
TAG_FLAG(block_coalesce_close, experimental);

DEFINE_bool(block_manager_lock_dirs, true,
            "Lock the data block directories to prevent concurrent usage. "
            "Note that read-only concurrent usage is still allowed.");
TAG_FLAG(block_manager_lock_dirs, unsafe);

namespace kudu {
namespace fs {

const char* BlockManager::kInstanceMetadataFileName = "block_manager_instance";

BlockManagerOptions::BlockManagerOptions()
  : read_only(false) {
}

BlockManagerOptions::~BlockManagerOptions() {
}

} // namespace fs
} // namespace kudu
