// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include "kudu/fs/block_manager.h"

// The default value is optimized for the case where:
// 1. the cfile blocks are colocated with the WALs.
// 2. The underlying hardware is a spinning disk.
// 3. The underlying filesystem is either XFS or EXT4.
// 4. cfile_do_on_finish is 'close' (see cfile/cfile_writer.cc).
//
// When all conditions hold, this value ensures low latency for WAL writes.
DEFINE_bool(block_coalesce_close, false,
            "Coalesce synchronization of data during CloseBlocks()");
