// Copyright (c) 2014, Cloudera, inc.

#ifndef KUDU_FS_BLOCK_MANAGER_H
#define KUDU_FS_BLOCK_MANAGER_H

#include <cstddef>
#include <stdint.h>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"

namespace kudu {

class BlockId;
class Slice;
class Status;

namespace fs {

// The smallest unit of Kudu data that is backed by the local filesystem.
//
// The block interface reflects Kudu on-disk storage design principles:
// - Blocks are append only.
// - Blocks are immutable once written.
// - Blocks opened for reading are thread-safe and may be used by multiple
//   concurrent readers.
// - Blocks opened for writing are not thread-safe.
class Block {
 public:
  virtual ~Block() {}

  // Destroys the in-memory representation of the block.
  //
  // Does not guarantee durability of written data; Sync() must be called
  // for all outstanding data to reach the disk.
  virtual Status Close() = 0;

  // Returns the identifier for this block.
  virtual const BlockId& id() const = 0;
};

// A block that has been opened for writing. There may only be a single
// writing thread, and data may only be appended to the block.
class WritableBlock : public Block {
 public:
  virtual ~WritableBlock() {}

  // Appends the chunk of data referenced by 'data' to the block.
  //
  // Does not guarantee durability of 'data'; Sync() must be called for all
  // outstanding data to reach the disk.
  virtual Status Append(const Slice& data) = 0;

  // Synchronizes all dirty block data and metadata with the disk. On
  // success, guarantees that the entire block is durable.
  virtual Status Sync() = 0;

  // Returns the number of bytes successfully appended via Append().
  virtual size_t BytesAppended() const = 0;
};

// A block that has been opened for reading. Multiple in-memory blocks may
// be constructed for the same logical block, and the same in-memory block
// may be shared amongst threads for concurrent reading.
class ReadableBlock : public Block {
 public:
  virtual ~ReadableBlock() {}

  // Returns the on-disk size of a written block.
  virtual Status Size(size_t* sz) const = 0;

  // Reads exactly 'length' bytes beginning from 'offset' in the block,
  // returning an error if fewer bytes exist. A slice referencing the
  // results is written to 'result' and may be backed by memory in
  // 'scratch'. As such, 'scratch' must be at least 'length' in size and
  // must remain alive while 'result' is used.
  //
  // Does not modify 'result' on error (but may modify 'scratch').
  virtual Status Read(uint64_t offset, size_t length,
                      Slice* result, uint8_t* scratch) const = 0;
};

// Provides options and hints for block placement.
struct CreateBlockOptions {
  // Call Sync() during Close().
  bool sync_on_close;

  CreateBlockOptions() :
    sync_on_close(false) { }
};

// Utilities for Kudu block lifecycle management. All methods are
// thread-safe.
class BlockManager {
 public:
  virtual ~BlockManager() {}

  // Creates a new on-disk representation for this block manager.
  //
  // Returns an error if one already exists or cannot be created.
  virtual Status Create() = 0;

  // Opens an existing on-disk representation of this block manager.
  //
  // Returns an error if one does not exist or cannot be opened.
  virtual Status Open() = 0;

  // Creates a new block using the provided options and opens it for
  // writing. The block's ID will be generated.
  //
  // Does not guarantee the durability of the block; Sync() must be called
  // to ensure that the block reaches disk.
  //
  // Does not modify 'block' on error.
  virtual Status CreateAnonymousBlock(gscoped_ptr<WritableBlock>* block,
                                      CreateBlockOptions opts = CreateBlockOptions()) = 0;

  // Creates a new block using the provided options and opens it for
  // writing. The block's ID must be provided by the caller.
  //
  // Does not guarantee the durability of the block; Sync() must be called
  // to ensure that the block reaches disk.
  //
  // Does not modify 'block' on error.
  virtual Status CreateNamedBlock(const BlockId& block_id,
                                  gscoped_ptr<WritableBlock>* block,
                                  CreateBlockOptions opts = CreateBlockOptions()) = 0;

  // Opens an existing block for reading.
  //
  // Does not modify 'block' on error.
  virtual Status OpenBlock(const BlockId& block_id,
                           gscoped_ptr<ReadableBlock>* block) = 0;

  // Deletes an existing block, allowing its space to be reclaimed by the
  // filesystem. The change is immediately made durable.
  //
  // Blocks may be deleted while they are open for reading or writing;
  // the actual deletion will take place after the last open reader or
  // writer is closed.
  virtual Status DeleteBlock(const BlockId& block_id) = 0;

  // Synchronizes all dirty data and metadata belonging to the provided
  // blocks. Effectively like Sync() for each block but may be optimized
  // for groups of blocks.
  //
  // On success, guarantees that outstanding data is durable.
  virtual Status SyncBlocks(const std::vector<WritableBlock*>& blocks) = 0;
};

} // namespace fs
} // namespace kudu

#endif
