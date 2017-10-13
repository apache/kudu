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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

#include "kudu/consensus/log_anchor_registry.h"
#include "kudu/consensus/log_util.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/stl_util.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/util/env.h"
#include "kudu/util/metrics.h"
#include "kudu/util/once.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"

namespace kudu {

class FsManager;

namespace tablet {
class TabletReplica;
} // namespace tablet

namespace tserver {

// Server-wide tablet source session metrics.
struct TabletCopySourceMetrics {
  explicit TabletCopySourceMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  scoped_refptr<Counter> bytes_sent;
  scoped_refptr<AtomicGauge<int32_t>> open_source_sessions;
};

// Caches file size and holds a shared_ptr reference to a RandomAccessFile.
// Assumes that the file underlying the RandomAccessFile is immutable.
struct ImmutableRandomAccessFileInfo {
  std::shared_ptr<RandomAccessFile> readable;
  int64_t size;

  ImmutableRandomAccessFileInfo(std::shared_ptr<RandomAccessFile> readable,
                                int64_t size)
      : readable(std::move(readable)), size(size) {}

  Status Read(uint64_t offset, Slice data) const {
    return readable->Read(offset, data);
  }
};

// Caches block size and holds an exclusive reference to a ReadableBlock.
// Assumes that the block underlying the ReadableBlock is immutable.
struct ImmutableReadableBlockInfo {
  std::unique_ptr<fs::ReadableBlock> readable;
  int64_t size;

  ImmutableReadableBlockInfo(fs::ReadableBlock* readable,
                             int64_t size)
  : readable(readable),
    size(size) {
  }

  Status Read(uint64_t offset, Slice data) const {
    return readable->Read(offset, data);
  }
};

// A potential Learner must establish a TabletCopySourceSession with the leader in order
// to fetch the needed superblock, blocks, and log segments.
// This class is refcounted to make it easy to remove it from the session map
// on expiration while it is in use by another thread.
class TabletCopySourceSession : public RefCountedThreadSafe<TabletCopySourceSession> {
 public:
  TabletCopySourceSession(const scoped_refptr<tablet::TabletReplica>& tablet_replica,
                          std::string session_id,
                          std::string requestor_uuid,
                          FsManager* fs_manager,
                          TabletCopySourceMetrics* tablet_copy_metrics);

  // Initialize the session, including anchoring files (TODO) and fetching the
  // tablet superblock and list of WAL segments.
  //
  // Must be called before accessing block state.
  Status Init();

  // Returns true if this session has been initialized.
  bool IsInitialized() const {
    return init_once_.init_succeeded();
  }

  // Return ID of tablet corresponding to this session.
  const std::string& tablet_id() const;

  // Return UUID of the requestor that initiated this session.
  const std::string& requestor_uuid() const;

  // Open block for reading, if it's not already open, and read some of it.
  // If maxlen is 0, we use a system-selected length for the data piece.
  // *data is set to a std::string containing the data. Ownership of this object
  // is passed to the caller. A string is used because the RPC interface is
  // sending data serialized as protobuf and we want to minimize copying.
  // On error, Status is set to a non-OK value and error_code is filled in.
  //
  // This method is thread-safe.
  Status GetBlockPiece(const BlockId& block_id,
                       uint64_t offset, int64_t client_maxlen,
                       std::string* data, int64_t* block_file_size,
                       TabletCopyErrorPB::Code* error_code);

  // Get a piece of a log segment.
  // The behavior and params are very similar to GetBlockPiece(), but this one
  // is only for sending WAL segment files.
  Status GetLogSegmentPiece(uint64_t segment_seqno,
                            uint64_t offset, int64_t client_maxlen,
                            std::string* data, int64_t* log_file_size,
                            TabletCopyErrorPB::Code* error_code);

  const tablet::TabletSuperBlockPB& tablet_superblock() const {
    DCHECK(init_once_.init_succeeded());
    return tablet_superblock_;
  }

  const consensus::ConsensusStatePB& initial_cstate() const {
    DCHECK(init_once_.init_succeeded());
    return initial_cstate_;
  }

  const log::SegmentSequence& log_segments() const {
    DCHECK(init_once_.init_succeeded());
    return log_segments_;
  }

  // Check if a block is currently open.
  bool IsBlockOpenForTests(const BlockId& block_id) const;

 private:
  friend class RefCountedThreadSafe<TabletCopySourceSession>;

  typedef std::unordered_map<
      BlockId,
      ImmutableReadableBlockInfo*,
      BlockIdHash,
      BlockIdEqual> BlockMap;
  typedef std::unordered_map<uint64_t, ImmutableRandomAccessFileInfo*> LogMap;

  ~TabletCopySourceSession();

  // Internal helper method for Init().
  Status InitOnce();

  // Open the block and add it to the block map.
  Status OpenBlock(const BlockId& block_id);

  // Look up cached block information.
  Status FindBlock(const BlockId& block_id,
                   ImmutableReadableBlockInfo** block_info,
                   TabletCopyErrorPB::Code* error_code);

  // Snapshot the log segment's length and put it into segment map.
  Status OpenLogSegment(uint64_t segment_seqno);

  // Look up log segment in cache or log segment map.
  Status FindLogSegment(uint64_t segment_seqno,
                        ImmutableRandomAccessFileInfo** file_info,
                        TabletCopyErrorPB::Code* error_code);

  // Unregister log anchor, if it's registered.
  Status UnregisterAnchorIfNeededUnlocked();

  const scoped_refptr<tablet::TabletReplica> tablet_replica_;
  const std::string session_id_;
  const std::string requestor_uuid_;
  FsManager* const fs_manager_;

  // Protects concurrent access to Init().
  KuduOnceDynamic init_once_;

  // The following fields are initialized during Init():
  BlockMap blocks_;
  LogMap logs_;
  ValueDeleter blocks_deleter_;
  ValueDeleter logs_deleter_;
  tablet::TabletSuperBlockPB tablet_superblock_;
  consensus::ConsensusStatePB initial_cstate_;
  // The sequence of log segments that will be sent in the course of this session.
  log::SegmentSequence log_segments_;
  log::LogAnchor log_anchor_;

  TabletCopySourceMetrics* tablet_copy_metrics_;

  DISALLOW_COPY_AND_ASSIGN(TabletCopySourceSession);
};

} // namespace tserver
} // namespace kudu
