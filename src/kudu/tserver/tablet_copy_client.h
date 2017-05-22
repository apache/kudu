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
#ifndef KUDU_TSERVER_TABLET_COPY_CLIENT_H
#define KUDU_TSERVER_TABLET_COPY_CLIENT_H

#include <string>
#include <memory>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/fs/block_id.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockIdPB;
class FsManager;
class HostPort;

namespace consensus {
class ConsensusMetadata;
class ConsensusStatePB;
class RaftConfigPB;
class RaftPeerPB;
} // namespace consensus

namespace rpc {
class ErrorStatusPB;
class Messenger;
class RpcController;
} // namespace rpc

namespace tablet {
class TabletMetadata;
class TabletReplica;
class TabletSuperBlockPB;
} // namespace tablet

namespace tserver {
class DataIdPB;
class DataChunkPB;
class TabletCopyServiceProxy;

// Client class for using tablet copy to copy a tablet from another host.
// This class is not thread-safe.
//
// TODO:
// * Parallelize download of blocks and WAL segments.
//
class TabletCopyClient {
 public:

  // Construct the tablet copy client.
  // 'fs_manager' and 'messenger' must remain valid until this object is destroyed.
  TabletCopyClient(std::string tablet_id, FsManager* fs_manager,
                        std::shared_ptr<rpc::Messenger> messenger);

  // Attempt to clean up resources on the remote end by sending an
  // EndTabletCopySession() RPC
  ~TabletCopyClient();

  // Pass in the existing metadata for a tombstoned tablet, which will be
  // replaced if validation checks pass in Start().
  // 'meta' is the metadata for the tombstoned tablet and 'caller_term' is the
  // term provided by the caller (assumed to be the current leader of the
  // consensus config) for validation purposes.
  // If the consensus metadata exists on disk for this tablet, and if
  // 'caller_term' is lower than the current term stored in that consensus
  // metadata, then this method will fail with a Status::InvalidArgument error.
  Status SetTabletToReplace(const scoped_refptr<tablet::TabletMetadata>& meta,
                            int64_t caller_term);

  // Start up a tablet copy session to bootstrap from the specified
  // bootstrap peer. Place a new superblock indicating that tablet copy is
  // in progress. If the 'metadata' pointer is passed as NULL, it is ignored,
  // otherwise the TabletMetadata object resulting from the initial remote
  // bootstrap response is returned.
  //
  // Upon success, tablet metadata will be created and the tablet will be
  // assigned to a data directory group.
  Status Start(const HostPort& copy_source_addr,
               scoped_refptr<tablet::TabletMetadata>* meta);

  // Runs a "full" tablet copy, copying the physical layout of a tablet
  // from the leader of the specified consensus configuration.
  Status FetchAll(const scoped_refptr<tablet::TabletReplica>& tablet_replica);

  // After downloading all files successfully, write out the completed
  // replacement superblock. Must be called after Start() and FetchAll().
  // Must not be called after Abort().
  Status Finish();

  // Abort an in-progress transfer and immediately delete the data blocks and
  // WALs downloaded so far. Does nothing if called after Finish().
  Status Abort();

 private:
  FRIEND_TEST(TabletCopyClientTest, TestNoBlocksAtStart);
  FRIEND_TEST(TabletCopyClientTest, TestBeginEndSession);
  FRIEND_TEST(TabletCopyClientTest, TestDownloadBlock);
  FRIEND_TEST(TabletCopyClientTest, TestVerifyData);
  FRIEND_TEST(TabletCopyClientTest, TestDownloadWalSegment);
  FRIEND_TEST(TabletCopyClientTest, TestDownloadAllBlocks);
  FRIEND_TEST(TabletCopyClientAbortTest, TestAbort);

  enum State {
    kInitialized,
    kStarted,
    kFinished,
  };

  static Status UnwindRemoteError(const Status& status, const rpc::RpcController& controller);

  // Set a new status message on the TabletReplica.
  // The string "TabletCopy: " will be prepended to each message.
  void SetStatusMessage(const std::string& message);

  // End the tablet copy session.
  Status EndRemoteSession();

  // Download all WAL files sequentially.
  Status DownloadWALs();

  // Download a single WAL file.
  // Assumes the WAL directories have already been created.
  // WAL file is opened with options so that it will fsync() on close.
  Status DownloadWAL(uint64_t wal_segment_seqno);

  // Write out the Consensus Metadata file based on the ConsensusStatePB
  // downloaded as part of initiating the tablet copy session.
  Status WriteConsensusMetadata();

  // Count the number of blocks on the remote (from 'remote_superblock_').
  int CountRemoteBlocks() const;

  // Download all blocks belonging to a tablet sequentially.
  //
  // Blocks are given new IDs upon creation. On success, 'superblock_'
  // is populated to reflect the new block IDs.
  Status DownloadBlocks();

  // Download the remote block specified by 'src_block_id'. 'num_blocks' should
  // be given as the total number of blocks there are to download (for logging
  // purposes).
  //
  // On success:
  // - 'dest_block_id' is set to the new ID of the downloaded block.
  // - 'block_count' is incremented by 1.
  Status DownloadAndRewriteBlock(const BlockIdPB& src_block_id,
                                 int num_blocks,
                                 int* block_count,
                                 BlockIdPB* dest_block_id);

  // Download a single block.
  // Data block is opened with options so that it will fsync() on close.
  //
  // On success, 'new_block_id' is set to the new ID of the downloaded block.
  Status DownloadBlock(const BlockId& old_block_id, BlockId* new_block_id);

  // Download a single remote file. The block and WAL implementations delegate
  // to this method when downloading files.
  //
  // An Appendable is typically a WritableBlock (block) or WritableFile (WAL).
  //
  // Only used in one compilation unit, otherwise the implementation would
  // need to be in the header.
  template<class Appendable>
  Status DownloadFile(const DataIdPB& data_id, Appendable* appendable);

  Status VerifyData(uint64_t offset, const DataChunkPB& resp);

  // Return standard log prefix.
  std::string LogPrefix();

  // Set-once members.
  const std::string tablet_id_;
  FsManager* const fs_manager_;
  const std::shared_ptr<rpc::Messenger> messenger_;

  // State of the progress of the tablet copy operation.
  State state_;

  // Session-specific data items.
  bool replace_tombstoned_tablet_;

  // Local tablet metadata file.
  scoped_refptr<tablet::TabletMetadata> meta_;

  // Local Consensus metadata file. This may initially be NULL if this is
  // bootstrapping a new replica (rather than replacing an old one).
  scoped_refptr<consensus::ConsensusMetadata> cmeta_;

  scoped_refptr<tablet::TabletReplica> tablet_replica_;
  std::shared_ptr<TabletCopyServiceProxy> proxy_;
  std::string session_id_;
  uint64_t session_idle_timeout_millis_;
  std::unique_ptr<tablet::TabletSuperBlockPB> remote_superblock_;
  std::unique_ptr<tablet::TabletSuperBlockPB> superblock_;
  std::unique_ptr<consensus::ConsensusStatePB> remote_cstate_;
  std::vector<uint64_t> wal_seqnos_;
  int64_t start_time_micros_;

  DISALLOW_COPY_AND_ASSIGN(TabletCopyClient);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_TABLET_COPY_CLIENT_H */
