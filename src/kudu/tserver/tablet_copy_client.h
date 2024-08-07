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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/tserver/tablet_copy_source_session.h"
#include "kudu/util/locks.h"
#include "kudu/util/metrics.h"
#include "kudu/util/random.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class BlockIdPB;
class FsManager;
class HostPort;
class Schema;
class ThreadPool;
class Throttler;
class WritableFile;

namespace consensus {
class ConsensusMetadata;
class ConsensusMetadataManager;
class ConsensusStatePB;
} // namespace consensus

namespace fs {
class BlockCreationTransaction;
class WritableBlock;
} // namespace fs

namespace rpc {
class Messenger;
class RpcController;
} // namespace rpc

namespace tablet {
class RowSetDataPB;
class TabletMetadata;
class TabletReplica;
class TabletSuperBlockPB;
} // namespace tablet

namespace tserver {
class DataChunkPB;
class DataIdPB;
class TabletCopyServiceProxy;

// Server-wide tablet copy metrics.
struct TabletCopyClientMetrics {
  explicit TabletCopyClientMetrics(const scoped_refptr<MetricEntity>& metric_entity);

  scoped_refptr<Counter> bytes_fetched;
  scoped_refptr<AtomicGauge<int32_t>> open_client_sessions;
  scoped_refptr<Histogram> copy_duration;
};

// Client class for using tablet copy to copy a tablet from another host.
// This class is not thread-safe.
//
class TabletCopyClient {
 public:
  // Attempt to clean up resources on the remote end by sending an
  // EndTabletCopySession() RPC
  virtual ~TabletCopyClient();

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
  // in progress. If the 'tmeta' pointer is passed as NULL, it is ignored,
  // otherwise the TabletMetadata object resulting from the initial remote
  // bootstrap response is returned.
  //
  // Upon success, tablet metadata will be created and the tablet will be
  // assigned to a data directory group.
  virtual Status Start(const HostPort& /*copy_source_addr*/,
                       scoped_refptr<tablet::TabletMetadata>* /*tmeta*/) {
    return Status::NotSupported("copy from remote peer not supported");
  }

  // Similar to above, but copy from local filesystem.
  virtual Status Start(const std::string& /*tablet_id*/,
                       scoped_refptr<tablet::TabletMetadata>* /*tmeta*/) {
    return Status::NotSupported("copy from local filesystem not supported");
  }

  // Runs a "full" tablet copy, copying the physical layout of a tablet
  // from the leader of the specified consensus configuration.
  // Adds all downloaded blocks during copying to the tablet copy's transaction,
  // which will be closed together at Finish().
  Status FetchAll(const scoped_refptr<tablet::TabletReplica>& tablet_replica);

  // After downloading all files successfully, commit all downloaded blocks.
  // Write out the completed replacement superblock.
  // Must be called after Start() and FetchAll(). Must not be called after Abort().
  Status Finish();

  // Abort an in-progress transfer and immediately delete the data blocks and
  // WALs downloaded so far. Does nothing if called after Finish().
  Status Abort();

 protected:
  FRIEND_TEST(TabletCopyClientBasicTest, TestNoBlocksAtStart);
  FRIEND_TEST(TabletCopyClientBasicTest, TestBeginEndSession);
  FRIEND_TEST(TabletCopyClientBasicTest, TestDownloadBlock);
  FRIEND_TEST(TabletCopyClientBasicTest, TestLifeCycle);
  FRIEND_TEST(TabletCopyClientBasicTest, TestVerifyData);
  FRIEND_TEST(TabletCopyClientBasicTest, TestDownloadBlockMayFail);
  FRIEND_TEST(TabletCopyClientBasicTest, TestDownloadWalMayFail);
  FRIEND_TEST(TabletCopyClientBasicTest, TestDownloadWalSegment);
  FRIEND_TEST(TabletCopyClientBasicTest, TestDownloadAllBlocks);
  FRIEND_TEST(TabletCopyClientAbortTest, TestAbort);
  FRIEND_TEST(TabletCopyThrottlerTest, TestThrottler);

  // Construct the tablet copy client.
  //
  // Objects behind raw pointers must remain valid until this object is destroyed.
  TabletCopyClient(std::string dst_tablet_id,
                   std::string src_tablet_id,
                   FsManager* dst_fs_manager,
                   scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
                   std::shared_ptr<rpc::Messenger> messenger,
                   TabletCopyClientMetrics* dst_tablet_copy_metrics,
                   std::shared_ptr<Throttler> throttler = nullptr);

  // State machine that guides the progression of a single tablet copy.
  // A tablet copy will go through the states:
  //
  //   kInitialized --> kStarting --> kStarted --> kFinished
  //        |               |             |          ^^^
  //        |               |             +----------+||
  //        |               +-------------------------+|
  //        +------------------------------------------+
  enum State {
    // The copy has yet to do anything and no changes have been made on disk.
    kInitialized,

    // The copy has begun updating metadata on disk, but has not begun
    // receiving WAL segments or blocks from the tablet copy source. Being in
    // this state or beyond implies that 'tmeta_' is non-null.
    kStarting,

    // The metadata has been updated on disk to indicate the start of a new
    // copy. Blocks, metadata, and WAL segments may be received from the tablet
    // copy source in this state.
    kStarted,

    // The copy is finished and needs no cleanup.
    kFinished,
  };

  static Status UnwindRemoteError(const Status& status, const rpc::RpcController& controller);

  // Returns an error if any directories in the tablet's directory group are
  // unhealthy.
  Status CheckHealthyDirGroup() const;

  // Set a new status message on the TabletReplica.
  // The string "TabletCopy: " will be prepended to each message.
  void SetStatusMessage(const std::string& message);

  // End the tablet copy session.
  Status EndRemoteSession();

  Status InitTabletMeta(const Schema& schema, const std::string& copy_peer_uuid);

  // Download all WAL files in parallel.
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

  // A task do download blocks of the specified rowset.
  // In case of a failure, a thread notifies others via 'end_status',
  // so they can abort downloading of their rowsets' blocks.
  // If all threads succeed, 'end_status' is set to Status::OK().
  void DownloadRowset(const tablet::RowSetDataPB& src_rowset,
                      int num_remote_blocks,
                      std::atomic<int>* block_count,
                      Status* end_status);

  // Download all blocks belonging to a tablet sequentially. Add all
  // downloaded blocks to the tablet copy's transaction.
  //
  // Blocks are given new IDs upon creation. On success, 'superblock_'
  // is populated to reflect the new block IDs.
  Status DownloadBlocks();

  // Download the remote block specified by 'src_block_id'. 'num_blocks' should
  // be given as the total number of blocks there are to download (for logging
  // purposes). Add the block to the tablet copy's transaction, to close blocks
  // belonging to the transaction together when the copying is complete.
  //
  // On success:
  // - 'dest_block_id' is set to the new ID of the downloaded block.
  // - 'block_count' is incremented by 1.
  Status DownloadAndRewriteBlock(const BlockIdPB& src_block_id,
                                 int num_blocks,
                                 std::atomic<int32_t>* block_count,
                                 BlockIdPB* dest_block_id);

  // Download and rewrite remote block if end_status it hasn't been set as failed.
  // This method calls DownloadAndRewriteBlock and return its' status.
  // This method is thread-safe.
  //
  // On failure, end_status is set as error status of DownloadAndRewriteBlock.
  Status DownloadAndRewriteBlockIfEndStatusOK(const BlockIdPB& src_block_id,
                                              int num_blocks,
                                              std::atomic<int32_t>* block_count,
                                              BlockIdPB* dest_block_id,
                                              Status* end_status);

  // Download a single block.
  // Data block is opened with new ID. After downloading, the block is finalized
  // and added to the tablet copy's transaction.
  //
  // On success, 'new_block_id' is set to the new ID of the downloaded block.
  Status DownloadBlock(const BlockId& old_block_id,
                       BlockId* new_block_id);

  virtual Status TransferFile(const DataIdPB& data_id, fs::WritableBlock* appendable) = 0;
  virtual Status TransferFile(const DataIdPB& data_id, WritableFile* appendable) = 0;

  Status VerifyData(uint64_t offset, const DataChunkPB& chunk);

  // Runs the provided functor, which must send an RPC and return the result
  // status, until it succeeds, times out, or fails with a non-retriable error.
  template<typename F>
  Status SendRpcWithRetry(rpc::RpcController* controller, F f);

  // Return standard log prefix.
  std::string LogPrefix();

  // Set-once members.
  const std::string dst_tablet_id_;
  const std::string src_tablet_id_;
  FsManager* const dst_fs_manager_;
  const scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager_;
  const std::shared_ptr<rpc::Messenger> messenger_;

  // State of the progress of the tablet copy operation.
  State state_;

  // Session-specific data items.
  bool replace_tombstoned_tablet_;

  // Local tablet metadata file.
  scoped_refptr<tablet::TabletMetadata> tmeta_;

  // Local Consensus metadata file. This may initially be NULL if this is
  // bootstrapping a new replica (rather than replacing an old one) but it is
  // guaranteed to be set after a successful call to Start().
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

  Random rng_;

  TabletCopyClientMetrics* dst_tablet_copy_metrics_;

  // Block transaction for the tablet copy.
  std::unique_ptr<fs::BlockCreationTransaction> transaction_;

  // Thread pool for downloading all data blocks and wals in parallel.
  std::unique_ptr<ThreadPool> tablet_download_pool_;

  // Protects adding/creating blocks, adding a rowset,
  // reading/updating rowset/wal download status.
  simple_spinlock simple_lock_;

  // A throttler to limit tablet copy speed.
  // The throttler_ is shared among multiple tablet copying tasks in the same session.
  std::shared_ptr<Throttler> throttler_;

  DISALLOW_COPY_AND_ASSIGN(TabletCopyClient);
};

class RemoteTabletCopyClient : public TabletCopyClient {
 public:
  RemoteTabletCopyClient(
      const std::string& tablet_id,
      FsManager* dst_fs_manager,
      scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
      std::shared_ptr<rpc::Messenger> messenger,
      TabletCopyClientMetrics* dst_tablet_copy_metrics,
      std::shared_ptr<Throttler> throttler = nullptr);

  RemoteTabletCopyClient(
      std::string dst_tablet_id,
      std::string src_tablet_id,
      FsManager* dst_fs_manager,
      scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
      std::shared_ptr<rpc::Messenger> messenger,
      TabletCopyClientMetrics* dst_tablet_copy_metrics,
      std::shared_ptr<Throttler> throttler = nullptr);

  ~RemoteTabletCopyClient() override;

  Status Start(const HostPort& copy_source_addr,
               scoped_refptr<tablet::TabletMetadata>* tmeta) override;

 private:
  Status TransferFile(const DataIdPB& data_id, fs::WritableBlock* appendable) override;
  Status TransferFile(const DataIdPB& data_id, WritableFile* appendable) override;

  // Download a single remote file. The block and WAL implementations delegate
  // to this method when downloading files.
  //
  // An Appendable is typically a fs::WritableBlock (block) or WritableFile (WAL).
  //
  // Only used in one compilation unit, otherwise the implementation would
  // need to be in the header.
  template<class Appendable>
  Status DownloadFile(const DataIdPB& data_id, Appendable* appendable);

  // Download the superblock from remote. The superblock will firstly be stored
  // in a temporary local file 'superblock_path' and then loaded into the memory.
  Status DownloadSuperBlock(std::string* superblock_path);
};

class LocalTabletCopyClient : public TabletCopyClient {
 public:
  LocalTabletCopyClient(
      std::string tablet_id,
      FsManager* dst_fs_manager,
      scoped_refptr<consensus::ConsensusMetadataManager> cmeta_manager,
      std::shared_ptr<rpc::Messenger> messenger,
      TabletCopyClientMetrics* dst_tablet_copy_metrics,
      FsManager* src_fs_manager,
      TabletCopySourceMetrics* src_tablet_copy_metrics);

  Status Start(const std::string& tablet_id,
               scoped_refptr<tablet::TabletMetadata>* tmeta) override;

 private:
  Status TransferFile(const DataIdPB& data_id, fs::WritableBlock* appendable) override;
  Status TransferFile(const DataIdPB& data_id, WritableFile* appendable) override;

  // Similar to RemoteTabletCopyClient::DownloadFile, but copy file from local filesystem.
  template<class Appendable>
  Status CopyFile(const DataIdPB& data_id, Appendable* appendable);

  FsManager* const src_fs_manager_;
  TabletCopySourceMetrics* src_tablet_copy_metrics_;
  scoped_refptr<TabletCopySourceSession> session_;
};

} // namespace tserver
} // namespace kudu
