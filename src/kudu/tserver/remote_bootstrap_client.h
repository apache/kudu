// Copyright (c) 2014 Cloudera, Inc.
// Confidential Cloudera Information: Covered by NDA.
#ifndef KUDU_TSERVER_REMOTE_BOOTSTRAP_CLIENT_H
#define KUDU_TSERVER_REMOTE_BOOTSTRAP_CLIENT_H

#include <string>
#include <tr1/memory>
#include <vector>

#include <gtest/gtest_prod.h>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/macros.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/status.h"

namespace kudu {

class BlockId;
class BlockIdPB;
class FsManager;
class HostPort;

namespace consensus {
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
class TabletPeer;
class TabletStatusListener;
class TabletSuperBlockPB;
} // namespace tablet

namespace tserver {
class DataIdPB;
class DataChunkPB;
class RemoteBootstrapServiceProxy;

// Client class for using remote bootstrap to copy a tablet from another host.
// This class is not thread-safe.
//
// TODO:
// * Parallelize download of blocks and WAL segments.
//
class RemoteBootstrapClient {
 public:

  // Construct the remote bootstrap client.
  // 'fs_manager' and 'messenger' must remain valid until this object is destroyed.
  // 'client_permanent_uuid' is the permanent UUID of the caller server.
  RemoteBootstrapClient(const std::string& tablet_id,
                        FsManager* fs_manager,
                        const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                        const std::string& client_permanent_uuid);

  // Attempt to clean up resources on the remote end by sending an
  // EndRemoteBootstrapSession() RPC
  ~RemoteBootstrapClient();

  // Start up a remote bootstrap session to bootstrap from the specified
  // bootstrap peer. Place a new superblock indicating that remote bootstrap is
  // in progress. If the 'metadata' pointer is passed as NULL, it is ignored,
  // otherwise the TabletMetadata object resulting from the initial remote
  // bootstrap response is returned.
  // TODO: Rename these parameters to bootstrap_source_*.
  Status Start(const std::string& bootstrap_peer_uuid,
               const HostPort& bootstrap_peer_addr,
               scoped_refptr<tablet::TabletMetadata>* metadata);

  // Runs a "full" remote bootstrap, copying the physical layout of a tablet
  // from the leader of the specified consensus configuration.
  Status FetchAll(tablet::TabletStatusListener* status_listener);

  // After downloading all files successfully, write out the completed
  // replacement superblock.
  Status Finish();

 private:
  FRIEND_TEST(RemoteBootstrapClientTest, TestBeginEndSession);
  FRIEND_TEST(RemoteBootstrapClientTest, TestDownloadBlock);
  FRIEND_TEST(RemoteBootstrapClientTest, TestVerifyData);
  FRIEND_TEST(RemoteBootstrapClientTest, TestDownloadWalSegment);
  FRIEND_TEST(RemoteBootstrapClientTest, TestDownloadAllBlocks);

  // Extract the embedded Status message from the given ErrorStatusPB.
  // The given ErrorStatusPB must extend RemoteBootstrapErrorPB.
  static Status ExtractRemoteError(const rpc::ErrorStatusPB& remote_error);

  static Status UnwindRemoteError(const Status& status, const rpc::RpcController& controller);

  // Update the bootstrap StatusListener with a message.
  // The string "RemoteBootstrap: " will be prepended to each message.
  void UpdateStatusMessage(const std::string& message);

  // End the remote bootstrap session.
  Status EndRemoteSession();

  // Download all WAL files sequentially.
  Status DownloadWALs();

  // Download a single WAL file.
  // Assumes the WAL directories have already been created.
  // WAL file is opened with options so that it will fsync() on close.
  Status DownloadWAL(uint64_t wal_segment_seqno);

  // Write out the Consensus Metadata file based on the ConsensusStatePB
  // downloaded as part of initiating the remote bootstrap session.
  Status WriteConsensusMetadata();

  // Download all blocks belonging to a tablet sequentially.
  //
  // Blocks are given new IDs upon creation. On success, 'new_superblock_'
  // is populated to reflect the new block IDs and should be used in lieu
  // of 'superblock_' henceforth.
  Status DownloadBlocks();

  // Download the block specified by 'block_id'.
  //
  // On success:
  // - 'block_id' is set to the new ID of the downloaded block.
  // - 'block_count' is incremented.
  Status DownloadAndRewriteBlock(BlockIdPB* block_id, int* block_count, int num_blocks);

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
  const std::tr1::shared_ptr<rpc::Messenger> messenger_;
  const std::string permanent_uuid_;

  // State flags that enforce the progress of remote bootstrap.
  bool started_;            // Session started.
  bool downloaded_wal_;     // WAL segments downloaded.
  bool downloaded_blocks_;  // Data blocks downloaded.

  // Session-specific data items.
  scoped_refptr<tablet::TabletMetadata> meta_;
  tablet::TabletStatusListener* status_listener_;
  std::tr1::shared_ptr<RemoteBootstrapServiceProxy> proxy_;
  std::string session_id_;
  uint64_t session_idle_timeout_millis_;
  gscoped_ptr<tablet::TabletSuperBlockPB> superblock_;
  gscoped_ptr<tablet::TabletSuperBlockPB> new_superblock_;
  gscoped_ptr<consensus::ConsensusStatePB> committed_cstate_;
  std::vector<uint64_t> wal_seqnos_;

  DISALLOW_COPY_AND_ASSIGN(RemoteBootstrapClient);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_REMOTE_BOOTSTRAP_CLIENT_H */
