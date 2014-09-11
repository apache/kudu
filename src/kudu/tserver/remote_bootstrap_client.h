// Copyright (c) 2014 Cloudera, Inc.
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
class FsManager;
class WritableFile;

namespace consensus {
class OpId;
} // namespace consensus

namespace metadata {
class QuorumPB;
class QuorumPeerPB;
} // namespace metadata

namespace rpc {
class ErrorStatusPB;
class Messenger;
class RpcController;
} // namespace rpc

namespace tablet {
class TabletMasterBlockPB;
class TabletMetadata;
class TabletStatusListener;
class TabletSuperBlockPB;
} // namespace tablet

namespace tserver {
class DataIdPB;
class DataChunkPB;
class TabletServer;
class TabletServerServiceProxy;

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
  RemoteBootstrapClient(FsManager* fs_manager,
                        const std::tr1::shared_ptr<rpc::Messenger>& messenger,
                        const std::string& client_permanent_uuid);

  // Runs a "full" remote bootstrap, copying the physical layout of a tablet
  // from the leader of the specified quorum.
  Status RunRemoteBootstrap(tablet::TabletMetadata* meta,
                            const metadata::QuorumPB& quorum,
                            tablet::TabletStatusListener* status_listener);

 private:
  FRIEND_TEST(RemoteBootstrapClientTest, TestBeginEndSession);
  FRIEND_TEST(RemoteBootstrapClientTest, TestDownloadBlock);
  FRIEND_TEST(RemoteBootstrapClientTest, TestVerifyData);
  FRIEND_TEST(RemoteBootstrapClientTest, TestDownloadWalSegment);

  // Whether a remote boostrap session has been started or not.
  enum State {
    kNoSession,
    kSessionStarted,
  };

  // Return the leader of the quorum, or Status::NotFound() on error.
  static Status ExtractLeaderFromQuorum(const metadata::QuorumPB& quorum,
                                        metadata::QuorumPeerPB* leader);

  // Extract the embedded Status message from the given ErrorStatusPB.
  // The given ErrorStatusPB must extend RemoteBootstrapErrorPB.
  static Status ExtractRemoteError(const rpc::ErrorStatusPB& remote_error);

  static Status UnwindRemoteError(const Status& status, const rpc::RpcController& controller);

  // Update the bootstrap StatusListener with a message.
  // The string "RemoteBootstrap: " will be prepended to each message.
  void UpdateStatusMessage(const std::string& message);

  // Start up a new bootstrap session with the remote leader.
  // This is a one-time-use object, and calling BeginRemoteBootstrapSession()
  // twice is an error.
  //
  // 'status_listener' may be passed as NULL.
  Status BeginRemoteBootstrapSession(const std::string& tablet_id,
                                     const metadata::QuorumPB& quorum,
                                     tablet::TabletStatusListener* status_listener);

  // End the remote bootstrap session.
  Status EndRemoteBootstrapSession();

  // Download all WAL files sequentially.
  Status DownloadWALs();

  // Download a single WAL file.
  // Assumes the WAL directories have already been created.
  // WAL file is opened with options so that it will fsync() on close.
  Status DownloadWAL(const consensus::OpId& initial_opid, uint64_t wal_segment_seqno);

  // Download all blocks belonging to a tablet sequentially.
  // Does not replace the superblock.
  Status DownloadBlocks();

  // Download a single block file.
  // Data block file is opened with options so that it will fsync() on close.
  Status DownloadBlock(const BlockId& block_id);

  // Download a single remote file. The block and WAL implementations delegate
  // to this method when downloading files.
  Status DownloadFile(const DataIdPB& data_id, const std::tr1::shared_ptr<WritableFile>& writer);

  Status VerifyData(uint64_t offset, const DataChunkPB& resp);

  // Set-once members.
  FsManager* const fs_manager_;
  const std::tr1::shared_ptr<rpc::Messenger> messenger_;
  const std::string permanent_uuid_;

  // Whether a session is active.
  State state_;

  // Session-specific data items.
  std::string tablet_id_;
  tablet::TabletStatusListener* status_listener_;
  std::tr1::shared_ptr<TabletServerServiceProxy> proxy_;
  std::string session_id_;
  uint64_t session_idle_timeout_millis_;
  gscoped_ptr<tablet::TabletSuperBlockPB> superblock_;
  std::vector<consensus::OpId> wal_initial_opids_;

  DISALLOW_COPY_AND_ASSIGN(RemoteBootstrapClient);
};

} // namespace tserver
} // namespace kudu
#endif /* KUDU_TSERVER_REMOTE_BOOTSTRAP_CLIENT_H */
