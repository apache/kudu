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

#include "kudu/tserver/tablet_copy_client.h"

#include <cstdint>
#include <memory>
#include <ostream>
#include <utility>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

#include "kudu/common/partition.h"
#include "kudu/common/schema.h"
#include "kudu/common/wire_protocol.h"
#include "kudu/consensus/consensus_meta.h"
#include "kudu/consensus/consensus_meta_manager.h"
#include "kudu/consensus/metadata.pb.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/fs/block_id.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/walltime.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "kudu/tablet/metadata.pb.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/tserver/tablet_copy.pb.h"
#include "kudu/tserver/tablet_copy.proxy.h"
#include "kudu/tserver/ts_tablet_manager.h"
#include "kudu/util/crc.h"
#include "kudu/util/env.h"
#include "kudu/util/env_util.h"
#include "kudu/util/fault_injection.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/net/sockaddr.h"
#include "kudu/util/path_util.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/random.h"
#include "kudu/util/random_util.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"

DEFINE_int32(tablet_copy_begin_session_timeout_ms, 30000,
             "Tablet server RPC client timeout for BeginTabletCopySession calls. "
             "Also used for EndTabletCopySession calls.");
TAG_FLAG(tablet_copy_begin_session_timeout_ms, advanced);

DEFINE_bool(tablet_copy_save_downloaded_metadata, false,
            "Save copies of the downloaded tablet copy files for debugging purposes. "
            "Note: This is only intended for debugging and should not be normally used!");
TAG_FLAG(tablet_copy_save_downloaded_metadata, advanced);
TAG_FLAG(tablet_copy_save_downloaded_metadata, hidden);
TAG_FLAG(tablet_copy_save_downloaded_metadata, runtime);

DEFINE_int32(tablet_copy_download_file_inject_latency_ms, 0,
             "Injects latency into the loop that downloads files, causing tablet copy "
             "to take much longer. For use in tests only.");
TAG_FLAG(tablet_copy_download_file_inject_latency_ms, hidden);

DEFINE_double(tablet_copy_fault_crash_on_fetch_all, 0.0,
              "Fraction of the time that the server will crash when FetchAll() "
              "is called on the TabletCopyClient. (For testing only!)");
TAG_FLAG(tablet_copy_fault_crash_on_fetch_all, unsafe);
TAG_FLAG(tablet_copy_fault_crash_on_fetch_all, runtime);

DEFINE_double(tablet_copy_fault_crash_before_write_cmeta, 0.0,
              "Fraction of the time that the server will crash before the "
              "TabletCopyClient persists the ConsensusMetadata file. "
              "(For testing only!)");
TAG_FLAG(tablet_copy_fault_crash_before_write_cmeta, unsafe);
TAG_FLAG(tablet_copy_fault_crash_before_write_cmeta, runtime);

DECLARE_int32(tablet_copy_transfer_chunk_size_bytes);

METRIC_DEFINE_counter(server, tablet_copy_bytes_fetched,
                      "Bytes Fetched By Tablet Copy",
                      kudu::MetricUnit::kBytes,
                      "Number of bytes fetched during tablet copy operations since server start");

METRIC_DEFINE_gauge_int32(server, tablet_copy_open_client_sessions,
                          "Open Table Copy Client Sessions",
                          kudu::MetricUnit::kSessions,
                          "Number of currently open tablet copy client sessions on this server");

// RETURN_NOT_OK_PREPEND() with a remote-error unwinding step.
#define RETURN_NOT_OK_UNWIND_PREPEND(status, controller, msg) \
  RETURN_NOT_OK_PREPEND(UnwindRemoteError(status, controller), msg)

namespace kudu {
namespace tserver {

using consensus::ConsensusMetadata;
using consensus::ConsensusMetadataManager;
using consensus::MakeOpId;
using consensus::OpId;
using env_util::CopyFile;
using fs::BlockManager;
using fs::CreateBlockOptions;
using fs::WritableBlock;
using rpc::Messenger;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using tablet::ColumnDataPB;
using tablet::DeltaDataPB;
using tablet::RowSetDataPB;
using tablet::TabletDataState;
using tablet::TabletDataState_Name;
using tablet::TabletMetadata;
using tablet::TabletReplica;
using tablet::TabletSuperBlockPB;

TabletCopyClientMetrics::TabletCopyClientMetrics(const scoped_refptr<MetricEntity>& metric_entity)
    : bytes_fetched(METRIC_tablet_copy_bytes_fetched.Instantiate(metric_entity)),
      open_client_sessions(METRIC_tablet_copy_open_client_sessions.Instantiate(metric_entity, 0)) {
}

TabletCopyClient::TabletCopyClient(std::string tablet_id,
    FsManager* fs_manager,
    scoped_refptr<ConsensusMetadataManager> cmeta_manager,
    shared_ptr<Messenger> messenger,
    TabletCopyClientMetrics* tablet_copy_metrics)
    : tablet_id_(std::move(tablet_id)),
      fs_manager_(fs_manager),
      cmeta_manager_(std::move(cmeta_manager)),
      messenger_(std::move(messenger)),
      state_(kInitialized),
      replace_tombstoned_tablet_(false),
      tablet_replica_(nullptr),
      session_idle_timeout_millis_(FLAGS_tablet_copy_begin_session_timeout_ms),
      start_time_micros_(0),
      rng_(GetRandomSeed32()),
      tablet_copy_metrics_(tablet_copy_metrics) {
  BlockManager* bm = fs_manager->block_manager();
  transaction_ = bm->NewCreationTransaction();
  if (tablet_copy_metrics_) {
    tablet_copy_metrics_->open_client_sessions->Increment();
  }
}

TabletCopyClient::~TabletCopyClient() {
  // Note: Ending the tablet copy session releases anchors on the remote.
  WARN_NOT_OK(EndRemoteSession(), Substitute("$0Unable to close tablet copy session",
                                             LogPrefix()));
  WARN_NOT_OK(Abort(), Substitute("$0Failed to fully clean up tablet after aborted copy",
                                  LogPrefix()));
  if (tablet_copy_metrics_) {
    tablet_copy_metrics_->open_client_sessions->IncrementBy(-1);
  }
}

Status TabletCopyClient::SetTabletToReplace(const scoped_refptr<TabletMetadata>& meta,
                                            int64_t caller_term) {
  CHECK_EQ(tablet_id_, meta->tablet_id());
  TabletDataState data_state = meta->tablet_data_state();
  if (data_state != tablet::TABLET_DATA_TOMBSTONED) {
    return Status::IllegalState(Substitute("Tablet $0 not in tombstoned state: $1 ($2)",
                                           tablet_id_,
                                           TabletDataState_Name(data_state),
                                           data_state));
  }

  boost::optional<OpId> last_logged_opid = meta->tombstone_last_logged_opid();
  if (!last_logged_opid) {
    // There are certain cases where we can end up with a tombstoned replica
    // that does not store its last-logged opid. One such case is when there is
    // WAL corruption at startup time, resulting in a replica being evicted and
    // deleted. In such a case, it is not possible to determine the last-logged
    // opid. Another such case (at the time of writing) is initialization
    // failure due to any number of problems, resulting in the replica going
    // into an error state. If the replica is tombstoned while in an error
    // state, the last-logged opid will not be stored. See KUDU-2106.
    LOG_WITH_PREFIX(INFO) << "overwriting existing tombstoned replica "
                             "with an unknown last-logged opid";
  } else if (last_logged_opid->term() > caller_term) {
    return Status::InvalidArgument(
        Substitute("Leader has term $0 but the last log entry written by the tombstoned replica "
                   "for tablet $1 has higher term $2. Refusing tablet copy from leader",
                   caller_term, tablet_id_, last_logged_opid->term()));
  }

  replace_tombstoned_tablet_ = true;
  meta_ = meta;

  // Load the old consensus metadata, if it exists.
  scoped_refptr<ConsensusMetadata> cmeta;
  Status s = cmeta_manager_->Load(tablet_id_, &cmeta);
  if (s.IsNotFound()) {
    // The consensus metadata was not written to disk, possibly due to a failed
    // tablet copy.
    return Status::OK();
  }
  RETURN_NOT_OK(s);
  cmeta_.swap(cmeta);
  return Status::OK();
}

Status TabletCopyClient::Start(const HostPort& copy_source_addr,
                               scoped_refptr<TabletMetadata>* meta) {
  CHECK_EQ(kInitialized, state_);
  start_time_micros_ = GetCurrentTimeMicros();

  Sockaddr addr;
  RETURN_NOT_OK(SockaddrFromHostPort(copy_source_addr, &addr));
  if (addr.IsWildcard()) {
    return Status::InvalidArgument("Invalid wildcard address to tablet copy from",
                                   Substitute("$0 (resolved to $1)",
                                              copy_source_addr.host(), addr.host()));
  }
  LOG_WITH_PREFIX(INFO) << "Beginning tablet copy session"
                        << " from remote peer at address " << copy_source_addr.ToString();

  // Set up an RPC proxy for the TabletCopyService.
  proxy_.reset(new TabletCopyServiceProxy(messenger_, addr, copy_source_addr.host()));

  BeginTabletCopySessionRequestPB req;
  req.set_requestor_uuid(fs_manager_->uuid());
  req.set_tablet_id(tablet_id_);

  rpc::RpcController controller;

  // Begin the tablet copy session with the remote peer.
  BeginTabletCopySessionResponsePB resp;
  RETURN_NOT_OK_PREPEND(SendRpcWithRetry(&controller, [&] {
    return proxy_->BeginTabletCopySession(req, &resp, &controller);
  }), "unable to begin tablet copy session");

  string copy_peer_uuid = resp.has_responder_uuid()
      ? resp.responder_uuid() : "(unknown uuid)";
  if (resp.superblock().tablet_data_state() != tablet::TABLET_DATA_READY) {
    Status s = Status::IllegalState("Remote peer (" + copy_peer_uuid + ")" +
                                    " is currently copying itself!",
                                    pb_util::SecureShortDebugString(resp.superblock()));
    LOG_WITH_PREFIX(WARNING) << s.ToString();
    return s;
  }

  session_id_ = resp.session_id();
  // Update our default RPC timeout to reflect the server's session timeout.
  session_idle_timeout_millis_ = resp.session_idle_timeout_millis();

  // Store a copy of the remote (old) superblock.
  remote_superblock_.reset(resp.release_superblock());

  // Make a copy of the remote superblock. We first clear out the remote blocks
  // from this structure and then add them back in as they are downloaded.
  superblock_.reset(new TabletSuperBlockPB(*remote_superblock_));

  // The block ids (in active rowsets as well as from orphaned blocks) on the
  // remote have no meaning to us and could cause data loss if accidentally
  // deleted locally. We must clear them all.
  superblock_->clear_rowsets();
  superblock_->clear_orphaned_blocks();

  // The UUIDs within the DataDirGroupPB on the remote are also unique to the
  // remote and have no meaning to us.
  superblock_->clear_data_dir_group();

  // Set the data state to COPYING to indicate that, on crash, this replica
  // should be discarded.
  superblock_->set_tablet_data_state(tablet::TABLET_DATA_COPYING);

  wal_seqnos_.assign(resp.wal_segment_seqnos().begin(), resp.wal_segment_seqnos().end());
  remote_cstate_.reset(resp.release_initial_cstate());

  Schema schema;
  RETURN_NOT_OK_PREPEND(SchemaFromPB(superblock_->schema(), &schema),
                        "Cannot deserialize schema from remote superblock");

  if (replace_tombstoned_tablet_) {
    // Also validate the term of the source peer, in case they are
    // different. This is a sanity check that protects us in case a bug or
    // misconfiguration causes us to attempt to copy from an out-of-date
    // source peer, even after passing the term check from the caller in
    // SetTabletToReplace().

    boost::optional<OpId> last_logged_opid = meta_->tombstone_last_logged_opid();
    if (last_logged_opid && last_logged_opid->term() > remote_cstate_->current_term()) {
      return Status::InvalidArgument(
          Substitute("Tablet $0: source peer has term $1 but "
                     "tombstoned replica has last-logged opid with higher term $2. "
                     "Refusing tablet copy from source peer $3",
                     tablet_id_,
                     remote_cstate_->current_term(),
                     last_logged_opid->term(),
                     copy_peer_uuid));
    }

    // Retain the last-logged OpId from the previous tombstone in case this
    // tablet copy is aborted.
    if (last_logged_opid) {
      *superblock_->mutable_tombstone_last_logged_opid() = *last_logged_opid;
    }

    // We are about to persist things to disk. Update the tablet copy state
    // machine so we know there is state to clean up in case we fail.
    state_ = kStarting;
    // Set the data state to 'COPYING' and remove any existing orphaned blocks
    // and WALs from the tablet.
    RETURN_NOT_OK_PREPEND(
        TSTabletManager::DeleteTabletData(meta_, cmeta_manager_,
                                          tablet::TABLET_DATA_COPYING,
                                          /*last_logged_opid=*/ boost::none),
        "Could not replace superblock with COPYING data state");
    RETURN_NOT_OK_PREPEND(fs_manager_->dd_manager()->CreateDataDirGroup(tablet_id_),
        "Could not create a new directory group for tablet copy");
  } else {
    // HACK: Set the initial tombstoned last-logged OpId to 1.0 when copying a
    // replica for the first time, so that if the tablet copy fails, the
    // tombstoned replica will still be able to vote.
    // TODO(KUDU-2122): Give this particular OpId a name.
    *superblock_->mutable_tombstone_last_logged_opid() = MakeOpId(1, 0);
    Partition partition;
    Partition::FromPB(superblock_->partition(), &partition);
    PartitionSchema partition_schema;
    RETURN_NOT_OK(PartitionSchema::FromPB(superblock_->partition_schema(),
                                          schema, &partition_schema));

    // Create the superblock on disk.
    RETURN_NOT_OK(TabletMetadata::CreateNew(fs_manager_, tablet_id_,
                                            superblock_->table_name(),
                                            superblock_->table_id(),
                                            schema,
                                            partition_schema,
                                            partition,
                                            superblock_->tablet_data_state(),
                                            superblock_->tombstone_last_logged_opid(),
                                            &meta_));
    // We have begun persisting things to disk. Update the tablet copy state
    // machine so we know there is state to clean up in case we fail.
    state_ = kStarting;
  }
  CHECK_OK(fs_manager_->dd_manager()->GetDataDirGroupPB(
      tablet_id_, superblock_->mutable_data_dir_group()));

  // Create the ConsensusMetadata before returning from Start() so that it's
  // possible to vote while we are copying the replica for the first time.
  RETURN_NOT_OK(WriteConsensusMetadata());

  state_ = kStarted;
  if (meta) {
    *meta = meta_;
  }
  return Status::OK();
}

Status TabletCopyClient::FetchAll(const scoped_refptr<TabletReplica>& tablet_replica) {
  CHECK_EQ(kStarted, state_);

  MAYBE_FAULT(FLAGS_tablet_copy_fault_crash_on_fetch_all);

  tablet_replica_ = tablet_replica;

  // Download all the files (serially, for now, but in parallel in the future).
  RETURN_NOT_OK(DownloadBlocks());
  RETURN_NOT_OK(DownloadWALs());

  return Status::OK();
}

Status TabletCopyClient::Finish() {
  CHECK(meta_);
  CHECK_EQ(kStarted, state_);

  // Defer the closures of all downloaded blocks to here, but before superblock
  // replacement for the following reasons:
  //  1) If DownloadWALs() fails there's no reason to commit all those blocks and do sync().
  //  2) While DownloadWALs() is running the kernel has more time to eagerly flush the blocks,
  //     so the fsync() operations could be cheaper.
  //  3) Downloaded blocks should be made durable before replacing superblock.
  RETURN_NOT_OK(transaction_->CommitCreatedBlocks());

  // Replace tablet metadata superblock. This will set the tablet metadata state
  // to TABLET_DATA_READY, since we checked above that the response
  // superblock is in a valid state to bootstrap from.
  LOG_WITH_PREFIX(INFO) << "Tablet Copy complete. Replacing tablet superblock.";
  SetStatusMessage("Replacing tablet superblock");

  boost::optional<OpId> last_logged_opid = superblock_->tombstone_last_logged_opid();
  auto revert_activate_superblock = MakeScopedCleanup([&] {
    // If we fail below, revert the updated state so further calls to Abort()
    // can clean up appropriately.
    if (last_logged_opid) {
      *superblock_->mutable_tombstone_last_logged_opid() = *last_logged_opid;
    }
    superblock_->set_tablet_data_state(TabletDataState::TABLET_DATA_COPYING);
  });

  superblock_->clear_tombstone_last_logged_opid();
  superblock_->set_tablet_data_state(tablet::TABLET_DATA_READY);

  RETURN_NOT_OK(meta_->ReplaceSuperBlock(*superblock_));

  if (FLAGS_tablet_copy_save_downloaded_metadata) {
    string meta_path = fs_manager_->GetTabletMetadataPath(tablet_id_);
    string meta_copy_path = Substitute("$0.copy.$1$2", meta_path, start_time_micros_, kTmpInfix);
    RETURN_NOT_OK_PREPEND(CopyFile(Env::Default(), meta_path, meta_copy_path,
                                   WritableFileOptions()),
                          "Unable to make copy of tablet metadata");
  }

  // Now that we've finished everything, complete.
  revert_activate_superblock.cancel();
  state_ = kFinished;
  return Status::OK();
}

Status TabletCopyClient::Abort() {
  // If we have not begun doing anything or have already finished, there is
  // nothing left to do.
  if (state_ == kInitialized || state_ == kFinished) {
    return Status::OK();
  }

  DCHECK_EQ(tablet::TABLET_DATA_COPYING, superblock_->tablet_data_state());
  // Ensure upon exiting that the copy is finished and that the tablet is left
  // tombstoned. This is guaranteed because we always call DeleteTabletData(),
  // which leaves the metadata in this state, even on failure.
  SCOPED_CLEANUP({
    DCHECK_EQ(tablet::TABLET_DATA_TOMBSTONED, meta_->tablet_data_state());
    state_ = kFinished;
  });

  // Load the in-progress superblock in-memory so that when we delete the
  // tablet, all the partial blocks we persisted will be deleted.
  //
  // Note: We warn instead of returning early here upon failure because even if
  // the superblock protobuf was somehow corrupted, we still want to attempt to
  // delete the tablet's data dir group, WAL segments, etc.
  WARN_NOT_OK(meta_->LoadFromSuperBlock(*superblock_),
      "Failed to load the new superblock");

  // Finally, tombstone the tablet metadata and try deleting all of the tablet
  // data on disk, including blocks and WALs.
  RETURN_NOT_OK_PREPEND(
      TSTabletManager::DeleteTabletData(meta_, cmeta_manager_,
                                        tablet::TABLET_DATA_TOMBSTONED,
                                        /*last_logged_opid=*/ boost::none),
      LogPrefix() + "Failed to tombstone tablet after aborting tablet copy");

  SetStatusMessage(Substitute("Tombstoned tablet $0: Tablet copy aborted", tablet_id_));
  return Status::OK();
}

// Enhance a RemoteError Status message with additional details from the remote.
Status TabletCopyClient::UnwindRemoteError(const Status& status,
                                           const rpc::RpcController& controller) {
  if (!status.IsRemoteError() ||
      !controller.error_response()->HasExtension(TabletCopyErrorPB::tablet_copy_error_ext)) {
    return status;
  }

  const TabletCopyErrorPB& error =
    controller.error_response()->GetExtension(TabletCopyErrorPB::tablet_copy_error_ext);

  return status.CloneAndAppend(
      strings::Substitute("$0: received error code $1 from remote service",
                          TabletCopyErrorPB::Code_Name(error.code()),
                          StatusFromPB(error.status()).ToString()));
}

void TabletCopyClient::SetStatusMessage(const string& message) {
  if (tablet_replica_ != nullptr) {
    tablet_replica_->SetStatusMessage(Substitute("Tablet Copy: $0", message));
  }
}

Status TabletCopyClient::EndRemoteSession() {
  if (state_ == kInitialized) {
    return Status::OK();
  }

  EndTabletCopySessionRequestPB req;
  req.set_session_id(session_id_);
  req.set_is_success(true);
  EndTabletCopySessionResponsePB resp;

  rpc::RpcController controller;
  RETURN_NOT_OK_PREPEND(SendRpcWithRetry(&controller, [&] {
    return proxy_->EndTabletCopySession(req, &resp, &controller);
  }), "failure ending tablet copy session");

  return Status::OK();
}

Status TabletCopyClient::DownloadWALs() {
  CHECK_EQ(kStarted, state_);

  // Delete and recreate WAL dir if it already exists, to ensure stray files are
  // not kept from previous copies and runs.
  string path = fs_manager_->GetTabletWalDir(tablet_id_);
  if (fs_manager_->env()->FileExists(path)) {
    RETURN_NOT_OK(fs_manager_->env()->DeleteRecursively(path));
  }
  RETURN_NOT_OK(fs_manager_->env()->CreateDir(path));
  RETURN_NOT_OK(fs_manager_->env()->SyncDir(DirName(path))); // fsync() parent dir.

  // Download the WAL segments.
  int num_segments = wal_seqnos_.size();
  LOG_WITH_PREFIX(INFO) << "Starting download of " << num_segments << " WAL segments...";
  uint64_t counter = 0;
  for (uint64_t seg_seqno : wal_seqnos_) {
    SetStatusMessage(Substitute("Downloading WAL segment with seq. number $0 ($1/$2)",
                                seg_seqno, counter + 1, num_segments));
    RETURN_NOT_OK(DownloadWAL(seg_seqno));
    ++counter;
  }

  return Status::OK();
}

int TabletCopyClient::CountRemoteBlocks() const {
  int num_blocks = 0;
  for (const RowSetDataPB& rowset : remote_superblock_->rowsets()) {
    num_blocks += rowset.columns_size();
    num_blocks += rowset.redo_deltas_size();
    num_blocks += rowset.undo_deltas_size();
    if (rowset.has_bloom_block()) {
      num_blocks++;
    }
    if (rowset.has_adhoc_index_block()) {
      num_blocks++;
    }
  }
  return num_blocks;
}

Status TabletCopyClient::DownloadBlocks() {
  CHECK_EQ(kStarted, state_);

  // Count up the total number of blocks to download.
  int num_remote_blocks = CountRemoteBlocks();

  // Download each block, writing the new block IDs into the new superblock
  // as each block downloads.
  int block_count = 0;
  LOG_WITH_PREFIX(INFO) << "Starting download of " << num_remote_blocks << " data blocks...";
  for (const RowSetDataPB& src_rowset : remote_superblock_->rowsets()) {
    // Create rowset.
    RowSetDataPB* dst_rowset = superblock_->add_rowsets();
    *dst_rowset = src_rowset;
    // Clear the data in the rowset so that we don't end up deleting the wrong
    // blocks (using the ids of the remote blocks) if we fail.
    // TODO(mpercy): This is pretty fragile. Consider building a class
    // structure on top of SuperBlockPB to abstract copying details.
    dst_rowset->clear_columns();
    dst_rowset->clear_redo_deltas();
    dst_rowset->clear_undo_deltas();
    dst_rowset->clear_bloom_block();
    dst_rowset->clear_adhoc_index_block();

    // We can't leave superblock_ unserializable with unset required field
    // values in child elements, so we must download and rewrite each block
    // before referencing it in the rowset.
    for (const ColumnDataPB& src_col : src_rowset.columns()) {
      BlockIdPB new_block_id;
      RETURN_NOT_OK(DownloadAndRewriteBlock(src_col.block(), num_remote_blocks,
                                            &block_count, &new_block_id));
      ColumnDataPB* dst_col = dst_rowset->add_columns();
      *dst_col = src_col;
      *dst_col->mutable_block() = new_block_id;
    }
    for (const DeltaDataPB& src_redo : src_rowset.redo_deltas()) {
      BlockIdPB new_block_id;
      RETURN_NOT_OK(DownloadAndRewriteBlock(src_redo.block(), num_remote_blocks,
                                            &block_count, &new_block_id));
      DeltaDataPB* dst_redo = dst_rowset->add_redo_deltas();
      *dst_redo = src_redo;
      *dst_redo->mutable_block() = new_block_id;
    }
    for (const DeltaDataPB& src_undo : src_rowset.undo_deltas()) {
      BlockIdPB new_block_id;
      RETURN_NOT_OK(DownloadAndRewriteBlock(src_undo.block(), num_remote_blocks,
                                            &block_count, &new_block_id));
      DeltaDataPB* dst_undo = dst_rowset->add_undo_deltas();
      *dst_undo = src_undo;
      *dst_undo->mutable_block() = new_block_id;
    }
    if (src_rowset.has_bloom_block()) {
      BlockIdPB new_block_id;
      RETURN_NOT_OK(DownloadAndRewriteBlock(src_rowset.bloom_block(), num_remote_blocks,
                                            &block_count, &new_block_id));
      *dst_rowset->mutable_bloom_block() = new_block_id;
    }
    if (src_rowset.has_adhoc_index_block()) {
      BlockIdPB new_block_id;
      RETURN_NOT_OK(DownloadAndRewriteBlock(src_rowset.adhoc_index_block(), num_remote_blocks,
                                            &block_count, &new_block_id));
      *dst_rowset->mutable_adhoc_index_block() = new_block_id;
    }
  }

  return Status::OK();
}

Status TabletCopyClient::DownloadWAL(uint64_t wal_segment_seqno) {
  VLOG_WITH_PREFIX(1) << "Downloading WAL segment with seqno " << wal_segment_seqno;
  RETURN_NOT_OK_PREPEND(CheckHealthyDirGroup(), "Not downloading WAL for replica");

  DataIdPB data_id;
  data_id.set_type(DataIdPB::LOG_SEGMENT);
  data_id.set_wal_segment_seqno(wal_segment_seqno);
  string dest_path = fs_manager_->GetWalSegmentFileName(tablet_id_, wal_segment_seqno);

  WritableFileOptions opts;
  opts.sync_on_close = true;
  unique_ptr<WritableFile> writer;
  RETURN_NOT_OK_PREPEND(fs_manager_->env()->NewWritableFile(opts, dest_path, &writer),
                        "Unable to open file for writing");
  RETURN_NOT_OK_PREPEND(DownloadFile(data_id, writer.get()),
                        Substitute("Unable to download WAL segment with seq. number $0",
                                   wal_segment_seqno));
  return Status::OK();
}

Status TabletCopyClient::WriteConsensusMetadata() {
  MAYBE_FAULT(FLAGS_tablet_copy_fault_crash_before_write_cmeta);

  // If we didn't find a previous consensus meta file, create one.
  if (!cmeta_) {
    return cmeta_manager_->Create(tablet_id_,
                                  remote_cstate_->committed_config(),
                                  remote_cstate_->current_term());
  }

  // Otherwise, update the consensus metadata to reflect the config and term
  // sent by the tablet copy source.
  cmeta_->MergeCommittedConsensusStatePB(*remote_cstate_);
  RETURN_NOT_OK(cmeta_->Flush());

  if (FLAGS_tablet_copy_save_downloaded_metadata) {
    string cmeta_path = fs_manager_->GetConsensusMetadataPath(tablet_id_);
    string cmeta_copy_path = Substitute("$0.copy.$1$2", cmeta_path, start_time_micros_, kTmpInfix);
    RETURN_NOT_OK_PREPEND(CopyFile(Env::Default(), cmeta_path, cmeta_copy_path,
                                   WritableFileOptions()),
                          "Unable to make copy of consensus metadata");
  }

  return Status::OK();
}

Status TabletCopyClient::DownloadAndRewriteBlock(const BlockIdPB& src_block_id,
                                                 int num_blocks,
                                                 int* block_count,
                                                 BlockIdPB* dest_block_id) {
  BlockId old_block_id(BlockId::FromPB(src_block_id));
  SetStatusMessage(Substitute("Downloading block $0 ($1/$2)",
                              old_block_id.ToString(),
                              *block_count + 1, num_blocks));
  BlockId new_block_id;
  RETURN_NOT_OK_PREPEND(DownloadBlock(old_block_id, &new_block_id),
      "Unable to download block with id " + old_block_id.ToString());

  new_block_id.CopyToPB(dest_block_id);
  (*block_count)++;
  return Status::OK();
}

Status TabletCopyClient::DownloadBlock(const BlockId& old_block_id,
                                       BlockId* new_block_id) {
  VLOG_WITH_PREFIX(1) << "Downloading block with block_id " << old_block_id.ToString();
  RETURN_NOT_OK_PREPEND(CheckHealthyDirGroup(), "Not downloading block for replica");

  unique_ptr<WritableBlock> block;
  RETURN_NOT_OK_PREPEND(fs_manager_->CreateNewBlock(CreateBlockOptions({ tablet_id_ }), &block),
                        "Unable to create new block");

  DataIdPB data_id;
  data_id.set_type(DataIdPB::BLOCK);
  old_block_id.CopyToPB(data_id.mutable_block_id());
  RETURN_NOT_OK_PREPEND(DownloadFile(data_id, block.get()),
                        Substitute("Unable to download block $0",
                                   old_block_id.ToString()));

  *new_block_id = block->id();
  RETURN_NOT_OK_PREPEND(block->Finalize(), "Unable to finalize block");
  transaction_->AddCreatedBlock(std::move(block));
  return Status::OK();
}

template<class Appendable>
Status TabletCopyClient::DownloadFile(const DataIdPB& data_id,
                                      Appendable* appendable) {
  uint64_t offset = 0;
  rpc::RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(session_idle_timeout_millis_));
  FetchDataRequestPB req;
  req.set_session_id(session_id_);
  req.mutable_data_id()->CopyFrom(data_id);
  req.set_max_length(FLAGS_tablet_copy_transfer_chunk_size_bytes);

  bool done = false;
  while (!done) {
    req.set_offset(offset);

    // Request the next data chunk.
    FetchDataResponsePB resp;
    RETURN_NOT_OK_PREPEND(SendRpcWithRetry(&controller, [&] {
          return proxy_->FetchData(req, &resp, &controller);
    }), "unable to fetch data from remote");

    // Sanity-check for corruption.
    RETURN_NOT_OK_PREPEND(VerifyData(offset, resp.chunk()),
                          Substitute("Error validating data item $0",
                                     pb_util::SecureShortDebugString(data_id)));

    // Write the data.
    RETURN_NOT_OK(appendable->Append(resp.chunk().data()));

    if (PREDICT_FALSE(FLAGS_tablet_copy_download_file_inject_latency_ms > 0)) {
      LOG_WITH_PREFIX(INFO) << "Injecting latency into file download: " <<
          FLAGS_tablet_copy_download_file_inject_latency_ms;
      SleepFor(MonoDelta::FromMilliseconds(FLAGS_tablet_copy_download_file_inject_latency_ms));
    }

    auto chunk_size = resp.chunk().data().size();
    done = offset + chunk_size == resp.chunk().total_data_length();
    offset += chunk_size;
    if (tablet_copy_metrics_) {
      tablet_copy_metrics_->bytes_fetched->IncrementBy(chunk_size);
    }
  }

  return Status::OK();
}

Status TabletCopyClient::VerifyData(uint64_t offset, const DataChunkPB& chunk) {
  // Verify the offset is what we expected.
  if (offset != chunk.offset()) {
    return Status::InvalidArgument("Offset did not match what was asked for",
        Substitute("$0 vs $1", offset, chunk.offset()));
  }

  // Verify that the chunk does not overflow the total data length.
  if (offset + chunk.data().length() > chunk.total_data_length()) {
    return Status::InvalidArgument("Chunk exceeds total block data length",
        Substitute("$0 vs $1", offset + chunk.data().length(), chunk.total_data_length()));
  }

  // Verify the checksum.
  uint32_t crc32 = crc::Crc32c(chunk.data().data(), chunk.data().length());
  if (PREDICT_FALSE(crc32 != chunk.crc32())) {
    return Status::Corruption(
        Substitute("CRC32 does not match at offset $0 size $1: $2 vs $3",
          offset, chunk.data().size(), crc32, chunk.crc32()));
  }
  return Status::OK();
}

string TabletCopyClient::LogPrefix() {
  return Substitute("T $0 P $1: tablet copy: ",
                    tablet_id_, fs_manager_->uuid());
}

Status TabletCopyClient::CheckHealthyDirGroup() const {
  if (fs_manager_->dd_manager()->IsTabletInFailedDir(tablet_id_)) {
    return Status::IOError(
        Substitute("Tablet $0 is in a failed directory", tablet_id_));
  }
  return Status::OK();
}

template<typename F>
Status TabletCopyClient::SendRpcWithRetry(rpc::RpcController* controller, F f) {
  MonoTime deadline = MonoTime::Now() + MonoDelta::FromMilliseconds(session_idle_timeout_millis_);
  for (int attempt = 1;; attempt++) {
    controller->Reset();
    controller->set_deadline(deadline);
    Status s = UnwindRemoteError(f(), *controller);

    // Retry after a backoff period if the error is retriable.
    const rpc::ErrorStatusPB* err = controller->error_response();
    if (!s.ok() && err && (err->code() == rpc::ErrorStatusPB::ERROR_SERVER_TOO_BUSY ||
                           err->code() == rpc::ErrorStatusPB::ERROR_UNAVAILABLE)) {

      // Polynomial backoff with 50% jitter.
      double kJitterPct = 0.5;
      int32_t kBackoffBaseMs = 10;
      MonoDelta backoff = MonoDelta::FromMilliseconds(
          (1 - kJitterPct + (kJitterPct * rng_.NextDoubleFraction()))
          * kBackoffBaseMs * attempt * attempt);
      if (MonoTime::Now() + backoff > deadline) {
        return Status::TimedOut("unable to fetch data from remote");
      }
      SleepFor(backoff);
      continue;
    }
    return s;
  }
}

} // namespace tserver
} // namespace kudu
