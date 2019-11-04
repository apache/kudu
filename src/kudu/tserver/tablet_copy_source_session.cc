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
#include "kudu/tserver/tablet_copy_source_session.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <vector>

#include <boost/optional/optional.hpp>
#include <gflags/gflags.h>

#include "kudu/consensus/consensus.pb.h"
#include "kudu/consensus/log.h"
#include "kudu/consensus/log.pb.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/consensus/opid.pb.h"
#include "kudu/consensus/opid_util.h"
#include "kudu/consensus/raft_consensus.h"
#include "kudu/fs/block_manager.h"
#include "kudu/fs/data_dirs.h"
#include "kudu/fs/fs.pb.h"
#include "kudu/fs/fs_manager.h"
#include "kudu/gutil/basictypes.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/type_traits.h"
#include "kudu/rpc/transfer.h"
#include "kudu/tablet/tablet_metadata.h"
#include "kudu/tablet/tablet_replica.h"
#include "kudu/util/flag_tags.h"
#include "kudu/util/monotime.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/slice.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

DEFINE_int32(tablet_copy_transfer_chunk_size_bytes, 4 * 1024 * 1024,
             "Size of chunks to transfer when copying tablets between "
             "tablet servers.");
TAG_FLAG(tablet_copy_transfer_chunk_size_bytes, hidden);

METRIC_DEFINE_counter(server, tablet_copy_bytes_sent,
                      "Bytes Sent For Tablet Copy",
                      kudu::MetricUnit::kBytes,
                      "Number of bytes sent during tablet copy operations since server start",
                      kudu::MetricLevel::kDebug);

METRIC_DEFINE_gauge_int32(server, tablet_copy_open_source_sessions,
                          "Open Table Copy Source Sessions",
                          kudu::MetricUnit::kSessions,
                          "Number of currently open tablet copy source sessions on this server",
                          kudu::MetricLevel::kInfo);

DEFINE_int32(tablet_copy_session_inject_latency_on_init_ms, 0,
             "How much latency (in ms) to inject when a tablet copy session is initialized. "
             "(For testing only!)");
TAG_FLAG(tablet_copy_session_inject_latency_on_init_ms, unsafe);
TAG_FLAG(tablet_copy_session_inject_latency_on_init_ms, hidden);

namespace kudu {
namespace tserver {

using consensus::MinimumOpId;
using consensus::OpId;
using fs::ReadableBlock;
using log::ReadableLogSegment;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;
using strings::Substitute;
using tablet::TabletMetadata;
using tablet::TabletReplica;

TabletCopySourceMetrics::TabletCopySourceMetrics(const scoped_refptr<MetricEntity>& metric_entity)
    : bytes_sent(METRIC_tablet_copy_bytes_sent.Instantiate(metric_entity)),
      open_source_sessions(METRIC_tablet_copy_open_source_sessions.Instantiate(metric_entity, 0)) {
}

TabletCopySourceSession::TabletCopySourceSession(
    scoped_refptr<TabletReplica> tablet_replica, std::string session_id,
    std::string requestor_uuid, FsManager* fs_manager,
    TabletCopySourceMetrics* tablet_copy_metrics)
    : tablet_replica_(std::move(tablet_replica)),
      session_id_(std::move(session_id)),
      requestor_uuid_(std::move(requestor_uuid)),
      fs_manager_(fs_manager),
      blocks_deleter_(&blocks_),
      logs_deleter_(&logs_),
      tablet_copy_metrics_(tablet_copy_metrics) {
  if (tablet_copy_metrics_) {
    tablet_copy_metrics_->open_source_sessions->Increment();
  }
}

TabletCopySourceSession::~TabletCopySourceSession() {
  // No lock taken in the destructor, should only be 1 thread with access now.
  CHECK_OK(UnregisterAnchorIfNeededUnlocked());
  if (tablet_copy_metrics_) {
    tablet_copy_metrics_->open_source_sessions->IncrementBy(-1);
  }
}

Status TabletCopySourceSession::Init() {
  return init_once_.Init(&TabletCopySourceSession::InitOnce, this);
}

Status TabletCopySourceSession::InitOnce() {
  // Inject latency during Init() for testing purposes.
  if (PREDICT_FALSE(FLAGS_tablet_copy_session_inject_latency_on_init_ms > 0)) {
    TRACE("Injecting $0ms of latency due to --tablet_copy_session_inject_latency_on_init_ms",
          FLAGS_tablet_copy_session_inject_latency_on_init_ms);
    SleepFor(MonoDelta::FromMilliseconds(FLAGS_tablet_copy_session_inject_latency_on_init_ms));
  }

  RETURN_NOT_OK(tablet_replica_->CheckRunning());
  RETURN_NOT_OK(CheckHealthyDirGroup());

  const string& tablet_id = tablet_replica_->tablet_id();

  // Prevent log GC while we grab log segments and Tablet metadata.
  string anchor_owner_token = Substitute("TabletCopy-$0", session_id_);
  tablet_replica_->log_anchor_registry()->Register(
      MinimumOpId().index(), anchor_owner_token, &log_anchor_);

  // Read the SuperBlock from disk.
  const scoped_refptr<TabletMetadata>& metadata = tablet_replica_->tablet_metadata();
  RETURN_NOT_OK_PREPEND(metadata->ReadSuperBlockFromDisk(&tablet_superblock_),
                        Substitute("Unable to access superblock for tablet $0",
                                   tablet_id));

  // Anchor the data blocks by opening them and adding them to the cache.
  //
  // All subsequent requests should reuse the opened blocks.
  vector<BlockIdPB> data_blocks =
      TabletMetadata::CollectBlockIdPBs(tablet_superblock_);
  for (const BlockIdPB& block_id : data_blocks) {
    VLOG(1) << "Opening block " << pb_util::SecureDebugString(block_id);
    RETURN_NOT_OK(OpenBlock(BlockId::FromPB(block_id)));
  }

  // Get the latest opid in the log at this point in time so we can re-anchor.
  // TODO(mpercy): Do we need special handling for boost::none case?
  boost::optional<OpId> last_logged_opid =
      tablet_replica_->consensus()->GetLastOpId(consensus::RECEIVED_OPID);
  if (!last_logged_opid) last_logged_opid = MinimumOpId();

  // Get the current segments from the log, including the active segment.
  // The Log doesn't add the active segment to the log reader's list until
  // a header has been written to it (but it will not have a footer).
  shared_ptr<log::LogReader> reader = tablet_replica_->log()->reader();
  if (!reader) {
    tablet::TabletStatePB tablet_state = tablet_replica_->state();
    return Status::IllegalState(Substitute(
        "Unable to initialize tablet copy session for tablet $0. "
        "Log reader is not available. Tablet state: $1 ($2)",
        tablet_id, tablet::TabletStatePB_Name(tablet_state), tablet_state));
  }
  reader->GetSegmentsSnapshot(&log_segments_);
  for (const scoped_refptr<ReadableLogSegment>& segment : log_segments_) {
    RETURN_NOT_OK(OpenLogSegment(segment->header().sequence_number()));
  }

  // Look up the committed consensus state.
  // We do this after snapshotting the log to avoid a scenario where the latest
  // entry in the log has a term higher than the term stored in the consensus
  // metadata, which will results in a CHECK failure on RaftConsensus init.
  shared_ptr<consensus::RaftConsensus> consensus = tablet_replica_->shared_consensus();
  if (!consensus) {
    tablet::TabletStatePB tablet_state = tablet_replica_->state();
    return Status::IllegalState(Substitute(
        "Unable to initialize tablet copy session for tablet $0. "
        "Raft Consensus is not available. Tablet state: $1 ($2)",
        tablet_id, tablet::TabletStatePB_Name(tablet_state), tablet_state));
  }
  RETURN_NOT_OK_PREPEND(consensus->ConsensusState(&initial_cstate_),
                        "Consensus state not available");

  // Re-anchor on the highest OpId that was in the log right before we
  // snapshotted the log segments. This helps ensure that we don't end up in a
  // tablet copy loop due to a follower falling too far behind the
  // leader's log when tablet copy is slow. The remote controls when
  // this anchor is released by ending the tablet copy session.
  RETURN_NOT_OK(tablet_replica_->log_anchor_registry()->UpdateRegistration(
      last_logged_opid->index(), anchor_owner_token, &log_anchor_));

  LOG(INFO) << Substitute(
      "T $0 P $1: Tablet Copy: opened $2 blocks and $3 log segments",
      tablet_id, consensus->peer_uuid(), data_blocks.size(), log_segments_.size());
  return Status::OK();
}

const std::string& TabletCopySourceSession::tablet_id() const {
  return tablet_replica_->tablet_id();
}

const std::string& TabletCopySourceSession::requestor_uuid() const {
  return requestor_uuid_;
}

// Determine the length of the data chunk to return to the client.
static int64_t DetermineReadLength(int64_t bytes_remaining, int64_t requested_len) {
  // Overhead in the RPC for things like headers, protobuf data, etc.
  static const int kOverhead = 4096;

  if (requested_len <= 0) {
    requested_len = FLAGS_tablet_copy_transfer_chunk_size_bytes;
  } else {
    requested_len = std::min<int64_t>(requested_len,
                                      FLAGS_tablet_copy_transfer_chunk_size_bytes);
  }
  requested_len = std::min<int64_t>(requested_len, FLAGS_rpc_max_message_size - kOverhead);
  CHECK_GT(requested_len, 0) << "rpc_max_message_size is too low to transfer data: "
                                     << FLAGS_rpc_max_message_size;
  return std::min(bytes_remaining, requested_len);
}

// Calculate the size of the data to return given a maximum client message
// length, the file itself, and the offset into the file to be read from.
static Status GetResponseDataSize(int64_t total_size,
                                  uint64_t offset, int64_t client_maxlen,
                                  TabletCopyErrorPB::Code* error_code, int64_t* data_size) {
  // If requested offset is off the end of the data, bail.
  if (offset >= total_size) {
    *error_code = TabletCopyErrorPB::INVALID_TABLET_COPY_REQUEST;
    return Status::InvalidArgument(
        Substitute("Requested offset ($0) is beyond the data size ($1)",
                   offset, total_size));
  }

  int64_t bytes_remaining = total_size - offset;

  *data_size = DetermineReadLength(bytes_remaining, client_maxlen);
  DCHECK_GT(*data_size, 0);
  if (client_maxlen > 0) {
    DCHECK_LE(*data_size, client_maxlen);
  }

  return Status::OK();
}

// Read a chunk of a file into a buffer.
// data_name provides a string for the block/log to be used in error messages.
template <class Info>
static Status ReadFileChunkToBuf(const Info* info,
                                 uint64_t offset, int64_t client_maxlen,
                                 const string& data_name,
                                 string* data, int64_t* file_size,
                                 TabletCopyErrorPB::Code* error_code) {
  int64_t response_data_size = 0;
  RETURN_NOT_OK_PREPEND(GetResponseDataSize(info->size, offset, client_maxlen, error_code,
                                            &response_data_size),
                        Substitute("Error reading $0", data_name));

  Stopwatch chunk_timer(Stopwatch::THIS_THREAD);
  chunk_timer.start();

  // Writing into a std::string buffer is basically guaranteed to work on C++11,
  // however any modern compiler should be compatible with it.
  // Violates the API contract, but avoids excessive copies.
  data->resize(response_data_size);
  uint8_t* buf = reinterpret_cast<uint8_t*>(const_cast<char*>(data->data()));
  Slice slice(buf, response_data_size);
  Status s = info->Read(offset, slice);
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend(
        Substitute("Unable to read existing file for $0", data_name));
    LOG(WARNING) << s.ToString();
    *error_code = TabletCopyErrorPB::IO_ERROR;
    return s;
  }
  // Figure out if Slice points to buf or if Slice points to the mmap.
  // If it points to the mmap then copy into buf.
  if (slice.data() != buf) {
    memcpy(buf, slice.data(), slice.size());
  }
  chunk_timer.stop();
  TRACE("Tablet Copy: $0: $1 total bytes read. Total time elapsed: $2",
        data_name, response_data_size, chunk_timer.elapsed().ToString());

  *file_size = info->size;
  return Status::OK();
}

Status TabletCopySourceSession::GetBlockPiece(const BlockId& block_id,
                                             uint64_t offset, int64_t client_maxlen,
                                             string* data, int64_t* block_file_size,
                                             TabletCopyErrorPB::Code* error_code) {
  DCHECK(init_once_.init_succeeded());
  RETURN_NOT_OK_PREPEND(CheckHealthyDirGroup(error_code),
                        "Tablet copy source could not get block");
  ImmutableReadableBlockInfo* block_info;
  RETURN_NOT_OK(FindBlock(block_id, &block_info, error_code));

  RETURN_NOT_OK(ReadFileChunkToBuf(block_info, offset, client_maxlen,
                                   Substitute("block $0", block_id.ToString()),
                                   data, block_file_size, error_code));

  // Note: We do not eagerly close the block, as doing so may delete the
  // underlying data if this was its last reader and it had been previously
  // marked for deletion. This would be a problem for parallel readers in
  // the same session; they would not be able to find the block.

  return Status::OK();
}

Status TabletCopySourceSession::GetLogSegmentPiece(uint64_t segment_seqno,
                                                   uint64_t offset, int64_t client_maxlen,
                                                   std::string* data, int64_t* log_file_size,
                                                   TabletCopyErrorPB::Code* error_code) {
  DCHECK(init_once_.init_succeeded());
  RETURN_NOT_OK_PREPEND(CheckHealthyDirGroup(error_code),
                        "Tablet copy source could not get log segment");
  ImmutableRandomAccessFileInfo* file_info;
  RETURN_NOT_OK(FindLogSegment(segment_seqno, &file_info, error_code));
  RETURN_NOT_OK(ReadFileChunkToBuf(file_info, offset, client_maxlen,
                                   Substitute("log segment $0", segment_seqno),
                                   data, log_file_size, error_code));

  // Note: We do not eagerly close log segment files, since we share ownership
  // of the LogSegment objects with the Log itself.
  return Status::OK();
}

bool TabletCopySourceSession::IsBlockOpenForTests(const BlockId& block_id) const {
  DCHECK(init_once_.init_succeeded());
  return ContainsKey(blocks_, block_id);
}

// Add a file to the cache and populate the given ImmutableRandomAcccessFileInfo
// object with the file ref and size.
template <class Collection, class Key, class Readable>
static Status AddImmutableFileToMap(Collection* const cache,
                                    const Key& key,
                                    const Readable& readable,
                                    uint64_t size) {
  // Sanity check for 0-length files.
  if (size == 0) {
    return Status::Corruption("Found 0-length object");
  }

  // Looks good, add it to the cache.
  typedef typename Collection::mapped_type InfoPtr;
  typedef typename base::remove_pointer<InfoPtr>::type Info;
  InsertOrDie(cache, key, new Info(readable, size));

  return Status::OK();
}

Status TabletCopySourceSession::CheckHealthyDirGroup(TabletCopyErrorPB::Code* error_code) const {
  if (fs_manager_->dd_manager()->IsTabletInFailedDir(tablet_id())) {
    if (error_code) {
      *error_code = TabletCopyErrorPB::IO_ERROR;
    }
    return Status::IOError(
        Substitute("Tablet $0 is in a failed directory", tablet_id()));
  }
  return Status::OK();
}

Status TabletCopySourceSession::OpenBlock(const BlockId& block_id) {
  unique_ptr<ReadableBlock> block;
  Status s = fs_manager_->OpenBlock(block_id, &block);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Unable to open requested (existing) block file: "
                 << block_id.ToString() << ": " << s.ToString();
    return s.CloneAndPrepend(Substitute("Unable to open block file for block $0",
                                        block_id.ToString()));
  }

  uint64_t size;
  s = block->Size(&size);
  if (PREDICT_FALSE(!s.ok())) {
    return s.CloneAndPrepend("Unable to get size of block");
  }

  s = AddImmutableFileToMap(&blocks_, block_id, block.get(), size);
  if (!s.ok()) {
    s = s.CloneAndPrepend(Substitute("Error accessing data for block $0", block_id.ToString()));
    LOG(DFATAL) << "Data block disappeared: " << s.ToString();
  } else {
    ignore_result(block.release());
  }
  return s;
}

Status TabletCopySourceSession::FindBlock(const BlockId& block_id,
                                         ImmutableReadableBlockInfo** block_info,
                                         TabletCopyErrorPB::Code* error_code) {
  if (!FindCopy(blocks_, block_id, block_info)) {
    *error_code = TabletCopyErrorPB::BLOCK_NOT_FOUND;
    return Status::NotFound("Block not found", block_id.ToString());
  }
  return Status::OK();
}

Status TabletCopySourceSession::OpenLogSegment(uint64_t segment_seqno) {
  scoped_refptr<log::ReadableLogSegment> log_segment;
  int position = -1;
  if (!log_segments_.empty()) {
    position = segment_seqno - log_segments_[0]->header().sequence_number();
  }
  if (position < 0 || position >= log_segments_.size()) {
    return Status::NotFound(Substitute("Segment with sequence number $0 not found",
                                       segment_seqno));
  }
  log_segment = log_segments_[position];
  CHECK_EQ(log_segment->header().sequence_number(), segment_seqno);

  uint64_t size = log_segment->readable_up_to();
  Status s = AddImmutableFileToMap(&logs_, segment_seqno, log_segment->readable_file(), size);
  if (!s.ok()) {
    s = s.CloneAndPrepend(
            Substitute("Error accessing data for log segment with seqno $0",
                       segment_seqno));
    LOG(INFO) << s.ToString();
  }
  return s;
}

Status TabletCopySourceSession::FindLogSegment(uint64_t segment_seqno,
                                              ImmutableRandomAccessFileInfo** file_info,
                                              TabletCopyErrorPB::Code* error_code) {
  if (!FindCopy(logs_, segment_seqno, file_info)) {
    *error_code = TabletCopyErrorPB::WAL_SEGMENT_NOT_FOUND;
    return Status::NotFound(Substitute("Segment with sequence number $0 not found",
                                       segment_seqno));
  }
  return Status::OK();
}

Status TabletCopySourceSession::UnregisterAnchorIfNeededUnlocked() {
  return tablet_replica_->log_anchor_registry()->UnregisterIfAnchored(&log_anchor_);
}

} // namespace tserver
} // namespace kudu
