// Copyright (c) 2014 Cloudera Inc.
#include "kudu/tserver/remote_bootstrap_session.h"

#include <algorithm>

#include "kudu/consensus/log.h"
#include "kudu/consensus/log_reader.h"
#include "kudu/fs/block_id-inl.h"
#include "kudu/fs/block_manager.h"
#include "kudu/gutil/map-util.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/rpc/transfer.h"
#include "kudu/server/metadata.h"
#include "kudu/tablet/tablet_peer.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/trace.h"

namespace kudu {
namespace tserver {

using consensus::OpId;
using consensus::MinimumOpId;
using fs::ReadableBlock;
using log::OpIdAnchorRegistry;
using tablet::TabletMetadata;
using tablet::TabletSuperBlockPB;
using std::tr1::shared_ptr;
using strings::Substitute;
using tablet::TabletPeer;

RemoteBootstrapSession::RemoteBootstrapSession(const scoped_refptr<TabletPeer>& tablet_peer,
                                               const std::string& session_id,
                                               const std::string& requestor_uuid,
                                               FsManager* fs_manager)
  : tablet_peer_(tablet_peer),
    session_id_(session_id),
    requestor_uuid_(requestor_uuid),
    fs_manager_(fs_manager),
    blocks_deleter_(&blocks_),
    logs_deleter_(&logs_) {
}

RemoteBootstrapSession::~RemoteBootstrapSession() {
  // No lock taken in the destructor, should only be 1 thread with access now.
  CHECK_OK(UnregisterAnchorIfNeededUnlocked());
}

Status RemoteBootstrapSession::Init() {
  // Take locks to support re-initialization of the same session.
  boost::lock_guard<simple_spinlock> l(session_lock_);
  RETURN_NOT_OK(UnregisterAnchorIfNeededUnlocked());

  STLDeleteValues(&blocks_);
  STLDeleteValues(&logs_);
  blocks_.clear();
  logs_.clear();

  const string& tablet_id = tablet_peer_->tablet()->tablet_id();

  // Look up the metadata.
  const TabletMetadata* metadata = tablet_peer_->shared_tablet()->metadata();
  RETURN_NOT_OK_PREPEND(metadata->ToSuperBlock(&tablet_superblock_),
                        Substitute("Unable to access superblock for tablet $0", tablet_id));
  // TODO: Anchor blocks once block GC is implemented. See KUDU-452.

  // Look up the log segments. To avoid races, we do a 2-phase thing where we
  // first anchor all the logs, get a list of the logs available, and then
  // atomically re-anchor on the minimum OpId in that set.
  // TODO: Implement one-shot anchoring through the Log API. See KUDU-284.
  OpIdAnchorRegistry* registry = tablet_peer_->tablet()->opid_anchor_registry();
  string anchor_owner_token = Substitute("RemoteBootstrap-$0", session_id_);

  registry->Register(MinimumOpId(), anchor_owner_token, &log_anchor_);

  // Get the current segments from the log.
  RETURN_NOT_OK(tablet_peer_->log()->GetLogReader()->GetSegmentsSnapshot(&log_segments_));

  // Remove the last one if it doesn't have a footer, i.e. if its currently being
  // written to.
  if (!log_segments_.empty() && !log_segments_.back()->HasFooter()) {
    log_segments_.pop_back();
  }

  // Re-anchor on the earliest OpId that we actually need.
  if (!log_segments_.empty()) {
    // Look for the first operation in the segments and anchor it.
    // The first segment in the sequence must have a footer and an operation with an id.
    // TODO The first segment should always have an operation with id, but it
    // might not if we crashed in the middle of doing log GC and didn't cleanup
    // properly. See KUDU-254.
    CHECK(log_segments_[0]->HasFooter());
    CHECK_GT(log_segments_[0]->footer().idx_entry_size(), 0);
    OpId earliest(log_segments_[0]->footer().idx_entry(0).id());
    // Anchor on the earliest id found in the segments
    RETURN_NOT_OK(registry->UpdateRegistration(earliest, anchor_owner_token, &log_anchor_));
  } else {
    // No log segments returned, so no log anchors needed.
    RETURN_NOT_OK(registry->Unregister(&log_anchor_));
  }

  return Status::OK();
}

const std::string& RemoteBootstrapSession::tablet_id() const {
  return tablet_peer_->tablet()->tablet_id();
}

const std::string& RemoteBootstrapSession::requestor_uuid() const {
  return requestor_uuid_;
}

// Determine the length of the data chunk to return to the client.
static int64_t DetermineReadLength(int64_t bytes_remaining, int64_t requested_len) {
  // Determine the size of the chunks we want to read.
  // Choose "system max" as a multiple of typical HDD block size (4K) with 4K to
  // spare for other stuff in the message, like headers, other protobufs, etc.
  const int32_t kSpareBytes = 4096;
  const int32_t kDiskSectorSize = 4096;
  int32_t system_max_chunk_size =
      ((FLAGS_rpc_max_message_size - kSpareBytes) / kDiskSectorSize) * kDiskSectorSize;
  CHECK_GT(system_max_chunk_size, 0) << "rpc_max_message_size is too low to transfer data: "
                                     << FLAGS_rpc_max_message_size;

  // The min of the {requested, system} maxes is the effective max.
  int64_t maxlen = (requested_len > 0) ? std::min<int64_t>(requested_len, system_max_chunk_size) :
                                        system_max_chunk_size;
  return std::min(bytes_remaining, maxlen);
}

// Calculate the size of the data to return given a maximum client message
// length, the file itself, and the offset into the file to be read from.
static Status GetResponseDataSize(int64_t total_size,
                                  uint64_t offset, int64_t client_maxlen,
                                  RemoteBootstrapErrorPB::Code* error_code, int64_t* data_size) {
  // If requested offset is off the end of the data, bail.
  if (offset >= total_size) {
    *error_code = RemoteBootstrapErrorPB::INVALID_REMOTE_BOOTSTRAP_REQUEST;
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
                                 RemoteBootstrapErrorPB::Code* error_code) {
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
  Slice slice;
  Status s = info->ReadFully(offset, response_data_size, &slice, buf);
  if (PREDICT_FALSE(!s.ok())) {
    s = s.CloneAndPrepend(
        Substitute("Unable to read existing file for $0", data_name));
    LOG(WARNING) << s.ToString();
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return s;
  }
  // Figure out if Slice points to buf or if Slice points to the mmap.
  // If it points to the mmap then copy into buf.
  if (slice.data() != buf) {
    memcpy(buf, slice.data(), slice.size());
  }
  chunk_timer.stop();
  TRACE("Remote bootstrap: $0: $1 total bytes read. Total time elapsed: $2",
        data_name, response_data_size, chunk_timer.elapsed().ToString());

  *file_size = info->size;
  return Status::OK();
}

Status RemoteBootstrapSession::GetBlockPiece(const BlockId& block_id,
                                             uint64_t offset, int64_t client_maxlen,
                                             string* data, int64_t* block_file_size,
                                             RemoteBootstrapErrorPB::Code* error_code) {
  ImmutableReadableBlockInfo* block_info;
  RETURN_NOT_OK(FindOrOpenBlock(block_id, &block_info, error_code));

  RETURN_NOT_OK(ReadFileChunkToBuf(block_info, offset, client_maxlen,
                                   Substitute("block $0", block_id.ToString()),
                                   data, block_file_size, error_code));

  // Eagerly close the block file if we just read the last byte.
  // This optimizes for the sequential-read case by freeing resources.
  if (offset + data->size() == block_info->size) {
    Status s = CloseBlock(block_id);
    if (!s.ok()) {
      // Since we allow parallel readers to read the same block, this may race
      // and the close may fail.
      VLOG(1) << Substitute("Remote bootstrap: Closing block after read failed with code $0: $1",
                            RemoteBootstrapErrorPB::Code_Name(*error_code),
                            s.ToString());
    }
  }

  return Status::OK();
}

Status RemoteBootstrapSession::GetLogSegmentPiece(uint64_t segment_seqno,
                                                  uint64_t offset, int64_t client_maxlen,
                                                  std::string* data, int64_t* block_file_size,
                                                  RemoteBootstrapErrorPB::Code* error_code) {
  ImmutableRandomAccessFileInfo* file_info;
  RETURN_NOT_OK(FindLogSegment(segment_seqno, &file_info, error_code));
  RETURN_NOT_OK(ReadFileChunkToBuf(file_info, offset, client_maxlen,
                                   Substitute("log segment $0", segment_seqno),
                                   data, block_file_size, error_code));

  // Note: We do not eagerly close log segment files, since we share ownership
  // of the LogSegment objects with the Log itself.

  return Status::OK();
}

bool RemoteBootstrapSession::IsBlockOpenForTests(const BlockId& block_id) const {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  return ContainsKey(blocks_, block_id);
}

Status RemoteBootstrapSession::CloseBlock(const BlockId& block_id) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  ImmutableReadableBlockInfo* block_info = EraseKeyReturnValuePtr(&blocks_, block_id);
  if (!block_info) {
    return Status::NotFound("Block is not open", block_id.ToString());
  } else {
    delete block_info;
    return Status::OK();
  }
}

// Add a file to the cache and populate the given ImmutableRandomAcccessFileInfo
// object with the file ref and size.
template <class Collection, class Key, class Readable, class Info>
static Status AddToCacheUnlocked(Collection* const cache,
                                 const Key& key,
                                 const Readable& readable,
                                 Info** info,
                                 RemoteBootstrapErrorPB::Code* error_code) {
  uint64_t size;
  Status s = readable->Size(&size);
  if (PREDICT_FALSE(!s.ok())) {
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return s.CloneAndPrepend("Unable to get size of object");
  }

  // Sanity check for 0-length files.
  if (size == 0) {
    *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    return Status::Corruption("Found 0-length object");
  }

  // Looks good, add it to the cache.
  *info = new Info(readable, size);
  InsertOrDie(cache, key, *info);

  return Status::OK();
}

Status RemoteBootstrapSession::FindOrOpenBlock(const BlockId& block_id,
                                               ImmutableReadableBlockInfo** block_info,
                                               RemoteBootstrapErrorPB::Code* error_code) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  if (FindCopy(blocks_, block_id, block_info)) {
    return Status::OK();
  }

  gscoped_ptr<ReadableBlock> block;
  Status s = fs_manager_->OpenBlock(block_id, &block);
  if (PREDICT_FALSE(!s.ok())) {
    LOG(WARNING) << "Unable to open requested (existing) block file: "
                 << block_id.ToString() << ": " << s.ToString();
    if (s.IsNotFound()) {
      *error_code = RemoteBootstrapErrorPB::BLOCK_NOT_FOUND;
    } else {
      *error_code = RemoteBootstrapErrorPB::IO_ERROR;
    }
    return s.CloneAndPrepend(Substitute("Unable to open block file for block $0",
                                        block_id.ToString()));
  }

  s = AddToCacheUnlocked(&blocks_, block_id, block.get(), block_info, error_code);
  if (!s.ok()) {
    s = s.CloneAndPrepend(Substitute("Error accessing data for block $0", block_id.ToString()));
    LOG(DFATAL) << "Data block disappeared: " << s.ToString();
  } else {
    ignore_result(block.release());
  }
  return s;
}

Status RemoteBootstrapSession::FindLogSegment(uint64_t segment_seqno,
                                              ImmutableRandomAccessFileInfo** file_info,
                                              RemoteBootstrapErrorPB::Code* error_code) {
  boost::lock_guard<simple_spinlock> l(session_lock_);
  if (FindCopy(logs_, segment_seqno, file_info)) {
    return Status::OK();
  }

  scoped_refptr<log::ReadableLogSegment> log_segment;
  int position = -1;
  if (!log_segments_.empty()) {
    position = segment_seqno - log_segments_[0]->header().sequence_number();
  }
  if (position < 0 || position > log_segments_.size()) {
    *error_code = RemoteBootstrapErrorPB::WAL_SEGMENT_NOT_FOUND;
    return Status::NotFound(Substitute("Segment with sequence number $0 not found",
                                       segment_seqno));
  }
  log_segment = log_segments_[position];
  CHECK_EQ(log_segment->header().sequence_number(), segment_seqno);

  Status s = AddToCacheUnlocked(&logs_, segment_seqno, log_segment->readable_file(),
                                file_info, error_code);
  if (!s.ok()) {
    s = s.CloneAndPrepend(
            Substitute("Error accessing data for log segment with seqno $0",
                       segment_seqno));
    LOG(INFO) << s.ToString();
  }
  return s;
}

Status RemoteBootstrapSession::UnregisterAnchorIfNeededUnlocked() {
  OpIdAnchorRegistry* registry = tablet_peer_->tablet()->opid_anchor_registry();
  if (registry->IsRegistered(&log_anchor_)) {
    RETURN_NOT_OK(registry->Unregister(&log_anchor_));
  }
  return Status::OK();
}

} // namespace tserver
} // namespace kudu
